package istc.bigdawg.monitoring;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.mortbay.log.Log;

import istc.bigdawg.BDConstants;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.migration.MigrationStatistics;
import istc.bigdawg.packages.CrossIslandCastNode;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.signature.Signature;

public class Monitor {

    private static Logger logger = Logger.getLogger(Monitor.class.getName());

    public static final String stringSeparator = "****";

    private static final String INSERT = "INSERT INTO monitoring (signature, index, lastRan, duration) SELECT '%s', %d, %d, -1 WHERE NOT EXISTS (SELECT 1 FROM monitoring WHERE signature='%s' AND index=%d)";
    private static final String DELETE = "DELETE FROM monitoring WHERE signature='%s'";
    private static final String UPDATE = "UPDATE monitoring SET lastRan=%d, duration=%d WHERE signature='%s' AND index=%d";
    private static final String RETRIEVE = "SELECT duration FROM monitoring WHERE signature='%s' ORDER BY index";
    private static final String SIGS = "SELECT DISTINCT(signature) FROM monitoring";
    private static final String MINDURATION = "SELECT min(duration) FROM monitoring";
    private static final String MIGRATE = "INSERT INTO migrationstats(fromLoc, toLoc, objectFrom, objectTo, startTime, endTime, countExtracted, countLoaded, message) VALUES ('%s', '%s', '%s', '%s', %d, %d, %d, %d, '%s')";
    private static final String RETRIEVEMIGRATE = "SELECT objectFrom, objectTo, startTime, endTime, countExtracted, countLoaded, message FROM migrationstats WHERE fromLoc='%s' AND toLoc='%s'";

    public static boolean addBenchmarks(Signature signature, boolean lean) throws Exception {
        BDConstants.Shim[] shims = BDConstants.Shim.values();
        return addBenchmarks(signature, lean, shims);
    }

    public static boolean addBenchmarks(Signature signature, boolean lean, BDConstants.Shim[] shims) throws Exception {
//        Map<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(signature.getQuery());
        logger.debug("Query for signature: " + signature.getQuery());
//        CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
//        CrossIslandQueryNode ciqn = ciqp.getTerminalNode();
        CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(signature.getQuery());
        CrossIslandQueryNode ciqn = null;
        if (ciqp.getTerminalNode() instanceof CrossIslandQueryNode)
        	ciqn = (CrossIslandQueryNode)ciqp.getTerminalNode();
        else {
        	ciqp.edgesOf(ciqp.getTerminalNode());
        	for (DefaultEdge e : ciqp.edgeSet()) {
        		if (ciqp.getEdgeTarget(e) instanceof CrossIslandCastNode) {
        			ciqn = (CrossIslandQueryNode)(ciqp.getEdgeSource(e));
        			break;
        		};
        	} 
        }
        List<QueryExecutionPlan> qeps = ciqn.getAllQEPs(true);

        for (int i = 0; i < qeps.size(); i++){
            try {
                if (!insert(signature, i)) {
                    return false;
                }
            } catch (NotSupportIslandException e) {
                e.printStackTrace();
            }
        }

        if (!lean) {
            try {
                runBenchmarks(qeps, signature);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public static boolean removeBenchmarks(Signature signature) {
        return delete(signature);
    }

    public static boolean allQueriesDone() {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            JdbcQueryResult qresult = handler.executeQueryPostgreSQL(MINDURATION);
            List<List<String>> rows = qresult.getRows();
            long minDuration = Long.MAX_VALUE;
            for (List<String> row: rows){
                long currentDuration = Long.parseLong(row.get(0));
                if (currentDuration < minDuration){
                    minDuration = currentDuration;
                }
            }
            if (minDuration < 0){
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static List<Long> getBenchmarkPerformance(Signature signature) throws NotSupportIslandException, SQLException {
        List<Long> perfInfo = new ArrayList<>();
        String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);

        PostgreSQLHandler handler = new PostgreSQLHandler();
        JdbcQueryResult qresult = handler.executeQueryPostgreSQL(String.format(RETRIEVE, escapedSignature));
        List<List<String>> rows = qresult.getRows();
        for (List<String> row: rows){
            long currentDuration = Long.parseLong(row.get(0));
            if (currentDuration >= 0){
                perfInfo.add(currentDuration);
            } else {
                perfInfo.add(Long.MAX_VALUE);
            }
        }
        System.out.printf("[BigDAWG] MONITOR: Performance information generated.\n");
        return perfInfo;
    }

    public static List<Signature> getAllSignatures() {
        List<Signature> signatures = new ArrayList<>();

        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            JdbcQueryResult qresult = handler.executeQueryPostgreSQL(SIGS);
            List<List<String>> rows = qresult.getRows();for (List<String> row: rows){
                String signature = row.get(0).replace(stringSeparator, "'");
                signatures.add(new Signature(signature));
            }
        } catch (Exception e) {
            Log.debug(e.getMessage());
            e.printStackTrace();
        }
        return signatures;
    }

    public static Signature getClosestSignature(Signature signature) {
        // TODO This needs to be changed to be much more efficient.
        // We need a way to do similarity in postgres (likely indexing on signature)
        // based on the dimensions we care about
        List<Signature> signatures = getAllSignatures();
        Signature closest = null;
        double distance = Double.MAX_VALUE;
        for (Signature current: signatures){
            // compare them and pick the closest Signature
            double curDist = signature.compare(current);
            if (curDist < distance){
                distance = curDist;
                closest = current;
            }
        }
        return closest;
    }

    private static boolean insert(Signature signature, int index) throws NotSupportIslandException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);
			handler.executeStatementPostgreSQL(String.format(INSERT, escapedSignature, index, System.currentTimeMillis(), escapedSignature, index));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    private static boolean delete(Signature signature) {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);
			handler.executeStatementPostgreSQL(String.format(DELETE, escapedSignature));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    public static void runBenchmarks(List<QueryExecutionPlan> qeps, Signature signature) throws ExecutorEngine.LocalQueryExecutionException, MigrationException {
        for (int i = 0; i < qeps.size(); i++){
            Executor.executePlan(qeps.get(i), signature, i);
        }
    }

    public void finishedBenchmark(Signature signature, int index, long startTime, long endTime) throws SQLException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);
        handler.executeStatementPostgreSQL(String.format(UPDATE, endTime, endTime-startTime, escapedSignature, index));

        // Only for testing purposes.Uncomment when necessary.
/*        try {
            File temp = File.createTempFile("queries", ".tmp");
            BufferedWriter bw = new BufferedWriter(new FileWriter(temp,true));
            bw.write(String.format("%d %s %s\n", endTime-startTime, qep.getIsland(), qepString));
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    public static void addMigrationStats(MigrationStatistics stats) throws SQLException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        String fromLoc = ConnectionInfoParser.connectionInfoToString(stats.getConnectionFrom());
        String toLoc = ConnectionInfoParser.connectionInfoToString(stats.getConnectionTo());
        long countExtracted = -1;
        long countLoaded = -1;
        if (stats.getCountExtractedElements() != null){
            countExtracted = stats.getCountExtractedElements();
        }
        if (stats.getCountLoadedElements() != null){
            countLoaded = stats.getCountLoadedElements();
        }
        handler.executeStatementPostgreSQL(String.format(MIGRATE, fromLoc, toLoc, stats.getObjectFrom(), stats.getObjectTo(), stats.getStartTimeMigration(), stats.getEndTimeMigration(), countExtracted, countLoaded, stats.getMessage()));
    }

    public List<MigrationStatistics> getMigrationStats(ConnectionInfo from, ConnectionInfo to) throws SQLException {
        String fromLoc = ConnectionInfoParser.connectionInfoToString(from);
        String toLoc = ConnectionInfoParser.connectionInfoToString(to);
        PostgreSQLHandler handler = new PostgreSQLHandler();
        JdbcQueryResult qresult = handler.executeQueryPostgreSQL(String.format(RETRIEVEMIGRATE, fromLoc, toLoc));
        List<MigrationStatistics> results = new ArrayList<>();
        List<List<String>> rows = qresult.getRows();
        for (List<String> row: rows){
            String objectFrom = row.get(0);
            String objectTo = row.get(1);
            long startTime = Long.parseLong(row.get(2));
            long endTime = Long.parseLong(row.get(3));
            long countExtracted = Long.parseLong(row.get(4));
            Long countExtractedElements = null;
            if (countExtracted >= 0) {
                countExtractedElements = countExtracted;
            }
            long countLoaded = Long.parseLong(row.get(5));
            Long countLoadedElements = null;
            if (countLoaded >= 0) {
                countLoadedElements = countLoaded;
            }
            String message = row.get(6);
            results.add(new MigrationStatistics(from, to, objectFrom, objectTo, startTime, endTime, countExtractedElements, countLoadedElements, message));
        }
        return results;
    }
}
