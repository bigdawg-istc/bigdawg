package istc.bigdawg.monitoring;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import istc.bigdawg.BDConstants;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.CrossIslandQueryNode;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.islands.CrossIslandQueryPlan;
import istc.bigdawg.migration.MigrationStatistics;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.signature.Signature;

public class Monitor {
    /**
     * API for adding and retrieving monitoring information
     */

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

    /**
     * Adds a query as a benchmark
     * @param signature - signature representing the query
     * @param lean - whether in lean (production) or expansive (training) mode.
     * @return true if all QueryExecutionPlans are inserted successfully and
     *          the signature has not been added beforehand. false otherwise
     * @throws Exception
     */
    public static boolean addBenchmarks(Signature signature, boolean lean) throws Exception {
        BDConstants.Shim[] shims = BDConstants.Shim.values();
        return addBenchmarks(signature, lean, shims);
    }

    public static boolean addBenchmarks(Signature signature, boolean lean, BDConstants.Shim[] shims) throws Exception {
        logger.debug("Query for signature: " + signature.getQuery());
//        CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);
//        IntraIslandQuery ciqn = ciqp.getTerminalNode();
        CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(signature.getQuery(), new HashSet<>());
        
        Set<IntraIslandQuery> qnSet = new HashSet<>();
        
        for (CrossIslandQueryNode cipn : ciqp.vertexSet())
        	if (cipn instanceof IntraIslandQuery) 
        		qnSet.add((IntraIslandQuery)cipn);
        
        boolean exitBoolean = true;
        for (IntraIslandQuery ciqn : qnSet) {
	        List<QueryExecutionPlan> qeps = ciqn.getAllQEPs(true);
	        boolean isContinue = false;
	        for (int i = 0; i < qeps.size(); i++){
	            try {
	                if (!insert(signature, i)) {
//	                    return false;
	                	exitBoolean = false;
	                	isContinue = true;
	                	break;
	                }
	            } catch (NotSupportIslandException e) {
	                e.printStackTrace();
	            }
	        }
	        if (isContinue) continue;
	        if (!lean) {
	            try {
	                runBenchmarks(qeps, signature);
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }
//	        return true;
        }
        return exitBoolean;
    }

    /**
     * Removes a query as a benchmark
     * @param signature - signature representing the query
     * @return - true if deleted successfully. false otherwise
     */
    public static boolean removeBenchmarks(Signature signature) {
        return delete(signature);
    }

    /**
     * Checks if all queries in the Monitor have been run at least once
     * @return true if all queries have been run at least once. false otherwise
     */
    public static boolean allQueriesDone() {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            JdbcQueryResult qresult = handler.executeQueryOnEngine(MINDURATION);
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

    /**
     * Retrieves the timings for a benchmark query
     * @param signature - signature representing the query
     * @return A list of times for each QueryExecutionPlan the signature can
     *          generate. The list is indexed by the order that QueryExecutionPlans are generated.
     * @throws NotSupportIslandException
     * @throws SQLException
     */
    public static List<Long> getBenchmarkPerformance(Signature signature) throws NotSupportIslandException, SQLException {
        List<Long> perfInfo = new ArrayList<>();
        String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);

        PostgreSQLHandler handler = new PostgreSQLHandler();
        JdbcQueryResult qresult = handler.executeQueryOnEngine(String.format(RETRIEVE, escapedSignature));
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

    /**
     *
     * @return The signatures for all benchmark queries in the Monitor
     */
    public static List<Signature> getAllSignatures() {
        List<Signature> signatures = new ArrayList<>();

        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            JdbcQueryResult qresult = handler.executeQueryOnEngine(SIGS);
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

    /**
     *
     * @param signature - signature representing the query
     * @return The signature of the benchmark query that is closest to the input signature
     */
    public static Signature getClosestSignature(Signature signature) {
        // TODO This could be changed to be much more efficient.
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

    /**
     * Inserts a benchmark for a specific QueryExecutionPlan
     * @param signature - signature representing the query
     * @param index - index of the QueryExecutionPlan when generating it from the signature
     * @return true if successfully inserted. false otherwise.
     * @throws NotSupportIslandException
     */
    private static boolean insert(Signature signature, int index) throws NotSupportIslandException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);
			handler.executeStatementOnConnection(String.format(INSERT, escapedSignature, index, System.currentTimeMillis(), escapedSignature, index));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    /**
     * Deletes all QueryExecutionPlans in the Monitor with the given signature
     * @param signature - signature representing the query
     * @return true if successfully deleted. false otherwise.
     */
    private static boolean delete(Signature signature) {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);
			handler.executeStatementOnConnection(String.format(DELETE, escapedSignature));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    /**
     * Executes a list of QueryExecutionPlans matching a signature using the Executor
     * @param qeps - QueryExecutionPlans matching the signature
     * @param signature - signature representing the query
     * @throws ExecutorEngine.LocalQueryExecutionException
     * @throws MigrationException
     */
    public static void runBenchmarks(List<QueryExecutionPlan> qeps, Signature signature) throws ExecutorEngine.LocalQueryExecutionException, MigrationException {
        for (int i = 0; i < qeps.size(); i++){
            Executor.executePlan(qeps.get(i), signature, i);
        }
    }

    /**
     * Used by the Executor. Updates the timing information for a QueryExecutionPlan
     * @param signature - signature representing the query
     * @param index - index of the QueryExecutionPlan when generating it from the signature
     * @param startTime - time the query started running on the Executor in ms
     * @param endTime - time the query finished running on the Executor in ms
     * @throws SQLException
     */
    public void finishedBenchmark(Signature signature, int index, long startTime, long endTime) throws SQLException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        String escapedSignature = signature.toRecoverableString().replace("'", stringSeparator);
        handler.executeStatementOnConnection(String.format(UPDATE, endTime, endTime-startTime, escapedSignature, index));
    }

    /**
     * Adds migration statistics to the Monitor
     * @param stats - migration statistics to be added
     * @throws SQLException
     */
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
        handler.executeStatementOnConnection(String.format(MIGRATE, fromLoc, toLoc, stats.getObjectFrom(), stats.getObjectTo(), stats.getStartTimeMigration(), stats.getEndTimeMigration(), countExtracted, countLoaded, stats.getMessage()));
    }

    /**
     * Retrieves migration statistics from the Monitor
     * @param from The engine migrated from
     * @param to The engine migrated to
     * @return the stored migration statistics
     * @throws SQLException
     */
    public List<MigrationStatistics> getMigrationStats(ConnectionInfo from, ConnectionInfo to) throws SQLException {
        String fromLoc = ConnectionInfoParser.connectionInfoToString(from);
        String toLoc = ConnectionInfoParser.connectionInfoToString(to);
        PostgreSQLHandler handler = new PostgreSQLHandler();
        JdbcQueryResult qresult = handler.executeQueryOnEngine(String.format(RETRIEVEMIGRATE, fromLoc, toLoc));
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
