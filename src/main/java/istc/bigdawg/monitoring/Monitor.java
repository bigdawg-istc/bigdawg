package istc.bigdawg.monitoring;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.BDConstants;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.migration.MigrationStatistics;
import istc.bigdawg.packages.QueriesAndPerformanceInformation;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.utils.IslandsAndCast.Scope;

public class Monitor {
    private static final String INSERT = "INSERT INTO monitoring (island, signature, query, lastRan, duration) SELECT '%s', '%s', '%s', -1, -1 WHERE NOT EXISTS (SELECT 1 FROM monitoring WHERE island='%s' AND query='%s')";
    private static final String DELETE = "DELETE FROM monitoring WHERE island='%s' AND query='%s'";
    private static final String UPDATE = "UPDATE monitoring SET lastRan=%d, duration=%d WHERE island='%s' AND query='%s'";
    private static final String RETRIEVE = "SELECT duration FROM monitoring WHERE island='%s' AND query='%s'";
    private static final String SIGRETRIEVE = "SELECT duration, query FROM monitoring WHERE signature='%s'";
    private static final String SIGS = "SELECT DISTINCT(signature) FROM monitoring";
    private static final String MINDURATION = "SELECT min(duration) FROM monitoring";
    private static final String MIGRATE = "INSERT INTO migrationstats(fromLoc, toLoc, objectFrom, objectTo, startTime, endTime, countExtracted, countLoaded, message) VALUES ('%s', '%s', '%s', '%s', %d, %d, %d, %d, '%s')";
    private static final String RETRIEVEMIGRATE = "SELECT objectFrom, objectTo, startTime, endTime, countExtracted, countLoaded, message FROM migrationstats WHERE fromLoc='%s' AND toLoc='%s'";

    public static boolean addBenchmarks(List<QueryExecutionPlan> qeps, Signature signature, boolean lean) {
        BDConstants.Shim[] shims = BDConstants.Shim.values();
        return addBenchmarks(qeps, signature, lean, shims);
    }

    public static boolean addBenchmarks(List<QueryExecutionPlan> qeps, Signature signature, boolean lean, BDConstants.Shim[] shims) {
        for (QueryExecutionPlan qep: qeps) {
            try {
                if (!insert(QueryExecutionPlan.qepToString(qep), signature, qep.getIsland())) {
                    return false;
                }
            } catch (NotSupportIslandException e) {
                e.printStackTrace();
            }
        }

        if (!lean) {
            try {
                runBenchmarks(qeps);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public static boolean removeBenchmarks(List<QueryExecutionPlan> qeps) {
        return removeBenchmarks(qeps, false);
    }

    public static boolean removeBenchmarks(List<QueryExecutionPlan> qeps, boolean removeAll) {
        for (QueryExecutionPlan qep: qeps) {
            try {
                if (!delete(QueryExecutionPlan.qepToString(qep), qep.getIsland())) {
                    return false;
                }
            } catch (NotSupportIslandException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    public static boolean allQueriesDone() {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            PostgreSQLHandler.QueryResult qresult = handler.executeQueryPostgreSQL(MINDURATION);
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

    public static QueriesAndPerformanceInformation getBenchmarkPerformance(List<QueryExecutionPlan> qeps) throws NotSupportIslandException {
        List<String> queries = new ArrayList<>();
        List<Long> perfInfo = new ArrayList<>();

        for (QueryExecutionPlan qep: qeps) {
            String qepString = QueryExecutionPlan.qepToString(qep);
            queries.add(qepString);
            PostgreSQLHandler handler = new PostgreSQLHandler();
            try {
                PostgreSQLHandler.QueryResult qresult = handler.executeQueryPostgreSQL(String.format(RETRIEVE, qep.getIsland().toString(), qepString));
                List<List<String>> rows = qresult.getRows();
                long duration = Long.MAX_VALUE;
                for (List<String> row: rows){
                    long currentDuration = Long.parseLong(row.get(0));
                    if (currentDuration >= 0 && currentDuration < duration){
                        duration = currentDuration;
                    }
                }
                perfInfo.add(duration);
            } catch (SQLException e) {
                e.printStackTrace();
                perfInfo.add(Long.MAX_VALUE);
            }
        }
        System.out.printf("[BigDAWG] MONITOR: Performance information generated.\n");
        return new QueriesAndPerformanceInformation(queries, perfInfo);
    }

    public static List<Signature> getAllSignatures() {
        List<Signature> signatures = new ArrayList<>();

        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            PostgreSQLHandler.QueryResult qresult = handler.executeQueryPostgreSQL(SIGS);
            List<List<String>> rows = qresult.getRows();for (List<String> row: rows){
                String signature = row.get(0);
                signatures.add(new Signature(signature));
            }
        } catch (Exception e) {
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

    public static QueriesAndPerformanceInformation getBenchmarkPerformance(Signature signature) {
        List<String> queries = new ArrayList<>();
        List<Long> perfInfo = new ArrayList<>();

        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
            PostgreSQLHandler.QueryResult qresult = handler.executeQueryPostgreSQL(String.format(SIGRETRIEVE, signature.toRecoverableString()));
            List<List<String>> rows = qresult.getRows();
            for (List<String> row: rows){
                long duration = Long.parseLong(row.get(0));
                if (duration < 0){
                    duration = Long.MAX_VALUE;
                }
                String qep = row.get(1);
                perfInfo.add(duration);
                queries.add(qep);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.printf("[BigDAWG] MONITOR: Performance information generated.\n");
        return new QueriesAndPerformanceInformation(queries, perfInfo);
    }

    private static boolean insert(String query, Signature signature, Scope island) throws NotSupportIslandException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
			handler.executeStatementPostgreSQL(String.format(INSERT, island.toString(), signature.toRecoverableString(), query, island.toString(), query));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    private static boolean delete(String query, Scope island) throws NotSupportIslandException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
			handler.executeStatementPostgreSQL(String.format(DELETE, island.toString(), query));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    public static void runBenchmarks(List<QueryExecutionPlan> qeps) throws SQLException, MigrationException {
        for (QueryExecutionPlan qep: qeps) {
            Executor.executePlan(qep);
        }
    }

    public void finishedBenchmark(QueryExecutionPlan qep, long startTime, long endTime) throws SQLException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        String qepString = QueryExecutionPlan.qepToString(qep);
        handler.executeStatementPostgreSQL(String.format(UPDATE, endTime, endTime-startTime, qep.getIsland(), qepString));

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
        PostgreSQLHandler.QueryResult qresult = handler.executeQueryPostgreSQL(String.format(RETRIEVEMIGRATE, fromLoc, toLoc));
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
