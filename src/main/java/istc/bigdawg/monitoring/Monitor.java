package istc.bigdawg.monitoring;

import istc.bigdawg.BDConstants;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ASTNode;
import istc.bigdawg.query.parser.Parser;
import istc.bigdawg.query.parser.simpleParser;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.packages.QueriesAndPerformanceInformation;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.signature.Signature;

import javax.ws.rs.core.Response;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Monitor {
    private static final String INSERT = "INSERT INTO monitoring(island, query, lastRan, duration) VALUES ('%s', '%s', -1, -1)";
    private static final String DELETE = "DELETE FROM monitoring WHERE island='%s' AND query='%s'";
    private static final String UPDATE = "UPDATE monitoring SET lastRan=%d, duration=%d WHERE island='%s' AND query='%s'";
    private static final String RETRIEVE = "SELECT * FROM monitoring WHERE island='%s' AND query='%s'";

    public static boolean addBenchmarks(List<QueryExecutionPlan> qeps, boolean lean) {
        BDConstants.Shim[] shims = BDConstants.Shim.values();
        return addBenchmarks(qeps, lean, shims);
    }

    public static boolean addBenchmarks(List<QueryExecutionPlan> qeps, boolean lean, BDConstants.Shim[] shims) {
        for (QueryExecutionPlan qep: qeps) {
            try {
                if (!insert(QueryExecutionPlan.qepToString(qep), qep.getIsland())) {
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

    public static QueriesAndPerformanceInformation getBenchmarkPerformance(List<QueryExecutionPlan> qeps) throws NotSupportIslandException {
        List<String> queries = new ArrayList<>();
        List<Object> perfInfo = new ArrayList<>();

        for (QueryExecutionPlan qep: qeps) {
            String qepString = QueryExecutionPlan.qepToString(qep);
            queries.add(qepString);
            PostgreSQLHandler handler = new PostgreSQLHandler();
            perfInfo.add(handler.executeQuery(String.format(RETRIEVE, qep.getIsland(), qepString)));
        }
        List<List<String>> queryList = new ArrayList();
        queryList.add(queries);
        System.out.printf("[BigDAWG] MONITOR: Performance information generated.\n");
        return new QueriesAndPerformanceInformation(queryList, perfInfo);
    }

    private static boolean insert(String query, String island) throws NotSupportIslandException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
			handler.executeNotQueryPostgreSQL(String.format(INSERT, island, query));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    private static boolean delete(String query, String island) throws NotSupportIslandException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        try {
			handler.executeNotQueryPostgreSQL(String.format(DELETE, island, query));
			return true;
		} catch (SQLException e) {
			return false;
		}
    }

    public static void runBenchmarks(List<QueryExecutionPlan> qeps) throws Exception {
        for (QueryExecutionPlan qep: qeps) {
            Executor.executePlan(qep);
        }
    }

    public void finishedBenchmark(QueryExecutionPlan qep, long startTime, long endTime) throws SQLException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        handler.executeStatementPostgreSQL(String.format(UPDATE, endTime, endTime-startTime, qep.getIsland(), QueryExecutionPlan.qepToString(qep)));
    }
}
