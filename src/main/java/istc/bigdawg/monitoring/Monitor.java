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
import java.util.Map;

public class Monitor {
    private static final String INSERT = "INSERT INTO monitoring(island, query, lastRan, duration) VALUES (%s, %s, -1, -1)";
    private static final String DELETE = "DELETE FROM monitoring WHERE island='%s' AND query='%s'";
    private static final String UPDATE = "UPDATE monitoring SET lastRan=%d, duration=%d WHERE island='%s' AND query='%s'";
    private static final String RETRIEVE = "SELECT * FROM monitoring WHERE island=%s AND query='%s'";

    public static boolean addBenchmark(ArrayList<ArrayList<Object>> permuted, boolean lean) {
        BDConstants.Shim[] shims = BDConstants.Shim.values();
        return addBenchmark(permuted, lean, shims);
    }

    public static boolean addBenchmark(ArrayList<ArrayList<Object>> permuted, boolean lean, BDConstants.Shim[] shims) {
        for (ArrayList<Object> currentList: permuted) {
            for (Object currentQuery: currentList) {
                if (currentQuery instanceof Signature) {
                    // TODO Convert the query for each shim
                    // Repeat for each of the converted queries
                    try {
                        if (!insert(((Signature) currentQuery).getQuery())) {
                            return false;
                        }
                    } catch (NotSupportIslandException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        if (!lean) {
            try {
                runBenchmark(permuted);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public static boolean removeBenchmark(ArrayList<ArrayList<Object>> permuted) {
        return removeBenchmark(permuted, false);
    }

    public static boolean removeBenchmark(ArrayList<ArrayList<Object>> permuted, boolean removeAll) {
        for (ArrayList<Object> currentList: permuted) {
            for (Object currentQuery: currentList) {
                if (currentQuery instanceof Signature) {
                    // TODO Convert the query for each shim
                    // Repeat for each of the converted queries
                    try {
                        if (!delete(((Signature) currentQuery).getQuery())) {
                            return false;
                        }
                    } catch (NotSupportIslandException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return true;
    }

    public static QueriesAndPerformanceInformation getBenchmarkPerformance(ArrayList<ArrayList<Object>> permuted) throws NotSupportIslandException {
        ArrayList<String> queries = new ArrayList<>();
        ArrayList<Object> perfInfo = new ArrayList<>();

        for (ArrayList<Object> currentList: permuted) {
            for (Object currentQuery: currentList) {
                if (currentQuery instanceof Signature) {
                    // TODO match to nearest existing benchmark
                    queries.add(((Signature) currentQuery).getQuery());

                    PostgreSQLHandler handler = new PostgreSQLHandler();
                    Parser parser = new simpleParser();
                    ASTNode node = parser.parseQueryIntoTree(((Signature) currentQuery).getQuery());
                    perfInfo.add(handler.executeQuery(String.format(RETRIEVE, node.getIsland().name(), node.getTarget())));
                }
            }
        }
        ArrayList<ArrayList<String>> queryList = new ArrayList();
        queryList.add(queries);
        System.out.printf("[BigDAWG] MONITOR: Performance information generated.\n");
        return new QueriesAndPerformanceInformation(queryList, perfInfo);
    }

    private static boolean insert(String query) throws NotSupportIslandException {
        Parser parser = new simpleParser();
        ASTNode node = parser.parseQueryIntoTree(query);

        PostgreSQLHandler handler = new PostgreSQLHandler();
        Response response = handler.executeQuery(String.format(INSERT, node.getIsland().name(), node.getTarget()));
        return response.getStatus() == 200;
    }

    private static boolean delete(String query) throws NotSupportIslandException {
        Parser parser = new simpleParser();
        ASTNode node = parser.parseQueryIntoTree(query);

        PostgreSQLHandler handler = new PostgreSQLHandler();
        Response response = handler.executeQuery(String.format(DELETE, node.getIsland().name(), node.getTarget()));
        return response.getStatus() == 200;
    }

    public static void runBenchmark(ArrayList<ArrayList<Object>> permuted) throws Exception {
        for (ArrayList<Object> currentList: permuted) {
            for (Object currentQuery: currentList) {
                if (currentQuery instanceof Signature) {
                    String query = ((Signature) currentQuery).getQuery();
                    // TODO match to nearest existing benchmark

                    // This is from the Planner's processQuery function
                    PostgreSQLHandler psqlh = new PostgreSQLHandler(0, 3);
                    SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, (query));
                    Operator root = queryPlan.getRootNode();

                    ArrayList<String> objs = new ArrayList<>(Arrays.asList(((Signature) currentQuery).getSig2().split("\t")));
                    Map<String, ArrayList<String>> map = CatalogViewer.getDBMappingByObj(CatalogInstance.INSTANCE.getCatalog(), objs);

                    ArrayList<String> root_dep = new ArrayList<String>();
                    root_dep.addAll(root.getTableLocations(map).keySet());
                    map.put("BIGDAWG_MAIN", root_dep);

                    QueryExecutionPlan qep = new QueryExecutionPlan();
                    Map<String, Operator> out =  ExecutionNodeFactory.traverseAndPickOutWiths(root, queryPlan);
                    ExecutionNodeFactory.addNodesAndEdges(qep, map, out, queryPlan.getStatement());

                    qep.setQueryId(query);
                    qep.setIsland(((Signature) currentQuery).getIsland());

                    Executor.executePlan(qep);
                }
            }
        }
    }

    public void finishedBenchmark(QueryExecutionPlan plan, long startTime, long endTime) throws SQLException {
        PostgreSQLHandler handler = new PostgreSQLHandler();
        handler.executeNotQueryPostgreSQL(String.format(UPDATE, endTime, endTime-startTime, plan.getIsland(), plan.getQueryId()));
    }
}
