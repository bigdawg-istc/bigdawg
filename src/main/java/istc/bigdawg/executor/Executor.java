package istc.bigdawg.executor;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.ExecutionNode;
import istc.bigdawg.executor.plan.LocalQueryExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.migration.Migrator;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

/**
 * TODO:
 *   multithreaded execution of a single plan + any necessary thread safety
 *   fully abstracted DbHandlers instead of casting to PostgreSQL
 *   shuffle joins
 *   better exception/error handling in the event of failure
 * 
 * @author ankush
 */
public class Executor {

    static Logger log = Logger.getLogger(Executor.class.getName());
    static final Monitor monitor = new Monitor();

    /**
     * Execute a given query plan, and return the result
     *
     * @param plan
     *            a data structure of the queries to be run and their ordering,
     *            with edges pointing to dependencies
     */
    public static QueryResult executePlan(QueryExecutionPlan plan) throws SQLException, MigrationException {
        long start = System.currentTimeMillis();

        log.debug(String.format("Executing query %s...", plan.getSerializedName()));

        // maps nodes to a list of all engines on which they are stored
        Map<ExecutionNode, Set<ConnectionInfo>> mapping = new HashMap<>();
        Map<ConnectionInfo, Set<String>> temporaryObjects = new HashMap<>();

        // Iterate through the plan in topological order
        for (ExecutionNode node : plan) {            
            log.debug(String.format("Examining query node %s...", node.getTableName()));

            if (node instanceof LocalQueryExecutionNode) {
                // colocate dependencies to single engine
                plan.getDependencies(node).stream()
                        // filter out dependencies already on proper engine
                        .filter(d -> !mapping.get(d).contains(node.getEngine()))
                        .forEach(m -> {
                            // migrate to node.getEngine()
                            m.getTableName().ifPresent((table) -> {
                                log.debug(String.format("Migrating %s from %s to %s...", table, m.getEngine(),
                                        node.getEngine()));
                                try {
                                    Migrator.migrate(m.getEngine(), table, node.getEngine(), table);
                                    
                                    // don't migrate to the same node again
                                    mapping.putIfAbsent(m, new HashSet<>());
                                    mapping.get(m).add(node.getEngine());

                                    // clean up the migrated data later
                                    temporaryObjects.putIfAbsent(node.getEngine(), new HashSet<>());
                                    temporaryObjects.get(node.getEngine()).add(table);
                                } catch (MigrationException e) {
                                    log.error("Error migrating dependency!", e);
                                }
                            });
                        });

                Optional<QueryResult> result = node.getQueryString().map((query) -> {
                    log.debug(String.format("Executing %s...", node.getTableName()));
                    PostgreSQLHandler handler = new PostgreSQLHandler((PostgreSQLConnectionInfo) node.getEngine());

                    try {
                        if (plan.getTerminalTableNode().equals(node)) {
                            // terminal node is a SELECT query
                            return handler.executeQueryPostgreSQL(query);
                        } else {
                            // SELECT INTO statements don't return QueryResults
                            handler.executeNotQueryPostgreSQL(query);
                            
                            // clean up the intermediate table later
                            node.getTableName().ifPresent((table) -> {
                                temporaryObjects.putIfAbsent(node.getEngine(), new HashSet<>());
                                temporaryObjects.get(node.getEngine()).add(table);
                            });
                        }
                        
                        // update mapping now that query has been executed
                        mapping.putIfAbsent(node, new HashSet<>());
                        mapping.get(node).add(node.getEngine());
                    } catch (SQLException e) {
                        log.error(String.format("Error executing %s", node), e);
                    }

                    return null;
                });

                if (plan.getTerminalTableNode().equals(node)) {
                    cleanTemporaryTables(temporaryObjects);

                    long end = System.currentTimeMillis();

                    log.debug(String.format("Finished executing %s, in %d seconds.", plan.getSerializedName(), (start - end) / 1000));
                    log.debug(String.format("Sending timing to monitor..."));
                    monitor.finishedBenchmark(plan, start, end);

                    log.debug(String.format("Returning result to planner..."));
                    return result.orElse(null);
                }
            }
        }

        return null;
    }

    private static void cleanTemporaryTables(Map<ConnectionInfo, Set<String>> temporaryTables) throws SQLException {
        for (ConnectionInfo c : temporaryTables.keySet()) {
            log.debug(String.format("Cleaning up %s by removing %s...", c, temporaryTables.get(c)));
            ((PostgreSQLHandler) c.getHandler()).executeNotQueryPostgreSQL(c.getCleanupQuery(temporaryTables.get(c)));
        }
    }
    
    
    @Deprecated
    public static QueryResult executeDSA(int querySerial, int subqueryPos, String dsa) throws SQLException {
        log.debug(String.format("Executing sub-query %d of query %d...", subqueryPos, querySerial));
        return (new PostgreSQLHandler()).executeQueryPostgreSQL(dsa);
    }
}
