package teddy.bigdawg.executor;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.Migrator;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import istc.bigdawg.query.ConnectionInfo;
import teddy.bigdawg.executor.plan.ExecutionNode;
import teddy.bigdawg.executor.plan.LocalQueryExecutionNode;
import teddy.bigdawg.executor.plan.QueryExecutionPlan;

/**
 * TODO:
 *  parallel/asynchronous query/migration execution
 *  fully abstracted DbHandlers instead of casting to PostgreSQL*
 *  handle errors better
 * @author ankush
 *
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
	public static QueryResult executePlan(QueryExecutionPlan plan) throws SQLException {
		// TODO(ankush) proper logging for query plan executions
	    long start = System.currentTimeMillis();
	    
	    System.out.printf("[BigDAWG] EXECUTOR: executing query %s...\n", plan);

		// maps nodes to a list of all engines on which they are stored
		Map<ExecutionNode, Set<ConnectionInfo>> mapping = new HashMap<>();

		// Iterate through the plan in topological order
		for (ExecutionNode node : plan) {
	       System.out.printf("[BigDAWG] EXECUTOR: examining query node %s...\n", node);
			if (node instanceof LocalQueryExecutionNode) {
				// migrate dependencies to the proper engine
				plan.getDependencies(node).stream()
						// filter out dependencies already on proper engine
						.filter(d -> !mapping.get(d).contains(node.getEngine()))
						.forEach(m -> {
							// migrate to node.getEngine()
							try {
						        System.out.printf("[BigDAWG] EXECUTOR: migrating dependency %s from %s to %s...\n", m, m.getEngine(), node.getEngine());
								Migrator.migrate(m.getEngine(), m.getTableName().get(), node.getEngine(),
										m.getTableName().get());
								
	                            // update mapping for future nodes with the same
	                            // dependencies
	                            mapping.putIfAbsent(m, new HashSet<>());
	                            mapping.get(m).add(node.getEngine());
							} catch (MigrationException e) {
								// TODO handle errors in migration properly
								e.printStackTrace();
							}
						});

				QueryResult result = null;

				if (node.getQueryString().isPresent()) {
					if (node.getEngine() instanceof PostgreSQLConnectionInfo) {
					    System.out.printf("[BigDAWG] EXECUTOR: executing query node %s...\n", node);
						PostgreSQLHandler handler = new PostgreSQLHandler((PostgreSQLConnectionInfo) node.getEngine());
						result = handler.executeQueryPostgreSQL(node.getQueryString().get());
					} else {
						throw new IllegalArgumentException("Node engine was not PostgreSQL");
					}
				}

				// if no output table, must be the final result node
				if (!node.getTableName().isPresent()) {
				    Map<ConnectionInfo, Set<String>> oldTables = new HashMap<>();
				    
				    for(ExecutionNode oldTable : mapping.keySet()) {
				        for(ConnectionInfo c : mapping.get(oldTable)) {
				            oldTables.putIfAbsent(c, new HashSet<>());
				            oldTable.getTableName().ifPresent(oldTables.get(c)::add);
				        }
				    }
				    
				    for(ConnectionInfo c : oldTables.keySet()) {
                        System.out.printf("[BigDAWG] EXECUTOR: cleaning up %s by removing %s...\n", c, oldTables.get(c));
				        ((PostgreSQLHandler) c.getHandler()).executeQueryPostgreSQL(c.getCleanupQuery(oldTables.get(c)));
				    }
				    
				    long end = System.currentTimeMillis();
                    
				    System.out.printf("[BigDAWG] EXECUTOR: finished executing %s, in %d seconds.\n", plan, (start - end)/1000);
				    System.out.printf("[BigDAWG] EXECUTOR: sending timing to monitor...");
				    monitor.finishedBenchmark(plan, start, end);
				    
				    System.out.printf("[BigDAWG] EXECUTOR: Returning result to planner...");
					return result;
				}
			}

			mapping.putIfAbsent(node, new HashSet<>());
			mapping.get(node).add(node.getEngine());
		}

		return null;
	}

	public static QueryResult executeDSA(int querySerial, int subqueryPos, String dsa) throws SQLException {
		System.out.printf("[BigDAWG] EXECUTOR: executing sub-query %d of query %d...\n", subqueryPos, querySerial);
		return (new PostgreSQLHandler()).executeQueryPostgreSQL(dsa);
	}
}
