package istc.bigdawg.executor;

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
import istc.bigdawg.executor.plan.ExecutionNode;
import istc.bigdawg.executor.plan.LocalQueryExecutionNode;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.executor.plan.TableExecutionNode;

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
	    
	    log.debug(String.format("Executing query %s...", plan));
	    
		// maps nodes to a list of all engines on which they are stored
		Map<ExecutionNode, Set<ConnectionInfo>> mapping = new HashMap<>();
		Map<ExecutionNode, Set<ConnectionInfo>> permObjects = new HashMap<>();

		// Iterate through the plan in topological order
		for (ExecutionNode node : plan) {
	       log.debug(String.format("Examining query node %s...", node));
	       if (node instanceof TableExecutionNode) {
	    	   permObjects.putIfAbsent(node, new HashSet<>());
	    	   permObjects.get(node).add(node.getEngine());
	       }
			if (node instanceof LocalQueryExecutionNode) {
				// migrate dependencies to the proper engine
				plan.getDependencies(node).stream()
						// filter out dependencies already on proper engine
						.filter(d -> !mapping.get(d).contains(node.getEngine()))
						.forEach(m -> {
							// migrate to node.getEngine()
							try {
						        log.debug(String.format("Migrating dependency %s from %s to %s...", m, m.getEngine(), node.getEngine()));
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
					    log.debug(String.format("Executing query node %s...", node));
						PostgreSQLHandler handler = new PostgreSQLHandler((PostgreSQLConnectionInfo) node.getEngine());
						result = handler.executeQueryPostgreSQL(node.getQueryString().get());
					} else {
						throw new IllegalArgumentException("Node engine was not PostgreSQL");
					}
				}

				// if no output table, must be the final result node
				if (node.getTableName().get().equals("BIGDAWG_MAIN")) {
				    Map<ConnectionInfo, Set<String>> removingTables = new HashMap<>();
				    
				    
				    
				    
				    for(ExecutionNode aTable : mapping.keySet()) {
				        for(ConnectionInfo c : mapping.get(aTable)) {
				        	
				        	if (permObjects.get(aTable) != null && permObjects.get(aTable).contains(c)) 
				        		continue;
				        	
				        	removingTables.putIfAbsent(c, new HashSet<>());
				            aTable.getTableName().ifPresent(removingTables.get(c)::add);
				        }
				    }
				    
//				    for(ExecutionNode oldTable : mapping.keySet()) {
//				        for(ConnectionInfo c : mapping.get(oldTable)) {
//				            oldTables.putIfAbsent(c, new HashSet<>());
//				            oldTable.getTableName().ifPresent(oldTables.get(c)::add);
//				        }
//				    }
//				    oldTables.remove(node.getEngine());
				    
				    
				    for(ConnectionInfo c : removingTables.keySet()) {
                        log.debug(String.format("Cleaning up %s by removing %s...", c, removingTables.get(c)));
//				        ((PostgreSQLHandler) c.getHandler()).executeQueryPostgreSQL(c.getCleanupQuery(removingTables.get(c)));
				        ((PostgreSQLHandler) c.getHandler()).executeNotQueryPostgreSQL(c.getCleanupQuery(removingTables.get(c)));
				    }
				    
				    long end = System.currentTimeMillis();
                    
				    log.debug(String.format("Finished executing %s, in %d seconds.", plan, (start - end)/1000));
		    		log.debug(String.format("Sending timing to monitor..."));
				    monitor.finishedBenchmark(plan, start, end);
				    
				    log.debug(String.format("Returning result to planner..."));
					return result;
				}
			}

			mapping.putIfAbsent(node, new HashSet<>());
			mapping.get(node).add(node.getEngine());
		}

		return null;
	}

	public static QueryResult executeDSA(int querySerial, int subqueryPos, String dsa) throws SQLException {
		log.debug(String.format("Executing sub-query %d of query %d...", subqueryPos, querySerial));
		return (new PostgreSQLHandler()).executeQueryPostgreSQL(dsa);
	}
}
