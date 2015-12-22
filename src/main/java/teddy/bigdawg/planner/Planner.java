package teddy.bigdawg.planner;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.core.Response;

import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import teddy.bigdawg.catalog.Catalog;
import teddy.bigdawg.executor.Executor;
import teddy.bigdawg.packages.QueriesAndPerformanceInformation;
import teddy.bigdawg.parsers.UserQueryParser;

public class Planner {

	/*
	To summarize (what I think is) the work flow, 

	1. Planner receives the query and calls a monitor function to deliver alternative subqueries;
	2. Monitor compiles an performance information and calls a planner function to deliver it;
	3. Planner comes up with a series of subqueries that represents the best cross-island join-order and call executor to execute each sub-query;
	4. Executor finds the best way to do each sub-query, execute everything locally, call the migrator to move finished cross-island intermediate results, and repeat;
	5. Executor calls planner to deliver the final result back to user.
	* if a query engine or a migrator is down and beyond localized recovery, executor and migrator should report to user. 
	
	Note: Let's assume that each query has a serial number or identifier associated with it.

	UPDATE, IMPORTANT:
	To have this thing going, we need a PostgreSQL database named "bigdawg" to house the Catalog.
	Also, to make it work, we'll need to run CatalogInitiator.createSchemaAndRelations(Catalog cc) to create the schema, relation, etc.
	Only then we can start using this Planner.
	
	Also, currently we assume that the query we run is wrapped in "bdrel(...)", and runs on a single query, and no intermediate result. 

	//*/
	
	private static LinkedList<ArrayList<String>> 	queryQueue = new LinkedList<ArrayList<String>>();  // queries to be executed
	private static LinkedList<Integer>				serialQueue = new LinkedList<Integer>(); // used to store serial number
	private static Integer							maxSerial = 0;
	private static Catalog 							catalog;
	
	public static void terminate() throws Exception {
		//CatalogInitiator.close(catalog);
	}
	
	public static Response processQuery(String userinput) throws Exception {
		System.out.printf("[BigDAWG] PLANNER: User query received.\n");
		
		getGetPerformanceAndPickTheBest(userinput);
		
		// now the serial number of query is added;
		int querySerial 	= serialQueue.getLast();
		int queryPosition 	= serialQueue.size() - 1;
		int subqueryPos 	= 0;
		
		// Execute and store results
		ArrayList<QueryResult> resps = new ArrayList<QueryResult>();
		int subqueryCount 			 = queryQueue.get(queryPosition).size();
		while (subqueryCount > subqueryPos) {
			resps.add(executeOneSubquery(querySerial, subqueryPos)); // store some results
			System.out.printf("[BigDAWG] PLANNER: Sub-query %d of query %d executed.\n", subqueryPos, querySerial);
			subqueryPos += 1;
		}
		
		// compile and return results
		return compileResults(querySerial, resps);
	}
	
	/**
	 * CALL MONITOR: Parses the userinput, generate alternative join plans, and GIVE IT TO MONITOR
	 * Note: this generates the result 
	 * @param userinput
	 * @return 0 if no error; otherwise incomplete
	 */
	public static void getGetPerformanceAndPickTheBest(String userinput) throws Exception {
		// calls a monitor function
		
		System.out.printf("[BigDAWG] PLANNER: parsing query to generate signatures...\n");
		ArrayList<String> crossIslandQuery 	= UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
		ArrayList<Object> sigAndCasts		= UserQueryParser.getSignaturesAndCasts(catalog, crossIslandQuery);

		// generate multiple shipping plan
		ArrayList<ArrayList<Object>> permuted = new ArrayList<ArrayList<Object>> ();
		permuted.add(sigAndCasts);
		
		// now call the corresponding monitor function to deliver permuted. Today there are no multiple plans
		QueriesAndPerformanceInformation qnp = Monitor.getBenchmarkPerformance(permuted); // TODO RETURN SOMETHING?
				
		// does some magic to pick out the best query, store it to the query plan queue
		maxSerial += 1;
		queryQueue.addLast(qnp.qList.get(0)); // TODO change this zero.
		serialQueue.addLast(maxSerial);
		
		System.out.printf("[BigDAWG] PLANNER: Performance information received; serial number: %d.\n", maxSerial);
	};


	/** CALL EXECUTOR OR MIGRATOR: evoked by planner itself. Look for the query and execute the first sub-query
	 * @return 0 if no error; otherwise incomplete
	 */
	private static QueryResult executeOneSubquery(int querySerial, int subqueryPos) throws Exception { 
		// call either an executor or migrator function
		
		int queryPosition 	= serialQueue.indexOf(querySerial);
		String subQ			= queryQueue.get(queryPosition).get(subqueryPos);
		
		System.out.printf("[BigDAWG] PLANNER: dispatching sub-query %d of query %d...\n", subqueryPos, querySerial);
		return Executor.executeDSA(querySerial, subqueryPos, subQ);
	}

	
	/**
	 * CALLED BY EXECUTOR: Receive result and send it to user  
	 * @param querySerial
	 * @param result
	 * @return 0 if no error; otherwise incomplete
	 */
	public static Response compileResults(int querySerial, ArrayList<QueryResult> result) {
		System.out.printf("[BigDAWG] PLANNER: Query %d is completed. Result:\n", querySerial);

		// remove corresponding elements in the two queues
		 queryQueue.remove(serialQueue.indexOf(querySerial));
		serialQueue.remove((Integer)querySerial);
		
		// print the result;
		StringBuffer out 		= new StringBuffer();
		List<List<String>> rows = result.get(0).getRows();
		List<String> cols 		= result.get(0).getColNames();
		
		for (String name : cols) {
			out.append("\t"+name);
		}
		out.append("\n");
		int rowCounter = 1;
		for (List<String> row : rows) {
			out.append(rowCounter + ".");
			for (String s : row) {
				out.append("\t" + s);
			}
			out.append("\n");
			rowCounter += 1;
		}
		
		return Response.status(200).entity(out.toString()).build();
	}

}
