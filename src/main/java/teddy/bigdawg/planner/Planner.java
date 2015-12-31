package teddy.bigdawg.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import teddy.bigdawg.cast.Cast;
import teddy.bigdawg.catalog.CatalogInstance;
import teddy.bigdawg.executor.Executor;
import teddy.bigdawg.packages.QueriesAndPerformanceInformation;
import teddy.bigdawg.parsers.UserQueryParser;
import teddy.bigdawg.signature.Signature;

public class Planner {

	/*
	 * 
	 */

	private static LinkedHashMap<Integer, ArrayList<String>> queryQueue = new LinkedHashMap<Integer, ArrayList<String>>(); // queries
																									// to
																									// be
																									// executed
	private static Integer maxSerial = 0;


	public static Response processQuery(String userinput) throws Exception {
		
		
		// UNROLLING
		System.out.printf("[BigDAWG] PLANNER: User query received. Parsing...\n");
		ArrayList<String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
		
		
		// GET SIGNATURE AND CASTS
		System.out.printf("[BigDAWG] PLANNER: generating signatures and casts...\n");
		Map<String, Object> sigAndCasts = UserQueryParser.getSignaturesAndCasts(CatalogInstance.INSTANCE.getCatalog(), crossIslandQuery);
		
		
		// POPULATE queryQueue WITH OPTIMAL PLANS
		getGetPerformanceAndPickTheBest(sigAndCasts);

		// now the serial number of query is added;
		int querySerial = maxSerial; // THIS IS A VERY STRONG ASSUMPTION IN MULTI-THREAD. NOT A PROBLEM AT THE MOMENT TODO
		int subqueryPos = 0;

		// EXECUTE TEH RESULT
		ArrayList<QueryResult> resps = new ArrayList<QueryResult>();
		int subqueryCount = queryQueue.get(querySerial).size();
		while (subqueryCount > subqueryPos) {
			resps.add(executeOneSubquery(querySerial, subqueryPos)); // store
																		// some
																		// results
			System.out.printf("[BigDAWG] PLANNER: Sub-query %d of query %d executed.\n", subqueryPos, querySerial);
			subqueryPos += 1;
		}

		// compile and return results
		return compileResults(querySerial, resps);
	}

	/**
	 * CALL MONITOR: Parses the userinput, generate alternative join plans, and
	 * GIVE IT TO MONITOR Note: this generates the result
	 * 
	 * @param userinput
	 * @return 0 if no error; otherwise incomplete
	 */
	public static void getGetPerformanceAndPickTheBest(Map<String, Object> sigAndCasts) throws Exception {
		
		// GENERATE PERMUTATIONS
		// TODO make trees and NOT list of signature and casts; how about changing signature 1 into execution plans?
		ArrayList<ArrayList<Object>> permuted = new ArrayList<ArrayList<Object>>();
		permuted.add(new ArrayList<Object>(sigAndCasts.values()));
		// 
		// 
		// CHEAT: JUST ONE 
		// TODO DON'T CHEAT

		// now call the corresponding monitor function to deliver permuted.
		// Today there IS ONLY ONE PLAN
		QueriesAndPerformanceInformation qnp = Monitor.getBenchmarkPerformance(permuted); // TODO CHANGE THE MONITOR FUNCTION
		
		// does some magic to pick out the best query, store it to the query plan queue
		// CHEAT: JUST ONE
		// TODO DON'T CHEAT; ACTUALLY PICK; CHANGE THIS 0
		maxSerial += 1;
		queryQueue.put(maxSerial, qnp.qList.get(0)); 

		System.out.printf("[BigDAWG] PLANNER: Performance information received; serial number: %d\n", maxSerial);
		System.out.printf("[BigDAWG] PLANNER: Query chosen: "+queryQueue.get(0).toString() + "\n");
	};

	/**
	 * CALL EXECUTOR OR MIGRATOR: evoked by planner itself. Look for the query
	 * and execute the first sub-query
	 * 
	 * @return 0 if no error; otherwise incomplete
	 */
	private static QueryResult executeOneSubquery(int querySerial, int subqueryPos) throws Exception {
		// call either an executor or migrator function

		String subQ = queryQueue.get(querySerial).get(subqueryPos);

		System.out.printf("[BigDAWG] PLANNER: dispatching sub-query %d of query %d...\n", subqueryPos, querySerial);
		return Executor.executeDSA(querySerial, subqueryPos, subQ);
	}

	/**
	 * CALLED BY EXECUTOR: Receive result and send it to user
	 * 
	 * @param querySerial
	 * @param result
	 * @return 0 if no error; otherwise incomplete
	 */
	public static Response compileResults(int querySerial, ArrayList<QueryResult> result) {
		System.out.printf("[BigDAWG] PLANNER: Query %d is completed. Result:\n", querySerial);

		// remove corresponding elements in the two queues
		queryQueue.remove(querySerial);

		// print the result;
		StringBuffer out = new StringBuffer();
		List<List<String>> rows = result.get(0).getRows();
		List<String> cols = result.get(0).getColNames();

		for (String name : cols) {
			out.append("\t" + name);
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
