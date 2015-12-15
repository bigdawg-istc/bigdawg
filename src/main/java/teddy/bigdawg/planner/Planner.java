package teddy.bigdawg.planner;

import java.util.ArrayList;
import java.util.LinkedList;

import istc.bigdawg.monitoring.Monitor;
import teddy.bigdawg.catalog.Catalog;
import teddy.bigdawg.catalog.CatalogInitiator;
import teddy.bigdawg.parsers.UserQueryParser;
import teddy.bigdawg.signature.Signature;

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

	//*/
	
	private static LinkedList<ArrayList<String>> 	queryQueue;  // queries to be executed
	private static LinkedList<Integer>				serialQueue; // used to store serial number
	private static Integer							maxSerial;
	private static Catalog 							catalog;

	
	public Planner (String address, String username, String password) throws Exception {
		maxSerial 		= 0;
		queryQueue 		= new LinkedList<ArrayList<String>>();
		serialQueue 	= new LinkedList<Integer>();
		catalog 		= new Catalog();
		CatalogInitiator.connect(catalog, address, username, password);
	}
	
	public static void terminate() throws Exception {
		CatalogInitiator.close(catalog);
	}
	
	/**
	 * CALL MONITOR: Parses the userinput, generate alternative join plans, and GIVE IT TO MONITOR
	 * Note: this generates the result 
	 * @param userinput
	 * @return 0 if no error; otherwise incomplete
	 */
	public int getAlternativeSubqueriesAndSendToMonitor (String userinput) {
		// calls a monitor function
		
		try {
			
			System.out.printf("[BigDAWG] PLANNER: Received user input. Next step: deliver parsed results to monitor.\n");
			ArrayList<String> crossIslandQuery 	= UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
			ArrayList<Object> sigAndCasts		= UserQueryParser.getSignaturesAndCasts(catalog, crossIslandQuery);
	
			// generate multiple shipping plan
			ArrayList<ArrayList<Object>> permuted = new ArrayList<ArrayList<Object>> ();
			permuted.add(sigAndCasts);
			
			// now call the corresponding monitor function to deliver permuted. Today there are no multiple plans
			Monitor.getBenchmarkPerformance(permuted);
			
		} catch  (Exception e) {
			e.printStackTrace();
			return 1;
		}
		
		return 0;
	};

	/**
	 * CALLED BY MONITOR: get performance information along with the queries, compute and pick plan, then put it to queryQueue to be executed.
	 * ArrayList<Object> is used because we haven't agreed on what the monitor will return at this moment.
	 * @param query, perfInfo - ArrayList of String, Float, Integer, Double, Boolean, etc.
	 * @return 0 if no error; otherwise incomplete
	 */
	public static int deliverPerformanceInformation(ArrayList<ArrayList<String>> query, ArrayList<Object> perfInfo) {
		// integrate p
		int subqueryPos = 0;
		int querySerial = 0;
		
		System.out.printf("[BigDAWG] PLANNER: Performance information delivered from monitor. Next step: execute a sub-query.\n");
		
		// does some magic to pick out the best query, store it to the query plan queue
		maxSerial += 1;
		queryQueue.addLast(query.get(0)); // TODO change this.
		serialQueue.addLast(maxSerial);
		
		executeOneSubquery(querySerial, subqueryPos);
		
		return 0;
	};

	/** CALL EXECUTOR OR MIGRATOR: evoked by planner itself. Look for the query and execute the first sub-query
	 * @return 0 if no error; otherwise incomplete
	 */
	private static int executeOneSubquery(int querySerial, int subqueryPos){ 
		// call either an executor or migrator function
		System.out.printf("[BigDAWG] PLANNER: Sub-query %d dispatched.\n", subqueryPos);
		
		// call them
		
		return 0;
	}

	/**
	 * CALLED BY EXECUTOR OR MIGRATOR: acknowledge completion of a sub-query; remove the sub-query from the query in queryQueue. 
	 * @param subquerySerial
	 * @param querySerial
	 * @return 0 if no error; otherwise incomplete
	 */
	public static int finishedOneSubquery(int subqueryPos, int querySerial) { 
		// remove the sub-query from the query list, call executeOneSubquery
		
		System.out.printf("[BigDAWG] PLANNER: Completion of sub-query %d of query %d acknowledged. Next step: check if any sub-query remains.\n", subqueryPos, querySerial);
		
		// assume environment benign
		int queryPosition 	= serialQueue.indexOf(querySerial);
		int querySize		= queryQueue.get(queryPosition).size();
		if (subqueryPos < querySize) {
			executeOneSubquery(querySerial, subqueryPos + 1);
		} else { 
			System.out.printf("[BigDAWG] PLANNER: No more sub-queries. Next step: Asking for result.\n");
			askForResult(querySerial);
		}
		
		return 0;
	}

	/**
	 * CALL EXECUTOR: ask for the final result of a query. Is this function necessary?
	 * @param querySerial
	 * @return 0 if no error; otherwise incomplete
	 */
	public static int askForResult(int querySerial) {
		// call executor to ask for result with regard to query
		System.out.printf("[BigDAWG] PLANNER: Result of query %d inquired.\n", querySerial);
		
		// call executor
		
		return 0;
	}
	
	/**
	 * CALLED BY EXECUTOR: Receive result and send it to user  
	 * @param querySerial
	 * @param result
	 * @return 0 if no error; otherwise incomplete
	 */
	public static int receiveResult(int querySerial, ArrayList<String> result) {
		System.out.printf("[BigDAWG] PLANNER: Query %d is completed. Result:\n", querySerial);

		// remove corresponding elements in the two queues
		queryQueue.remove(serialQueue.indexOf(querySerial));
		serialQueue.remove((Integer)querySerial);
		
		// print the result;
		return 0;
	}

}
