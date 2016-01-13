package istc.bigdawg.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import net.sf.jsqlparser.statement.select.Select;
import istc.bigdawg.cast.Cast;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.plan.ExecutionNode;
import istc.bigdawg.executor.plan.ExecutionNodeFactory;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.packages.QueriesAndPerformanceInformation;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.schema.SQLDatabaseSingleton;
import istc.bigdawg.signature.Signature;

public class Planner {

	/*
	 * 
	 */
	private static Logger logger = Logger.getLogger(Planner.class.getName());
	private static LinkedHashMap<Integer, ArrayList<String>> queryQueue = new LinkedHashMap<Integer, ArrayList<String>>(); // queries
																									// to
																									// be
																									// executed
	private static Integer maxSerial = 0;


	public static Response processQuery(String userinput) throws Exception {
		
		SQLDatabaseSingleton.getInstance().setDatabase("bigdawg_schemas", "src/main/resources/plain.sql");
				
		
		// UNROLLING
		logger.debug("User query received. Parsing...");
		Map<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
		
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);

		// pass this to monitor, and pick your favorite
		
		
		// currently there should be just one island, therefore one child, root.
		QueryExecutionPlan qep = ciqp.getRoot().getQEP(0);
		
		
		
//		System.out.println("QueryExecutionPlan:: ");
//		for (ExecutionNode v : qep.vertexSet()) {
//			System.out.print(v.getTableName()+"\t\t----- "+ v.getQueryString()+"\n");
//		};
//		
		
		// EXECUTE TEH RESULT
		logger.debug("Executing query execution tree...");
		return compileResults(ciqp.getSerial(), Executor.executePlan(qep));
	}

	
	/**
	 * CALL MONITOR: Parses the userinput, generate alternative join plans, and
	 * GIVE IT TO MONITOR Note: this generates the result
	 * 
	 * @param userinput
	 */
	public static void getGetPerformanceAndPickTheBest(Map<String, Object> sigAndCasts) throws Exception {
		
		// GENERATE PERMUTATIONS
		// TODO make trees and NOT list of signature and casts; how about changing signature 1 into execution plans?
		ArrayList<ArrayList<Object>> permuted = new ArrayList<ArrayList<Object>>();
		permuted.add(new ArrayList<Object>(sigAndCasts.values())); // this should be qeps
		//
		// 
		// CHEAT: JUST ONE 
		// TODO DON'T CHEAT

		// now call the corresponding monitor function to deliver permuted.
		// Today there IS ONLY ONE PLAN
		// TODO generate list of QueryExecutionPlans to send to monitor
		ArrayList<QueryExecutionPlan> qeps = new ArrayList<>();
		QueriesAndPerformanceInformation qnp = Monitor.getBenchmarkPerformance(qeps); // TODO CHANGE THE QEPS SENT TO MONITOR FUNCTION
		
		// does some magic to pick out the best query, store it to the query plan queue
		// CHEAT: JUST ONE
		// TODO DON'T CHEAT; ACTUALLY PICK; CHANGE THIS 0
		maxSerial += 1;
		queryQueue.put(maxSerial, (ArrayList<String>) qnp.qList.get(0)); // TODO 
		
		logger.debug("Performance information received; serial number: "+maxSerial);
	};


	/**
	 * CALLED BY EXECUTOR: Receive result and send it to user
	 * 
	 * @param querySerial
	 * @param result
	 * @return 0 if no error; otherwise incomplete
	 */
	public static Response compileResults(int querySerial, QueryResult result) {
		System.out.printf("[BigDAWG] PLANNER: Query %d is completed. Result:\n", querySerial);

		// remove corresponding elements in the two queues
		queryQueue.remove(querySerial);

		// print the result;
		StringBuffer out = new StringBuffer();
		List<List<String>> rows = result.getRows();
		List<String> cols = result.getColNames();

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
		System.out.println(out);
		return Response.status(200).entity(out.toString()).build();
	}

	
	public static void permuteOperators(Operator root) throws Exception {
		if (root instanceof SeqScan) {
			
		} else if (root instanceof Join) {
			
		} else {
			throw new Exception ("Unsupported Operator!!");
		}
	}
}
