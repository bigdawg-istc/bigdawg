package istc.bigdawg.planner;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.packages.CrossIslandQueryNode;
import istc.bigdawg.packages.CrossIslandQueryPlan;
import istc.bigdawg.packages.QueriesAndPerformanceInformation;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.postgresql.PostgreSQLHandler.QueryResult;
import istc.bigdawg.schema.SQLDatabaseSingleton;

public class Planner {

	private static Logger logger = Logger.getLogger(Planner.class.getName());
//	private static LinkedHashMap<Integer, List<String>> queryQueue = new LinkedHashMap<>(); // queries
//																									// to
//																									// be
//																									// executed
	private static Integer maxSerial = 0;


	public static Response processQuery(String userinput) throws Exception {
		
		SQLDatabaseSingleton.getInstance().setDatabase("bigdawg_schemas", "src/main/resources/schemas/plain.sql");
		// WE CURRENTLY NEED TO DOCUMENT WHICH TABLES ARE CREATED IN THIS FILE. 
		// NEXT VERSION I'LL REMOVE THIS CONSTRAINT. TODO
		
		// UNROLLING
		logger.debug("User query received. Parsing...");
		Map<String, String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
		
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(crossIslandQuery);

		// pass this to monitor, and pick your favorite
		CrossIslandQueryNode ciqn = ciqp.getRoot();
		int choice = getGetPerformanceAndPickTheBest(ciqn);
		
		
		// currently there should be just one island, therefore one child, root.
		QueryExecutionPlan qep = ciqp.getRoot().getQEP(choice);
		
		
		
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
	public static int getGetPerformanceAndPickTheBest(CrossIslandQueryNode ciqn) throws Exception {
		
		int choice = 0;
		
		// now call the corresponding monitor function to deliver permuted.
		// Today there IS ONLY ONE PLAN
		// TODO generate list of QueryExecutionPlans to send to monitor
		List<QueryExecutionPlan> qeps = ciqn.getAllQEPs();
		Monitor.addBenchmarks(qeps, false);
		QueriesAndPerformanceInformation qnp = Monitor.getBenchmarkPerformance(qeps); // TODO CHANGE THE QEPS SENT TO MONITOR FUNCTION

		// does some magic to pick out the best query, store it to the query plan queue

		long minDuration = Long.MAX_VALUE;
		for (int i = 0; i < qnp.qList.size(); i++){
			long currentDuration = qnp.pInfo.get(i);
			if (currentDuration < minDuration){
				minDuration = currentDuration;
				choice = i;
			}
		}
		
		return choice;
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
//		queryQueue.remove(querySerial);

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
