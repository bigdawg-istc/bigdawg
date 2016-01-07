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
import istc.bigdawg.packages.QueriesAndPerformanceInformation;
import istc.bigdawg.parsers.UserQueryParser;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.extract.SQLPlanParser;
import istc.bigdawg.plan.operators.Operator;
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
		
		PostgreSQLHandler psqlh = new PostgreSQLHandler(0, 3);
		SQLDatabaseSingleton.getInstance().setDatabase("bigdawg_schemas", "src/main/resources/plain.sql");
		// WE CURRENTLY NEED TO DOCUMENT WHICH TABLES ARE CREATED IN THIS FILE. 
		// NEXT VERSION I'LL REMOVE THIS CONSTRAINT. TODO
		
		
		// UNROLLING
		logger.debug("User query received. Parsing...");
		ArrayList<String> crossIslandQuery = UserQueryParser.getUnwrappedQueriesByIslands(userinput, "BIGDAWGTAG_");
		
		
		// GET SIGNATURE AND CASTS
		logger.debug("Generating signatures and casts...");
		Map<String, Object> sigAndCasts = UserQueryParser.getSignaturesAndCasts(CatalogInstance.INSTANCE.getCatalog(), crossIslandQuery);
		
		
		// POPULATE queryQueue WITH OPTIMAL PLANS
		getGetPerformanceAndPickTheBest(sigAndCasts);

		// now the serial number of query is added;
		int querySerial = maxSerial; // THIS IS A STRONG ASSUMPTION IN MULTI-THREAD. NOT A PROBLEM AT THE MOMENT TODO

		
		// generating query tree 
		logger.debug("Generating query execution tree...");
		QueryExecutionPlan qep = new QueryExecutionPlan();
		populateQueryExecutionPlan(qep, psqlh, sigAndCasts);
		
//		System.out.println("QueryExecutionPlan:: ");
//		for (ExecutionNode v : qep.vertexSet()) {
//			System.out.print(v.getTableName()+"\t\t----- "+ v.getQueryString()+"\n");
//		};
//		
		
		// EXECUTE TEH RESULT
		logger.debug("Executing query execution tree...");
		return compileResults(querySerial, Executor.executePlan(qep));
	}

	/**
	 * Populate the query execution plan
	 * @param qep
	 * @param psqlh
	 * @param sigAndCasts
	 * @throws Exception
	 */
	public static void populateQueryExecutionPlan(QueryExecutionPlan qep, PostgreSQLHandler psqlh, Map<String, Object> sigAndCasts) throws Exception {
		try {
		SQLQueryPlan queryPlan = SQLPlanParser.extractDirect(psqlh, ((Signature)sigAndCasts.get("OUTPUT")).getQuery());
		Operator root = queryPlan.getRootNode();
		
		
		
		ArrayList<String> objs = new ArrayList<>(Arrays.asList(((Signature) sigAndCasts.get("OUTPUT")).getSig2().split("\t")));
		Map<String, ArrayList<String>> map = CatalogViewer.getDBMappingByObj(CatalogInstance.INSTANCE.getCatalog(), objs);
		
		
		
		ArrayList<String> root_dep = new ArrayList<String>();
		root_dep.addAll(root.getTableLocations(map).keySet());
		map.put("BIGDAWG_MAIN", root_dep);
		root.generatePlaintext(queryPlan.getStatement()); // the production of AST should be root
		
		
		Map<String, Operator> out =  ExecutionNodeFactory.traverseAndPickOutWiths(root, queryPlan);
		
		
		ExecutionNodeFactory.addNodesAndEdges(qep, map, out, queryPlan.getStatement());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	};
	
	
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
		
		logger.debug("Performance information received; serial number: "+maxSerial);
	};

	/**
	 * DEPRECATED
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

}
