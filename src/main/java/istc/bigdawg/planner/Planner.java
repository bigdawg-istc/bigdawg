package istc.bigdawg.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import istc.bigdawg.cast.CastOverseer;
import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.catalog.CatalogUtilities;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.CrossIslandCast;
import istc.bigdawg.islands.CrossIslandNonOperatorNode;
import istc.bigdawg.islands.CrossIslandQueryNode;
import istc.bigdawg.islands.CrossIslandQueryPlan;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.islands.IslandAndCastResolver;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.signature.Signature;

public class Planner {

	private static final double SIGNATURE_DISTANCE = .05;

	private static Logger logger = Logger.getLogger(Planner.class);

	
	private static Response processCatalogQuery(String input) throws Exception {
		/*
		 * catalog_command
		 * |- Catalog table name followed, optionally, by column names
		 * |- SQL commands -- SELECT, INSERT, UPDATE, DELETE
		 * Usage: bdcatalog(catalog_command; ...)
		 */
		String trim = input;
		if (input.startsWith("\"bdcatalog(")) {
			trim = input.substring(1, input.length() - 1);
		} else if (input.startsWith("bdcatalog(")) {
			// do nothing
		} else 
			return null;
		
		return Response.status(200).entity(CatalogUtilities.catalogQueryResult(CatalogUtilities.parseCatalogQuery(trim))).build();
	}
	

	
	private static Set<CrossIslandQueryNode> processCrossIslandPlanNodes (
			CrossIslandQueryPlan ciqp, 
			Set<CrossIslandQueryNode> entryNodes,  
			Map<CrossIslandQueryNode, ConnectionInfo> connectionInfoMap, 
			Map<ConnectionInfo, Collection<String>> tempTableInfo, 
			Set<Integer> objectsToDelete) throws Exception {
		
		Set<CrossIslandQueryNode> nextNodes = new HashSet<>();
		for (CrossIslandQueryNode node : entryNodes) {

			// If we arrive at a terminalNode, skip and process later
			if (node == ciqp.getTerminalNode()) {
				continue;
			}

			if (node instanceof CrossIslandCast) {
				// Todo: move this code block to a function
				// we make the assumption that there is no chain CASTing
				// we also assume that the user does not directly cast an object 
				// throw new Exception("Unimplemented feature: CAST");
				
				CrossIslandCast castNode = ((CrossIslandCast) node);

				IntraIslandQuery source = (IntraIslandQuery) castNode.getSourceVertex(ciqp);
				IntraIslandQuery target = (IntraIslandQuery) castNode.getTargetVertex(ciqp);


				// get the target, and pick destination -- default location
				
				
				
				int oid = CastOverseer.cast(castNode, source, target, connectionInfoMap, tempTableInfo);
				
				
				

				// add the temporary objects to be deleted
//				if (!tempTableInfo.containsKey(connectionInfoMap.get(source))) {
//					tempTableInfo.put(connectionInfoMap.get(source), new HashSet<>());
//				}
//				tempTableInfo.get(connectionInfoMap.get(source)).add(source.getName());
//				if (!tempTableInfo.containsKey(targetConnInfo)) {
//					tempTableInfo.put(targetConnInfo, new HashSet<>());
//				}
//				tempTableInfo.get(targetConnInfo).add(remoteName);


				// add catalog entry of the temp table, add to catalog set of destruction
				// unsafe use of ""
				objectsToDelete.add(oid); // find the correct DBID for source

				// add target to the next gen
				nextNodes.add(target);

				// remove source from connectionInfoMap
				connectionInfoMap.remove(source);

			} else if (node instanceof IntraIslandQuery) {

				// Todo: move this block to a separate function
				// business as usual
				IntraIslandQuery ciqn = (IntraIslandQuery) node;

				// TODO make hte best choice of permutation. We postpone this to the next release, 0.2 
				// int choice = getGetPerformanceAndPickTheBest(ciqn, isTrainingMode);
				int choice = 0;

				// currently there should be just one island, therefore one child, root.
				QueryExecutionPlan qep = ((IntraIslandQuery) node).getQEP(choice, false);

				// EXECUTE THE RESULT SUB RESULT
				logger.debug("Executing query cross-island subquery " + node + "...");
				connectionInfoMap.put(node, Executor.executePlan(qep, ciqn.getSignature(), choice).getConnectionInfo());

			} else if (node instanceof CrossIslandNonOperatorNode) {
				connectionInfoMap.put(node, IslandAndCastResolver.runOperatorFreeIslandQuery((CrossIslandNonOperatorNode) node).getConnectionInfo());
			} else {
				throw new BigDawgException("Planner::processQuery has unimplemented Cross Island Plan Node: " + node.getClass().getSimpleName());
			}
			// add the child node to nextGen
			nextNodes.add(node.getTargetVertex(ciqp));
		}
		return nextNodes;
	}
	
	public static Response processQuery(String userinput, boolean isTrainingMode) throws Exception {
		
		String input = userinput.replaceAll("[/\n/]", "").replaceAll("[ /\t/]+", " ");
		
		// UNROLLING
		logger.debug("User query received. Parsing... " + input.replaceAll("[\"']", "*"));
		
		// identify and process Catalog query
		Response r = processCatalogQuery(input);
		if (r != null) return r;

		// Track the temporary objects and table info for later deletion
		Set<Integer> objectsToDelete = new HashSet<>();

		// Create cross island query plan (ciqp)
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(input, objectsToDelete);
		if (ciqp.getTerminalNode() == null) {
			throw new Exception("Ill formed input: " + input + "\n");
		}
		logger.info("CrossIslandQueryPlan: #nodes: " + ciqp.vertexSet().size() +
				"; #edges: " + ciqp.edgeSet().size() + "\n" + ciqp.toString() + "\n\n");

		// Traverse the graph and run the execution plans
		Set<CrossIslandQueryNode> entryNodes = new HashSet<>(ciqp.getEntryNodes());
		Map<CrossIslandQueryNode, ConnectionInfo> connectionInfoMap = new HashMap<>();
		Map<ConnectionInfo, Collection<String>> tempTableInfo = new HashMap<>();
		try {
			while (!entryNodes.isEmpty()) {
				entryNodes = processCrossIslandPlanNodes(ciqp, entryNodes, connectionInfoMap, tempTableInfo, objectsToDelete);
			}
		} catch (Exception e) {
			cleanUpTemporaryTables(objectsToDelete, tempTableInfo);
			throw e;
		} 

		// we assume there is no lone CAST
		// pass this to monitor, and pick your favorite

		// Execute terminal node
		CrossIslandQueryNode cipn = ciqp.getTerminalNode();
		if (cipn instanceof CrossIslandCast) {

			// pick the first engine in the database and migrate everything over
			// save for later
			throw new Exception("Unimplemented feature: CASTing output");

		} 
		
		QueryResult queryResult = null;
		try {
			if (cipn instanceof IntraIslandQuery) {
	
				// business as usual
				IntraIslandQuery ciqn = (IntraIslandQuery) cipn;
	
				// Ask the Monitor for the best QueryExecutionPlan
				int choice = getGetPerformanceAndPickTheBest(ciqn, isTrainingMode);
				QueryExecutionPlan qep = ciqn.getQEP(choice, true);
	
				// Execute the plan
				logger.debug("Executing terminal node...");
				queryResult = Executor.executePlan(qep, ciqn.getSignature(), choice);
	
			} else if (cipn instanceof CrossIslandNonOperatorNode) {
				// EXECUTE THE RESULT
				logger.debug("Executing CrossIslandNonOperatorNode exit node...");
				queryResult = IslandAndCastResolver.runOperatorFreeIslandQuery((CrossIslandNonOperatorNode) cipn);
	
			} else {
				throw new BigDawgException("Planner::processQuery has unimplemented Cross Island Plan Node: " + cipn.getClass().getSimpleName());
			}
		} finally {
			cleanUpTemporaryTables(objectsToDelete, tempTableInfo);
		}
		
		Response response = compileResults(ciqp.getSerial(), queryResult);
		return response ;
	}


	private static void cleanUpTemporaryTables(Set<Integer> catalogSOD, Map<ConnectionInfo, Collection<String>> tempTableMOD) throws Exception{
		
		Log.debug("Garbage collection starts; Next up: catalog entries");
		Long time = System.currentTimeMillis();
		CatalogModifier.deleteMultipleObjects(catalogSOD);
		Log.debug(String.format("Catalog entries cleaned, time passed: %s; Next up: tempTables", System.currentTimeMillis() - time));
		for (ConnectionInfo c : tempTableMOD.keySet()) {
            final Collection<String> tables = tempTableMOD.get(c);
            Log.debug(String.format("removing %s on %s...", tables, c.getDatabase()));
            try {
            	for (String s : tables) {
            		c.getLocalQueryExecutor().dropDataSetIfExists(s);
            	}
//            	Collection<String> cs = c.getCleanupQuery(tables);
//            	for (String cleanupQuery : cs)
//            		c.getLocalQueryExecutor().execute(cleanupQuery);
            } catch (ConnectionInfo.LocalQueryExecutorLookupException e) {
                e.printStackTrace();
            }
		}
		Log.debug(String.format("Temp tables cleaned, time passed: %s; clean up finished", System.currentTimeMillis() - time));
	}

	
	
	/**
	 * CALL MONITOR: Parses the userinput, generate alternative join plans, and
	 * GIVE IT TO MONITOR Note: this generates the result
	 * 
	 * @param
	 */
	public static int getGetPerformanceAndPickTheBest(IntraIslandQuery ciqn, boolean isTrainingMode) throws Exception {
		int choice = 0;
		Signature signature = ciqn.getSignature();

		List<QueryExecutionPlan> qeps = ciqn.getAllQEPs(true);
		Log.debug("Number of qeps: " + qeps.size());

		if (qeps.size() <= 1){
			return choice;
		}
		
		if (isTrainingMode) {
			Log.debug("Running in Training Mode...");
			// now call the corresponding monitor function to deliver permuted.
			Monitor.addBenchmarks(signature, false);
			List<Long> perfInfo = Monitor.getBenchmarkPerformance(signature);
	
			// does some magic to pick out the best query, store it to the query plan queue
			long minDuration = Long.MAX_VALUE;
			for (int i = 0; i < perfInfo.size(); i++){
				long currentDuration = perfInfo.get(i);
				if (currentDuration < minDuration){
					minDuration = currentDuration;
					choice = i;
				}
			}
		} else {
			Log.debug("Running in production mode!!!");
			Signature closest = Monitor.getClosestSignature(signature);
			if (closest != null){
				Log.debug("Closest query found");
				double distance = signature.compare(closest);
				Log.debug("Minimum distance between queries: " + distance);
				if (distance > SIGNATURE_DISTANCE){
					Log.debug("No queries that are similar enough");
					Monitor.addBenchmarks(signature, true);
					return choice;
				}

				List<Long> perfInfo = Monitor.getBenchmarkPerformance(closest);

				// TODO does some magic to match the best query from the closest
				// signature to a query plan for the current query
				// Placeholder: This assumes that the order of
				// permutations for a similar query will be the
				// same as that of the current query
				long minDuration = Long.MAX_VALUE;
				for (int i = 0; i < perfInfo.size(); i++){
					long currentDuration = perfInfo.get(i);
					if (currentDuration < minDuration){
						minDuration = currentDuration;
						choice = i;
					}
				}
			} else {
				Log.debug("No queries that are even slightly similar");
				Monitor.addBenchmarks(signature, true);
			}
		}
		
		return choice;
	};


	/**
	 * FINAL COMPILATION Receive result and send it to user
	 * 
	 * @param querySerial
	 * @param result
	 * @return 0 if no error; otherwise incomplete
	 */
	public static Response compileResults(int querySerial, QueryResult result) throws Exception {
		if (result == null) {
			throw new Exception("Unknown execution error; contact the administrator with query number " + querySerial + "\n");
		}
		logger.debug("[BigDAWG] PLANNER: Query "+querySerial+" is completed. Result:\n"+result.toPrettyString());
		return Response.status(200).entity(result.toPrettyString()).build();
	}

}
