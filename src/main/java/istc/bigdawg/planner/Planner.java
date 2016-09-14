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

import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.Executor;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.CrossIslandCastNode;
import istc.bigdawg.islands.CrossIslandNonOperatorNode;
import istc.bigdawg.islands.CrossIslandPlanNode;
import istc.bigdawg.islands.CrossIslandQueryNode;
import istc.bigdawg.islands.CrossIslandQueryPlan;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.TheObjectThatResolvesAllDifferencesAmongTheIslands;
import istc.bigdawg.migration.MigrationParams;
import istc.bigdawg.migration.Migrator;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.signature.Signature;

public class Planner {

	private static final double SIGNATURE_DISTANCE = .05;

	private static Logger logger = Logger.getLogger(Planner.class);

	public static Response processQuery(String userinput, boolean isTrainingMode) throws Exception {
		
		String input = userinput.replaceAll("[/\n/]", "").replaceAll("[ /\t/]+", " ");
		
		// UNROLLING
		logger.debug("User query received. Parsing... " + input.replaceAll("[\"']", "*"));
		
		if (input.startsWith("bdcatalog(")) {
			// process catalog query
			throw new Exception("bdcatalog function not implemented");
		}
		
		
		// adding new entries
		Set<Integer> catalogSOD = new HashSet<>();
		Map<ConnectionInfo, Collection<String>> tempTableMOD = new HashMap<>();
		
		CrossIslandQueryPlan ciqp = new CrossIslandQueryPlan(input, catalogSOD);
		if (ciqp.getTerminalNode() == null) throw new Exception("Ill formed input: "+input+"\n");
		
		System.out.printf("CrossIslandQueryPlan print out: #nodes: %s; #edges: %s\n%s\n\n", ciqp.vertexSet().size(), ciqp.edgeSet().size(), ciqp.toString());
		
		// NEW CODE 2
		// use a walker
		Set<CrossIslandPlanNode> nodeWalker = new HashSet<>(ciqp.getEntryNodes());
		Set<CrossIslandPlanNode> nextGeneration;
		Map<CrossIslandPlanNode, ConnectionInfo> nodeToResult = new HashMap<>();
		
		while (!nodeWalker.isEmpty()) {
			nextGeneration = new HashSet<>();
			for (CrossIslandPlanNode node : nodeWalker) {

				// this is the terminalNode; save for later.
				if (node == ciqp.getTerminalNode()) {
					continue;
				}
				
				if (node instanceof CrossIslandCastNode) {
					
					// we make the assumption that there is no chain CASTing
					// we also assume that the user does not directly cast an object 
//					throw new Exception("Unimplemented feature: CAST");

					CrossIslandQueryNode source = (CrossIslandQueryNode)((CrossIslandCastNode)node).getSourceVertex(ciqp);
					CrossIslandQueryNode target = (CrossIslandQueryNode)((CrossIslandCastNode)node).getTargetVertex(ciqp);

					int sourceLoc = 0;
					int targetLoc = 0;
					
					// get the target, and pick destination -- default location
					if (target.getRemainderLoc() != null) {
						targetLoc = Integer.parseInt(target.getRemainderLoc().get(0));
					} else {
						targetLoc = Integer.parseInt(target.getQueryContainer().entrySet().iterator().next().getValue().getDBID());
					}
					
					// get the source, and get the engine 
					ConnectionInfo ci = null;
					if (targetLoc > 0) ci = CatalogViewer.getConnectionInfo(targetLoc);
					else throw new Exception(String.format("\n\nNegative target loc: %s; requires resolution.\n\n", targetLoc));
					String remoteName = processRemoteName(((CrossIslandCastNode)node).getSourceScope(), ((CrossIslandCastNode)node).getDestinationScope(), node.getName());
					
					logger.debug(String.format("\n\nnodeToResult: %s; source: %s\n\n", nodeToResult, source));
					
					logger.debug(String.format("Interisland Migration from %s at %s (%s) to %s at %s (%s)"
							, source.getName()
							, nodeToResult.get(source).getHost()+":"+nodeToResult.get(source).getPort()
							, nodeToResult.get(source).getClass().getSimpleName()
							, remoteName
							, ci.getHost()+":"+ci.getPort()
							, ci.getClass().getSimpleName()));

					// Create schema before migration 
//					remoteSchemaCreation((CrossIslandCastNode)node, ci);
					logger.debug(String.format("CAST query string: %s", node.getQueryString()));
					
					// migrate
					Migrator.migrate(nodeToResult.get(source), source.getName(), ci, remoteName, new MigrationParams(node.getQueryString()));
					
					
					// add to Table set of destruction
					if (!tempTableMOD.containsKey(ci)) tempTableMOD.put(ci, new HashSet<>());
					if (!tempTableMOD.containsKey(nodeToResult.get(source))) tempTableMOD.put(nodeToResult.get(source), new HashSet<>());
					tempTableMOD.get(nodeToResult.get(source)).add(source.getName());
					tempTableMOD.get(ci).add(remoteName);
					
					
					// add catalog entry of the temp table, add to catalog set of destruction
					// unsafe use of ""
					catalogSOD.add(CatalogModifier.addObject(remoteName, "", sourceLoc, targetLoc)); //TODO find the correct DBID for source
					
					// add target to the next gen
					nextGeneration.add(target);
					
					// remove source from nodeToResult
					nodeToResult.remove(source);
					
				} else if (node instanceof CrossIslandQueryNode) {
					// business as usual
					
					CrossIslandQueryNode ciqn = (CrossIslandQueryNode)node;
					
//					int choice = getGetPerformanceAndPickTheBest(ciqn, isTrainingMode);
					int choice = 0;
					
					// currently there should be just one island, therefore one child, root.
					QueryExecutionPlan qep = ((CrossIslandQueryNode)node).getQEP(choice, false);
					
					
					// EXECUTE THE RESULT SUB RESULT
					logger.debug("Executing query cross-island subquery "+node+"...");
					nodeToResult.put(node, Executor.executePlan(qep, ciqn.getSignature(), choice).getConnectionInfo());
					
				} else if (node instanceof CrossIslandNonOperatorNode) {
					nodeToResult.put(node, TheObjectThatResolvesAllDifferencesAmongTheIslands.runOperatorFreeIslandQuery((CrossIslandNonOperatorNode)node).getConnectionInfo());
				} else {
					throw new BigDawgException("Planner::processQuery has unimplemented Cross Island Plan Node: "+node.getClass().getSimpleName());
				}
				// add the child node to nextGen
				nextGeneration.add(node.getTargetVertex(ciqp));
			}
			nodeWalker = nextGeneration;
		}
		
		
		// we assume there is no lone CAST
		// pass this to monitor, and pick your favorite
		
		
		CrossIslandPlanNode cipn = ciqp.getTerminalNode();
		if (cipn instanceof CrossIslandCastNode) {

			// pick the first engine in the database and migrate everything over
			// save for later
			throw new Exception("Unimplemented feature: CASTing output");
			
		} else if (cipn instanceof CrossIslandQueryNode) {
			// business as usual
			
			CrossIslandQueryNode ciqn = (CrossIslandQueryNode)cipn;
			int choice = getGetPerformanceAndPickTheBest(ciqn, isTrainingMode);
			
			
			// currently there should be just one island, therefore one child, root.
			QueryExecutionPlan qep = ((CrossIslandQueryNode)ciqp.getTerminalNode()).getQEP(choice, true);
			
			
			// EXECUTE THE RESULT
			logger.debug("Executing query execution tree exit node...");
			Response responseHolder = compileResults(ciqp.getSerial(), Executor.executePlan(qep, ciqn.getSignature(), choice));
			
			cleanUpTemporaryTables(catalogSOD, tempTableMOD);
			
			return responseHolder;
			
		} else if (cipn instanceof CrossIslandNonOperatorNode) {
			// EXECUTE THE RESULT
			logger.debug("Executing CrossIslandNonOperatorNode exit node...");
			Response responseHolder = compileResults(ciqp.getSerial(), TheObjectThatResolvesAllDifferencesAmongTheIslands.runOperatorFreeIslandQuery((CrossIslandNonOperatorNode)cipn));
			
			cleanUpTemporaryTables(catalogSOD, tempTableMOD);
			
			return responseHolder;
			
		} else {
			throw new BigDawgException("Planner::processQuery has unimplemented Cross Island Plan Node: "+cipn.getClass().getSimpleName());
		}
		
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
            	Collection<String> cs = c.getCleanupQuery(tables);
            	for (String cleanupQuery : cs)
            		c.getLocalQueryExecutor().execute(cleanupQuery);
            } catch (ConnectionInfo.LocalQueryExecutorLookupException e) {
                e.printStackTrace();
            }
		}
		Log.debug(String.format("Temp tables cleaned, time passed: %s; clean up finished", System.currentTimeMillis() - time));
	}

	private static String processRemoteName(Scope sourceScope, Scope destinationScope, String originalString) {
		
		if (sourceScope.equals(destinationScope)) 
			return originalString;
		if (sourceScope.equals(Scope.ARRAY)) 
			return originalString.replaceAll("___", ".");
		if (destinationScope.equals(Scope.ARRAY))
			return originalString.replaceAll("[.]", "___");
		
		return originalString;
	}
	
	/**
	 * CALL MONITOR: Parses the userinput, generate alternative join plans, and
	 * GIVE IT TO MONITOR Note: this generates the result
	 * 
	 * @param userinput
	 */
	public static int getGetPerformanceAndPickTheBest(CrossIslandQueryNode ciqn, boolean isTrainingMode) throws Exception {
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
	public static Response compileResults(int querySerial, QueryResult result) {
		logger.debug("[BigDAWG] PLANNER: Query "+querySerial+" is completed. Result:\n"+result.toPrettyString());
		return Response.status(200).entity(result.toPrettyString()).build();
	}

}
