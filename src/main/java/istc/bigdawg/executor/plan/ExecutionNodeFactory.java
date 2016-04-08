package istc.bigdawg.executor.plan;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode.JoinOperand;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.utils.IslandsAndCast.Scope;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jgrapht.Graphs;

public class ExecutionNodeFactory {
	static final Logger log = Logger.getLogger(ExecutionNodeFactory.class.getName());

	private static int maxSerial = 0;

	/**
	 * Produces a String representation of an ExecutionNode
	 * @param node The ExecutionNode we want to make into a String
	 * @return The representation
	 */
	public static String executionNodeToString(ExecutionNode node) {
		if (node.getClass().getName().contains("BinaryJoinExecutionNode")){
			return node.serialize();
		}

		StringBuilder currentRep = new StringBuilder();
		currentRep.append("(");

		Optional<String> queryString = node.getQueryString();
		if (queryString.isPresent()) {
			currentRep.append("QUERY:");
			currentRep.append(queryString.get());
		}

		Optional<String> tableName = node.getTableName();
		if (tableName.isPresent()) {
			currentRep.append("TABLE:");
			currentRep.append(tableName.get());
		}

		currentRep.append("ENGINE:(");
		currentRep.append(ConnectionInfoParser.connectionInfoToString(node.getEngine()));
		currentRep.append(")");

		currentRep.append(String.format("NODETYPE:%s", node.getClass().getName()));
		currentRep.append(")");
		return currentRep.toString();
	}

	/**
	 * Produces an ExecutionNode from the output of executionNodeToString
	 * @param representation an output of executionNodeToString
	 * @return the ExecutionNode
	 */
	public static ExecutionNode stringToExecutionNode(String representation) {
		Pattern queryTable = Pattern.compile("(?<=QUERY:)(?s).*(?=TABLE:)");
		Pattern queryEngine = Pattern.compile("(?<=QUERY:)(?s).*(?=ENGINE:)");
		Pattern table = Pattern.compile("(?<=TABLE:)(?s).*(?=ENGINE:)");
		Pattern engine = Pattern.compile("(?<=ENGINE:\\()[^\\)]*(?=\\))");
		Pattern nodeType = Pattern.compile("(?<=NODETYPE:)[^\\)]*(?=\\))");

		// Get the type of ExecutionNode
		String nodeClass = "LocalQueryExecutionNode";
		Matcher m = nodeType.matcher(representation);
		if (m.find()) {
			nodeClass = m.group();
		}

		if (nodeClass.contains("BinaryJoinExecutionNode")) {
			return BinaryJoinExecutionNode.deserialize(representation);
		}

		// Extract the query
		Optional<String> query = Optional.empty();
		if (representation.contains("QUERY:")) {
			if (representation.contains("TABLE:")) {
				m = queryTable.matcher(representation);
				if (m.find()) {
					query = Optional.of(m.group());
				}
			} else {
				m = queryEngine.matcher(representation);
				if (m.find()) {
					query = Optional.of(m.group());
				}
			}
		}

		// Extract the tableName
		Optional<String> tableName = Optional.empty();
		if (representation.contains("TABLE:")){
			m = table.matcher(representation);
			if (m.find()) {
				tableName = Optional.of(m.group());
			}
		}

		// Extract the ConnectionInfo
		m = engine.matcher(representation);
		String engineInfo = "";
		if (m.find()) {
			engineInfo = m.group();
		}
		ConnectionInfo connectionInfo = ConnectionInfoParser.stringToConnectionInfo(engineInfo);



		ExecutionNode result = null;
		if (nodeClass.contains("LocalQueryExecutionNode")) {
			result = new LocalQueryExecutionNode(query.get(), connectionInfo, tableName.get());
		} else if (nodeClass.contains("TableExecutionNode")) {
			result = new TableExecutionNode(connectionInfo, tableName.get());
		}
		return result;
	}

	@Deprecated
	public static Map<String, Operator> traverseAndPickOutWiths (Operator root) throws Exception {
		Map<String, Operator> result = new HashMap<>();
		maxSerial++;
		result.put("BIGDAWG_MAIN_"+maxSerial, root);//.generateSelectForExecutionTree(queryPlan.getStatement(), null));
		
		List<Operator> treeWalker = root.getChildren();
		while(treeWalker.size() > 0) {
			List<Operator> nextGeneration = new ArrayList<Operator>();
			for(Operator c : treeWalker) {
				
				nextGeneration.addAll(c.getChildren());
				if(c instanceof CommonSQLTableExpressionScan) {
					CommonSQLTableExpressionScan co = ((CommonSQLTableExpressionScan) c);
					String name = co.getTable().getName();
					result.put(name, co);//co.generateSelectForExecutionTree(queryPlan.getStatement(), name));
					nextGeneration.add(co.getSourceStatement());
				}
			}
			treeWalker = nextGeneration;
		}
		return result;
	}

	private static BinaryJoinExecutionNode createJoinNode(String broadcastQuery, ConnectionInfo engine, String joinDestinationTable, Join joinOp) throws Exception {
		Operator left = joinOp.getChildren().get(0);
		Operator right = joinOp.getChildren().get(1);

		// Break apart Join Predicate Objects into usable Strings
		List<String> predicateObjects = joinOp.getJoinPredicateObjectsForBinaryExecutionNode();

		if (predicateObjects.isEmpty()) {
			System.out.printf("Null predicate objects; \n\nleft: %s\n\nright: %s\n\npredicateObjects: %s\n\ntree: %s\n\n\n",
					left.generateSQLString(null), right.generateSQLString(null), predicateObjects, joinOp.generateSQLString(null));
		}

		String comparator = predicateObjects.get(0);
		String leftTable = StringUtils.substringBetween(predicateObjects.get(1), "{", ",");
		String leftAttribute = StringUtils.substringBetween(predicateObjects.get(1), " ", "}");
		String rightTable = StringUtils.substringBetween(predicateObjects.get(2), "{", ",");
		String rightAttribute = StringUtils.substringBetween(predicateObjects.get(2), " ", "}");

		// TODO(jack): verify that this mechanism for coming up with the queries to run on each shuffle node makes sense with the Operator model

		// TODO@Ankush: you mentioned that you want to separate the data depends on bins or betweens, this might help you: [from Operator.java] generateSQLWithWidthBucket(String widthBucketString, String into, Select srcStatement)
		// also, what you're doing here is that you want to only replace the `leftTable' and `rightTable' with the result of the other Shuffle Join Query, and not bother with other things, right?

		String shuffleLeftJoinQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable + "_LEFTRESULTS", true).replace(rightTable, joinDestinationTable + "_RIGHTPARTIAL");
		String shuffleRightJoinQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable + "_RIGHTRESULTS", true).replace(leftTable, joinDestinationTable + "_LEFTPARTIAL");

		JoinOperand leftOp = new JoinOperand(engine, leftTable, leftAttribute, shuffleLeftJoinQuery);
		JoinOperand rightOp = new JoinOperand(engine, rightTable, rightAttribute, shuffleRightJoinQuery);

		log.debug(String.format("Created join node for query %s with left dependency on %s and right dependency on %s\n", broadcastQuery, leftTable, rightTable));

		return new BinaryJoinExecutionNode(broadcastQuery, engine, joinDestinationTable, leftOp, rightOp, comparator);
	}

	private static ExecutionNodeSubgraph buildOperatorSubgraph(Operator op, ConnectionInfo engine, String dest, Map<String, LocalQueryExecutionNode> containerNodes, boolean isSelect) throws Exception {
		StringBuilder sb = new StringBuilder();
		Join joinOp = op.generateSQLStatementForPresentNonJoinSegment(sb, isSelect);
		final String sqlStatementForPresentNonJoinSegment = sb.toString();
		
		// TODO CHANGE NAME OF JOIN'S CHILDREN
		// TODO(ankush): allow for multiple types of engines (not just SQL)

		ExecutionNodeSubgraph result = new ExecutionNodeSubgraph();
		
		LocalQueryExecutionNode lqn = null;
		if (sqlStatementForPresentNonJoinSegment.length() > 0) {
			// this and joinOp == null will not happen at the same time
			lqn = new LocalQueryExecutionNode(sqlStatementForPresentNonJoinSegment, engine, dest);
//			log.debug(String.format("Created new LocalQueryExecutionNode for %s, from sqlStatementForPresentNonJoinSegment\n", sqlStatementForPresentNonJoinSegment));
			result.addVertex(lqn);
			result.exitPoint = lqn;
		}

		if (joinOp != null) {
			String joinDestinationTable = joinOp.getJoinToken();

			String broadcastQuery;
			if (sqlStatementForPresentNonJoinSegment.length() == 0 && isSelect) {
				broadcastQuery = joinOp.generateSQLString(null);
			} else {
				broadcastQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable, true);
			}
			
			// TODO(ankush): re-enable binary join handling
//			BinaryJoinExecutionNode joinNode = ExecutionNodeFactory.createJoinNode(broadcastQuery, engine, joinDestinationTable, joinOp);
			LocalQueryExecutionNode joinNode = new LocalQueryExecutionNode(broadcastQuery, engine, joinDestinationTable);

			result.addVertex(joinNode);
			
			if (sqlStatementForPresentNonJoinSegment.length() == 0) {
				result.exitPoint = joinNode;
			} else {
				result.addEdge(joinNode, result.exitPoint);
			}

			for(Operator child : joinOp.getChildren()) {
				if(child.isPruned()) {
					ExecutionNode containerNode = containerNodes.get(child.getPruneToken());
					result.addVertex(containerNode);
					result.addEdge(containerNode, joinNode);
				} else {
					String token = child.isSubTree() ? child.getSubTreeToken() : null;
					ExecutionNodeSubgraph subgraph = buildOperatorSubgraph(child, engine, token, containerNodes, false);
					Graphs.addGraph(result, subgraph);
					result.addEdge(subgraph.exitPoint, joinNode);
				}
			}
		}

		return result;
	}

	public static void addNodesAndEdgesWithJoinHandling(QueryExecutionPlan qep, Operator remainder, List<String> remainderLoc, Map<String,
			QueryContainerForCommonDatabase> containers, boolean isSelect) throws Exception {
		log.debug(String.format("Creating QEP %s...", qep.getSerializedName()));

		int remainderDBID;
		if (remainderLoc != null) {
			remainderDBID = Integer.parseInt(remainderLoc.get(0));
		} else {
			remainderDBID = Integer.parseInt(containers.values().iterator().next().getDBID());
		}

		String remainderSelectIntoString;
		ConnectionInfo remainderCI;
		if (qep.getIsland().equals(Scope.RELATIONAL)) {
			remainderCI = CatalogViewer.getPSQLConnectionInfo(remainderDBID);
			remainderSelectIntoString = remainder.generateSQLString(null);
		} else if (qep.getIsland().equals(Scope.ARRAY)) {
			remainderCI = CatalogViewer.getSciDBConnectionInfo(remainderDBID);
			remainderSelectIntoString = remainder.generateAFLStoreStringForExecutionTree(null);
		} else {
			throw new Exception("Unsupported island code: " + qep.getIsland().toString());
		}

		Map<String, LocalQueryExecutionNode> containerNodes = new HashMap<>();
		for(Map.Entry<String, QueryContainerForCommonDatabase> entry : containers.entrySet()) {
			String table = entry.getKey();
			QueryContainerForCommonDatabase container = entry.getValue();

			String selectIntoString;
			if (qep.getIsland().equals(Scope.RELATIONAL))
				selectIntoString = container.generateSQLSelectIntoString();
			else if (qep.getIsland().equals(Scope.ARRAY))
				selectIntoString = container.generateAFLStoreString();
			else
				throw new Exception("Unsupported island code: " + qep.getIsland().toString());

			LocalQueryExecutionNode localQueryNode = new LocalQueryExecutionNode(selectIntoString, container.getConnectionInfo(), table);

//			log.debug(String.format("Created LQN %s for container.", table));

			containerNodes.put(table, localQueryNode);
		}
		
		remainder.setSubTree(true);
		String remainderInto = remainder.getSubTreeToken();
		qep.setTerminalTableName(remainderInto);

		if (remainderLoc != null) {
			LocalQueryExecutionNode lqn = new LocalQueryExecutionNode(remainderSelectIntoString, remainderCI, remainderInto);
			qep.addNode(lqn);
			qep.setTerminalTableNode(lqn);
		} else {
			ExecutionNodeSubgraph subgraph = buildOperatorSubgraph(remainder, remainderCI, remainderInto, containerNodes, isSelect);
			Graphs.addGraph(qep, subgraph);
			qep.setTerminalTableNode(subgraph.exitPoint);
		}

		log.debug(String.format("Finished creating QEP %s.", qep.getSerializedName()));
	}

	@Deprecated
	public static void addNodesAndEdgesNaive(QueryExecutionPlan qep, Operator remainder, List<String> remainderLoc, Map<String, 
			QueryContainerForCommonDatabase> container) throws Exception {
		// this should take a new QEP, a local map, the remainder, the container, and something else about the query
		
		HashMap<String, ExecutionNode> dependentNodes = new HashMap<>();
		ArrayList<String> edgesFrom = new ArrayList<>();
		ArrayList<String> edgesTo = new ArrayList<>();
		
		int remainderDBID;
		ConnectionInfo remainderCI;
		if (remainderLoc != null) {
//			System.out.println("remainderLoc not null; result: "+ remainderLoc.get(0));
			remainderDBID = Integer.parseInt(remainderLoc.get(0));
		} else {
//			System.out.println("remainderLoc IS null; result: "+ remainderDBID);
			remainderDBID = Integer.parseInt(container.values().iterator().next().getDBID());
		}
		
		if (qep.getIsland().equals(Scope.RELATIONAL))
			remainderCI = CatalogViewer.getPSQLConnectionInfo(remainderDBID);
		else if (qep.getIsland().equals(Scope.ARRAY))
			remainderCI = CatalogViewer.getSciDBConnectionInfo(remainderDBID);
		else 
			throw new Exception("Unsupported island code: "+qep.getIsland().toString());
		
		
		String remainderInto = qep.getSerializedName();
		String remainderSelectIntoString;

		// if RELATIONAL
		if (qep.getIsland().equals(Scope.RELATIONAL))
			remainderSelectIntoString = remainder.generateSQLSelectIntoStringForExecutionTree(null, false);
		else if (qep.getIsland().equals(Scope.ARRAY))
			remainderSelectIntoString = remainder.generateAFLStoreStringForExecutionTree(null);
		else 
			throw new Exception("Unsupported island code: "+qep.getIsland().toString());
		
		LocalQueryExecutionNode remainderNode = new LocalQueryExecutionNode(remainderSelectIntoString, remainderCI, remainderInto);
		dependentNodes.put(remainderInto, remainderNode);
			
		qep.setTerminalTableName(remainderInto);
		qep.setTerminalTableNode(remainderNode);
		
		// if there remainderLoc is not null, then there is nothing in the container. 
		// Return.
		if (remainderLoc != null) {
			qep.addVertex(remainderNode);
			return;
		}
		
		
		// this function is called the naive version because it just migrates everything to a random DB -- first brought up by iterator
		for (String statementName : container.keySet()) {
			
			String selectIntoString;
			
			// if relational
			if (qep.getIsland().equals(Scope.RELATIONAL))
				selectIntoString = container.get(statementName).generateSQLSelectIntoString();
			else if (qep.getIsland().equals(Scope.ARRAY))
				selectIntoString = container.get(statementName).generateAFLStoreString();
			else 
				throw new Exception("Unsupported island code: "+qep.getIsland().toString());
			
			ConnectionInfo ci = container.get(statementName).getConnectionInfo();
			dependentNodes.put(statementName, new LocalQueryExecutionNode(selectIntoString, ci, statementName));
			
			edgesFrom.add(statementName);
			edgesTo.add(remainderInto);
			
		}
		
		for (String s : dependentNodes.keySet()) {
			qep.addVertex(dependentNodes.get(s));
		}
		for (int i = 0; i < edgesFrom.size() ; i++) {
			qep.addDagEdge(dependentNodes.get(edgesFrom.get(i)), dependentNodes.get(edgesTo.get(i)));
		}
	}
}