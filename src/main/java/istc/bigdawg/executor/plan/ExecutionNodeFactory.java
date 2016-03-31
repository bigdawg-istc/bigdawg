package istc.bigdawg.executor.plan;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode.JoinOperand;
import istc.bigdawg.executor.plan.BinaryJoinExecutionNode;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.utils.IslandsAndCast.Scope;
import net.sf.jsqlparser.statement.select.Select;

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
			return BinaryJoinExecutionNode.stringTo(representation);
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

	private static OperatorTree buildOperatorSubgraph(Operator op, ConnectionInfo engine, String dest) throws Exception {
		StringBuilder sb = new StringBuilder();
		Join joinOp = op.generateSQLStatementForPresentNonJoinSegment(sb);
		
		// TODO CHANGE NAME OF JOIN'S CHILDREN 
		
		OperatorTree result = new OperatorTree();
		
		//--- TODO WHAT IF op IS A JOIN? SOLUTION PART 1
		LocalQueryExecutionNode lqn = null;

		if (sb.toString().length() > 0) {
			// this and joinOp == null will not happen at the same time
			lqn = new LocalQueryExecutionNode(sb.toString(), engine, dest);
			log.debug(String.format("Created new LocalQueryExecutionNode for %s\n", sb.toString()));
			result.addVertex(lqn);
			result.exitPoint = lqn;
		}
		//--- END OF PART 1/2 OF SOLUTION
		

		if (joinOp != null) {
			// Get left and right child operators of joinOp
			Operator left = joinOp.getChildren().get(0);
			Operator right = joinOp.getChildren().get(1);

			// Break apart Join Predicate Objects into usable Strings
			List<String> predicateObjects = joinOp.getJoinPredicateObjectsForBinaryExecutionNode();
			
			if (predicateObjects.isEmpty()) System.out.printf("Null predicate objects; \n\nleft: %s\n\nright: %s\n\npredicateObjects: %s\n\ntree: %s\n\n\n",
					left.generateSQLString(null), right.generateSQLString(null), predicateObjects, joinOp.generateSQLString(null));
			
			
			String comparator = predicateObjects.get(0);
			String leftTable = StringUtils.substringBetween(predicateObjects.get(1), "{", ",");
			String leftAttribute = StringUtils.substringBetween(predicateObjects.get(1), " ", "}");

			String rightTable = StringUtils.substringBetween(predicateObjects.get(2), "{", ",");
			String rightAttribute = StringUtils.substringBetween(predicateObjects.get(2), " ", "}");

			// TODO(ankush): allow for multiple types of engines (not just SQL)

			// (jack): verify this is the correct destination table desired for the JOIN upon completion --- CHECK
			String joinDestinationTable = joinOp.getJoinToken();

			// (jack): verify this is the correct mechanism for generating a query for --- CHECK 
			// computing this join as a regular broadcast join rather than a shuffle join
			String broadcastQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable, null);

			// TODO(jack): verify that this mechanism for coming up with the queries to 
			// run on each shuffle node makes sense with the Operator model
			
			// TODO@Ankush: you mentioned that you want to separate the data depends on bins or betweens, this
			// might help you: [from Operator.java] generateSQLWithWidthBucket(String widthBucketString, String into, Select srcStatement)
			// also, what you're doing here is that you want to only replace the `leftTable' and `rightTable' with the result of the 
			// other Shuffle Join Query, and not bother with other things, right? 
			
			
			String shuffleLeftJoinQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable + "_LEFTRESULTS", true).replace(rightTable, joinDestinationTable + "_RIGHTPARTIAL");
			String shuffleRightJoinQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable + "_RIGHTRESULTS", true).replace(leftTable, joinDestinationTable + "_LEFTPARTIAL");

			JoinOperand leftOp = new JoinOperand(engine, leftTable, leftAttribute, shuffleLeftJoinQuery);
			JoinOperand rightOp = new JoinOperand(engine, rightTable, rightAttribute, shuffleRightJoinQuery);

			log.debug(String.format("Created join node for query %s with left dependency on %s and right dependency on %s\n", broadcastQuery, leftTable, rightTable));
			BinaryJoinExecutionNode joinNode = new BinaryJoinExecutionNode(broadcastQuery, engine, joinDestinationTable, leftOp, rightOp, comparator);
			result.addVertex(joinNode);
			
			//--- TODO WHAT IF op IS A JOIN? SOLUTION PART 2/2
			if (sb.toString().length() == 0) {
				result.exitPoint = joinNode;
			}
			//--- END OF PART 2/2 OF SOLUTION
			

			OperatorTree leftSubtree = buildOperatorSubgraph(left, engine, leftTable);
			Graphs.addGraph(result, leftSubtree);
			result.addEdge(leftSubtree.exitPoint, joinNode);

			OperatorTree rightSubtree = buildOperatorSubgraph(right, engine, rightTable);
			Graphs.addGraph(result, rightSubtree);
			result.addEdge(rightSubtree.exitPoint, joinNode);

			result.entryPoints = Sets.union(leftSubtree.entryPoints, rightSubtree.entryPoints);
		} else {
			result.entryPoints = Collections.singleton(lqn);
		}

		return result;
	}

	public static void addNodesAndEdgesWithJoinHandling(QueryExecutionPlan qep, Operator remainder, List<String> remainderLoc, Map<String,
			QueryContainerForCommonDatabase> containers) throws Exception {

		int remainderDBID;
		if (remainderLoc != null) {
			remainderDBID = Integer.parseInt(remainderLoc.get(0));
		} else {
			remainderDBID = Integer.parseInt(containers.values().iterator().next().getDBID());
		}

		ConnectionInfo remainderCI;
		if (qep.getIsland().equals(Scope.RELATIONAL))
			remainderCI = CatalogViewer.getPSQLConnectionInfo(remainderDBID);
		else if (qep.getIsland().equals(Scope.ARRAY))
			remainderCI = CatalogViewer.getSciDBConnectionInfo(remainderDBID);
		else
			throw new Exception("Unsupported island code: " + qep.getIsland().toString());

		String remainderInto = qep.getSerializedName();

		OperatorTree operatorTree = buildOperatorSubgraph(remainder, remainderCI, remainderInto);

		Graphs.addGraph(qep, operatorTree);

		qep.setTerminalTableName(remainderInto);
		qep.setTerminalTableNode(operatorTree.exitPoint);

		// if remainderLoc is not null, then there are no containers
		if (remainderLoc != null) {
			return;
		}

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

			qep.addVertex(localQueryNode);
			operatorTree.entryPoints.forEach((v) -> qep.addEdge(localQueryNode, v));
		}
	}
	
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