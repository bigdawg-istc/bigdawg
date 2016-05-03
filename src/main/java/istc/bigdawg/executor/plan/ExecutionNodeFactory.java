package istc.bigdawg.executor.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jgrapht.Graphs;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.plan.generators.AFLQueryGenerator;
import istc.bigdawg.plan.generators.OperatorVisitor;
import istc.bigdawg.plan.generators.SQLQueryGenerator;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.utils.IslandsAndCast.Scope;

public class ExecutionNodeFactory {
	static final Logger log = Logger.getLogger(ExecutionNodeFactory.class.getName());

	private static int maxSerial = 0;

	/**
	 * Produces a String representation of an ExecutionNode
	 *
	 * @param node The ExecutionNode we want to make into a String
	 * @return The representation
	 */
	public static String executionNodeToString(ExecutionNode node) {
		if (node.getClass().getName().contains("BinaryJoinExecutionNode")) {
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
	 *
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
		if (representation.contains("TABLE:")) {
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
	public static Map<String, Operator> traverseAndPickOutWiths(Operator root) throws Exception {
		Map<String, Operator> result = new HashMap<>();
		maxSerial++;
		result.put("BIGDAWG_MAIN_" + maxSerial, root);//.generateSelectForExecutionTree(queryPlan.getStatement(), null));

		List<Operator> treeWalker = root.getChildren();
		while (treeWalker.size() > 0) {
			List<Operator> nextGeneration = new ArrayList<Operator>();
			for (Operator c : treeWalker) {

				nextGeneration.addAll(c.getChildren());
				if (c instanceof CommonSQLTableExpressionScan) {
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

	private static BinaryJoinExecutionNode createJoinNode(String broadcastQuery, ConnectionInfo engine, String joinDestinationTable, Join joinOp, Scope island) throws Exception {
		Operator left = joinOp.getChildren().get(0);
		Operator right = joinOp.getChildren().get(1);

		OperatorVisitor gen = null;
		if (island.equals(Scope.RELATIONAL)) gen = new SQLQueryGenerator();
		else if (island.equals(Scope.ARRAY)) gen = new AFLQueryGenerator();
		else throw new Exception("Unsupported Island from buildOperatorSubgraph: " + island.toString());

		// Break apart Join Predicate Objects into usable Strings
		List<String> predicateObjects = gen.getJoinPredicateObjectsForBinaryExecutionNode(joinOp);

		if (predicateObjects.isEmpty()) {
			throw new RuntimeException("No predicates for join!");
		}

		String comparator = predicateObjects.get(0);
		String leftTable = StringUtils.substringBetween(predicateObjects.get(1), "{", ",");
		String leftAttribute = StringUtils.substringBetween(predicateObjects.get(1), " ", "}");
		String rightTable = StringUtils.substringBetween(predicateObjects.get(2), "{", ",");
		String rightAttribute = StringUtils.substringBetween(predicateObjects.get(2), " ", "}");

		joinOp.accept(gen);
		String shuffleLeftJoinQuery = gen.generateSelectIntoStatementForExecutionTree(joinDestinationTable + "_LEFTRESULTS")
				.replace(rightTable, joinDestinationTable + "_RIGHTPARTIAL");
		String shuffleRightJoinQuery = gen.generateSelectIntoStatementForExecutionTree(joinDestinationTable + "_RIGHTRESULTS")
				.replace(leftTable, joinDestinationTable + "_LEFTPARTIAL");

		BinaryJoinExecutionNode.JoinOperand leftOp = new BinaryJoinExecutionNode.JoinOperand(engine, leftTable, leftAttribute, shuffleLeftJoinQuery);
		BinaryJoinExecutionNode.JoinOperand rightOp = new BinaryJoinExecutionNode.JoinOperand(engine, rightTable, rightAttribute, shuffleRightJoinQuery);

		return new BinaryJoinExecutionNode(broadcastQuery, engine, joinDestinationTable, leftOp, rightOp, comparator);
	}

	private static ExecutionNodeSubgraph buildOperatorSubgraph(Operator op, ConnectionInfo engine, String dest, Map<String, LocalQueryExecutionNode> containerNodes, boolean isSelect, Scope island) throws Exception {
		StringBuilder sb = new StringBuilder();

		OperatorVisitor gen = null;
		if (island.equals(Scope.RELATIONAL)) gen = new SQLQueryGenerator();
		else if (island.equals(Scope.ARRAY)) gen = new AFLQueryGenerator();
		else throw new Exception("Unsupported Island from buildOperatorSubgraph: " + island.toString());

		Join joinOp = gen.generateStatementForPresentNonJoinSegment(op, sb, isSelect);
		final String sqlStatementForPresentNonJoinSegment = sb.toString();

		// TODO CHANGE NAME OF JOIN'S CHILDREN

		ExecutionNodeSubgraph result = new ExecutionNodeSubgraph();

		LocalQueryExecutionNode lqn = null;
		if (sqlStatementForPresentNonJoinSegment.length() > 0) {
			// this and joinOp == null will not happen at the same time
			lqn = new LocalQueryExecutionNode(sqlStatementForPresentNonJoinSegment, engine, dest);
			result.addVertex(lqn);
			result.exitPoint = lqn;
		}

		if (joinOp != null) {
			String joinDestinationTable = joinOp.getJoinToken();

			String broadcastQuery;
			if (sqlStatementForPresentNonJoinSegment.length() == 0 && isSelect) {
//				broadcastQuery = joinOp.generateSQLString(null);
					gen.configure(true, false);
					joinOp.accept(gen);
				broadcastQuery = gen.generateStatementString();
			} else {
//				broadcastQuery = joinOp.generateSQLSelectIntoStringForExecutionTree(joinDestinationTable, true);
				gen.configure(true, true);
				joinOp.accept(gen);
				broadcastQuery = gen.generateSelectIntoStatementForExecutionTree(joinDestinationTable);
			}

			BinaryJoinExecutionNode joinNode = ExecutionNodeFactory.createJoinNode(broadcastQuery, engine, joinDestinationTable, joinOp, island);
//			LocalQueryExecutionNode joinNode = new LocalQueryExecutionNode(broadcastQuery, engine, joinDestinationTable);

			result.addVertex(joinNode);

			if (sqlStatementForPresentNonJoinSegment.length() == 0) {
				result.exitPoint = joinNode;
			} else {
				result.addEdge(joinNode, result.exitPoint);
			}

			for (Operator child : joinOp.getChildren()) {
				if (child.isPruned()) {
					ExecutionNode containerNode = containerNodes.get(child.getPruneToken());
					result.addVertex(containerNode);
					result.addEdge(containerNode, joinNode);
				} else {
					String token = child.isSubTree() ? child.getSubTreeToken() : null;
					ExecutionNodeSubgraph subgraph = buildOperatorSubgraph(child, engine, token, containerNodes, false, island);
					Graphs.addGraph(result, subgraph);
					result.addEdge(subgraph.exitPoint, joinNode);
				}
			}
		}

		return result;
	}

	public static void addNodesAndEdges(QueryExecutionPlan qep, Operator remainder, List<String> remainderLoc, Map<String,
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
		OperatorVisitor gen = null;
		if (qep.getIsland().equals(Scope.RELATIONAL)) {
			remainderCI = CatalogViewer.getPSQLConnectionInfo(remainderDBID);
			gen = new SQLQueryGenerator();
		} else if (qep.getIsland().equals(Scope.ARRAY)) {
			remainderCI = CatalogViewer.getSciDBConnectionInfo(remainderDBID);
			gen = new AFLQueryGenerator();
		} else {
			throw new Exception("Unsupported island code: " + qep.getIsland().toString());
		}
		remainder.accept(gen);
		remainderSelectIntoString = gen.generateStatementString();

		Map<String, LocalQueryExecutionNode> containerNodes = new HashMap<>();
		for (Map.Entry<String, QueryContainerForCommonDatabase> entry : containers.entrySet()) {
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
			ExecutionNodeSubgraph subgraph = buildOperatorSubgraph(remainder, remainderCI, remainderInto, containerNodes, isSelect, qep.getIsland());
			Graphs.addGraph(qep, subgraph);
			qep.setTerminalTableNode(subgraph.exitPoint);
		}

		log.debug(String.format("Finished creating QEP %s.", qep.getSerializedName()));
	}
}