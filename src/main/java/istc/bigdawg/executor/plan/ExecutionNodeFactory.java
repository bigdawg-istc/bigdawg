package istc.bigdawg.executor.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.ConnectionInfoParser;
import istc.bigdawg.utils.IslandsAndCast.Scope;


public class ExecutionNodeFactory {

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

		// Extract the query
		Optional<String> query = Optional.empty();
		if (representation.contains("QUERY:")) {
			if (representation.contains("TABLE:")) {
				Matcher m = queryTable.matcher(representation);
				if (m.find()) {
					query = Optional.of(m.group());
				}
			} else {
				Matcher m = queryEngine.matcher(representation);
				if (m.find()) {
					query = Optional.of(m.group());
				}
			}
		}

		// Extract the tableName
		Optional<String> tableName = Optional.empty();
		if (representation.contains("TABLE:")){
			Matcher m = table.matcher(representation);
			if (m.find()) {
				tableName = Optional.of(m.group());
			}
		}

		// Extract the ConnectionInfo
		Matcher m = engine.matcher(representation);
		String engineInfo = "";
		if (m.find()) {
			engineInfo = m.group();
		}
		ConnectionInfo connectionInfo = ConnectionInfoParser.stringToConnectionInfo(engineInfo);

		// Get the type of ExecutionNode
		String nodeClass = "LocalQueryExecutionNode";
		m = nodeType.matcher(representation);
		if (m.find()) {
			nodeClass = m.group();
		}

		ExecutionNode result = null;
		if (nodeClass.contains("LocalQueryExecutionNode")) {
			result = new LocalQueryExecutionNode(query.get(), connectionInfo, tableName.get());
		} else if (nodeClass.contains("TableExecutionNode")) {
			result = new TableExecutionNode(connectionInfo, tableName.get());
		} else if (nodeClass.contains("BinaryJoinExecutionNode")) {
			// TODO: look into this
//			result = new BinaryJoinExecutionNode();
			result = null;
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
	
	
	
	public static void addNodesAndEdgesNaive(QueryExecutionPlan qep, Operator remainder, List<String> remainderLoc, Map<String, 
			QueryContainerForCommonDatabase> container) throws Exception {
		// this should take a new QEP, a local map, the remainder, the container, and something else about the query
		
		HashMap<String, ExecutionNode> dependentNodes = new HashMap<>();
		ArrayList<String> edgesFrom = new ArrayList<>();
		ArrayList<String> edgesTo = new ArrayList<>();
		
		String remainderDBID;
		ConnectionInfo remainderCI;
		if (remainderLoc != null) {
//			System.out.println("remainderLoc not null; result: "+ remainderLoc.get(0));
			remainderCI = PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(remainderLoc.get(0)));	
		} else {
			remainderDBID = container.values().iterator().next().getConnectionInfos().keySet().iterator().next();
//			System.out.println("remainderLoc IS null; result: "+ remainderDBID);
			remainderCI = PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(remainderDBID));
		}
		
		String remainderInto = qep.getSerializedName();
		String remainderSelectIntoString;

		// if RELATIONAL
		if (qep.getIsland().equals(Scope.RELATIONAL))
			remainderSelectIntoString = remainder.generateSQLSelectIntoStringForExecutionTree(null);
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
			
			ConnectionInfo ci = container.get(statementName).getConnectionInfos().values().iterator().next();
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