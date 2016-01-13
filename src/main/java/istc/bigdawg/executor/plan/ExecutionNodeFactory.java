package istc.bigdawg.executor.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;


public class ExecutionNodeFactory {

	private static int maxSerial = 0;
	
	/**
	 * NO LONGER USED
	 * TODO CHECK REMOVABILITY
	 * @author Jack
	 *
	 */
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
	
	/**
	 * NO LONGER USED
	 * TODO DETERMINE UTILITY AND REMOVE
	 * @param qep
	 * @param map
	 * @param withNSelect
	 * @param srcSTMT
	 * @throws Exception
	 */
	private static void addNodesAndEdgesOLD(QueryExecutionPlan qep, Map<String, ArrayList<String>> map, Map<String, Operator> withNSelect, Select srcSTMT) throws Exception {

		HashMap<String, ExecutionNode> dependentNodes = new HashMap<>();
		ArrayList<String> edgesFrom = new ArrayList<>();
		ArrayList<String> edgesTo = new ArrayList<>();
		
		for (String statementName : withNSelect.keySet()) {

			// root node
			
			String ref = map.get(statementName).get(0);
			
			while ((ref.charAt(0) ^ 0x30) > 0x9) { // trying to find the operator a home
				ref = map.get(ref).get(0);
			}
			
			ConnectionInfo psqlInfo = PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(ref));
			
			Operator o = withNSelect.get(statementName);
			String put = null; if (o instanceof CommonSQLTableExpressionScan) put = statementName;
			String sel = o.generateSelectForExecutionTree(srcSTMT, put);
			
			dependentNodes.put(statementName, new LocalQueryExecutionNode(sel, psqlInfo, statementName));
//			System.out.printf("Adding node: %s; %s\n", statementName, sel);
			// dependences
			for (String s : map.get(statementName)) {

				String loc = map.get(s).get(0);
				if ((loc.charAt(0) ^ 0x30) <= 0x9) {
					dependentNodes.put(s, new TableExecutionNode(PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(loc)), s));
					edgesFrom.add(s);
					edgesTo.add(statementName);
					continue;
				}
				
				String i = s;
				while ((i.charAt(0) ^ 0x30) > 0x9) {
					i = map.get(i).get(0);
				}
				
				psqlInfo = PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(i));
				o 	= withNSelect.get(s);
				put = null; if (o instanceof CommonSQLTableExpressionScan) put = s;
				sel = o.generateSelectForExecutionTree(srcSTMT, put);
				
				dependentNodes.put(s, new LocalQueryExecutionNode(sel, psqlInfo, s));
				edgesFrom.add(s);
				edgesTo.add(statementName);
			}
			
		}
		for (String s : dependentNodes.keySet()) {
			qep.addVertex(dependentNodes.get(s));
		}
		for (int i = 0; i < edgesFrom.size() ; i ++) {
			qep.addDagEdge(dependentNodes.get(edgesFrom.get(i)), dependentNodes.get(edgesTo.get(i)));
		}
	}
	
	public static void addNodesAndEdgesNaive(QueryExecutionPlan qep, Operator remainder, List<String> remainderLoc, Map<String, QueryContainerForCommonDatabase> container, Select srcStmt) throws Exception {
		//TODO this should take a new QEP, a local map, the remainder, the container, and something else about the query
		
		HashMap<String, ExecutionNode> dependentNodes = new HashMap<>();
		ArrayList<String> edgesFrom = new ArrayList<>();
		ArrayList<String> edgesTo = new ArrayList<>();
		
		String remainderDBID;
		ConnectionInfo remainderCI;
		if (remainderLoc != null) {
			remainderCI = PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(remainderLoc.get(0)));	
		} else {
			remainderDBID = container.values().iterator().next().getConnectionInfos().keySet().iterator().next();
			remainderCI = PostgreSQLHandler.generateConnectionInfo(Integer.parseInt(remainderDBID));
		}
		
		String remainderInto = qep.getSerializedName();
		String remainderSelectIntoString = remainder.generateSelectForExecutionTree(srcStmt, null);
		LocalQueryExecutionNode remainderNode = new LocalQueryExecutionNode(remainderSelectIntoString, remainderCI, remainderInto);
		dependentNodes.put(remainderInto, remainderNode);
			
		qep.setTerminalTableName(remainderInto);
		qep.setTerminalTableNode(remainderNode);
		
		// if there remainderLoc is not null, then there is nothing in the container. 
		// Return.
		if (remainderLoc != null) return;
		
		
		// this function is called the naive version because it just migrates everything to a random DB -- first brought up by iterator
		for (String statementName : container.keySet()) {
			
			String selectIntoString = container.get(statementName).generateSelectIntoString();
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