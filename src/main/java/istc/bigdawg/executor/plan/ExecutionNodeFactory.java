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
import istc.bigdawg.query.ConnectionInfo;
import net.sf.jsqlparser.statement.select.Select;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.plan.SQLQueryPlan;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Operator;

public class ExecutionNodeFactory {

	public static Map<String, Operator> traverseAndPickOutWiths (Operator root, SQLQueryPlan queryPlan) throws Exception {
		Map<String, Operator> result = new HashMap<>(); 
		result.put("BIGDAWG_MAIN", root);//.generateSelectForExecutionTree(queryPlan.getStatement(), null));
		
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
	
	private static ConnectionInfo getConnectionInfo(int dbid) throws Exception {
		ArrayList<String> infos = CatalogViewer.getConnectionInfo(CatalogInstance.INSTANCE.getCatalog(), dbid);
		return new PostgreSQLConnectionInfo(infos.get(0), infos.get(1), infos.get(2), infos.get(3), infos.get(4));
	}
	
	public static void addNodesAndEdges(QueryExecutionPlan qep, Map<String, ArrayList<String>> dependency_map, Map<String, Operator> withNSelect, Select srcSTMT) throws Exception {

		HashMap<String, ExecutionNode> dependentNodes = new HashMap<>();
		ArrayList<String> edgesFrom = new ArrayList<>();
		ArrayList<String> edgesTo = new ArrayList<>();
		
		for (String statementName : withNSelect.keySet()) {

			// root node
			
			String ref = dependency_map.get(statementName).get(0);
			
			while ((ref.charAt(0) ^ 0x30) > 0x9) { // trying to find the operator a home
				ref = dependency_map.get(ref).get(0);
			}
			
			ConnectionInfo psqlInfo = getConnectionInfo(Integer.parseInt(ref));
			
			Operator o = withNSelect.get(statementName);
			String put = null; if (o instanceof CommonSQLTableExpressionScan) put = statementName;
			String sel = o.generateSelectForExecutionTree(srcSTMT, put);
			
			dependentNodes.put(statementName, new LocalQueryExecutionNode(sel, psqlInfo, statementName));
//			System.out.printf("Adding node: %s; %s\n", statementName, sel);
			// dependences
			for (String s : dependency_map.get(statementName)) {

				String loc = dependency_map.get(s).get(0);
				if ((loc.charAt(0) ^ 0x30) <= 0x9) {
					dependentNodes.put(s, new TableExecutionNode(getConnectionInfo(Integer.parseInt(loc)), s));
					edgesFrom.add(s);
					edgesTo.add(statementName);
					continue;
				}
				
				String i = s;
				while ((i.charAt(0) ^ 0x30) > 0x9) {
					i = dependency_map.get(i).get(0);
				}
				
				psqlInfo = getConnectionInfo(Integer.parseInt(i));
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
	
	// not using it for now
	public static List<Operator> joinGrouper(Join root) throws Exception {
		ArrayList<Operator> result = new ArrayList<>();
		if (root.getChildren() == null)
			throw new Exception("A join without a child: "+root);
		for (Operator o : root.getChildren()) {
			if (o instanceof Join) {
				result.addAll(joinGrouper((Join)o));
			} else {
				result.add(o);
			}
		}
		return result;
	}
}