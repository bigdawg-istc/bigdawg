package teddy.bigdawg.executor.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import teddy.bigdawg.plan.SQLQueryPlan;
import teddy.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import teddy.bigdawg.plan.operators.Join;
import teddy.bigdawg.plan.operators.Operator;

public class ExecutionNodeFactory {

	public static Map<String, String> traverseAndPickOutWiths (Operator root, SQLQueryPlan queryPlan) throws Exception {
		Map<String, String> result = new HashMap<>(); 
		result.put("BIGDAWG_MAIN", root.generateSelectForExecutionTree(queryPlan.getStatement(), null));
		
		List<Operator> treeWalker = root.getChildren();
		while(treeWalker.size() > 0) {
			List<Operator> nextGeneration = new ArrayList<Operator>();
			for(Operator c : treeWalker) {
				
				nextGeneration.addAll(c.getChildren());
				if(c instanceof CommonSQLTableExpressionScan) {
					CommonSQLTableExpressionScan co = ((CommonSQLTableExpressionScan) c);
					String name = co.getTable().getName();
					result.put(name, co.generateSelectForExecutionTree(queryPlan.getStatement(), name));
					nextGeneration.add(co.getSourceStatement());
				}
			}
			treeWalker = nextGeneration;
		}
		return result;
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