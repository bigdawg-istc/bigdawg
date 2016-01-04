package istc.bigdawg.plan.operators;

import java.util.List;
import java.util.Map;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.SQLQueryPlan;

public class OperatorFactory {

	public static Operator get(String opType, Map<String, String> parameters, List<String> output,  List<String> sortKeys, List<Operator> children, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception {
	
		switch (opType) {
			case "Aggregate":
			case "HashAggregate":
			case "GroupAggregate":
				if(supplement.hasDistinct()) {
					return new Distinct(parameters, output, children.get(0), plan, supplement);
				}
				return new Aggregate(parameters, output, children.get(0), plan, supplement);
			case "CTE Scan":
				return new CommonSQLTableExpressionScan(parameters, output, null, plan, supplement);
			case "Hash Join":
			case "Nested Loop":
				return new Join(parameters, output, children.get(0), children.get(1), plan, supplement);
			case "Seq Scan":
				return new SeqScan(parameters, output, null, supplement);
			case "Sort":
				return new Sort(parameters, output, sortKeys, children.get(0), plan, supplement);					
			case "WindowAgg":
				return new WindowAggregate(parameters, output, children.get(0), plan, supplement);
				
			default: // skip it, only designed for 1:1 io like hash and materialize
				return children.get(0);
		}
		
	}
	
}
