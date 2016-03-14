package istc.bigdawg.plan.operators;

import java.util.List;
import java.util.Map;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.packages.SciDBArray;
import istc.bigdawg.plan.AFLQueryPlan;
import istc.bigdawg.plan.SQLQueryPlan;

public class OperatorFactory {

	public static Operator get(String opType, Map<String, String> parameters, List<String> output,  List<String> sortKeys, List<Operator> children, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception {
	
		switch (opType) {
			case "Aggregate":
			case "HashAggregate":
			case "GroupAggregate":
				if(supplement.hasDistinct()) {
					return new Distinct(parameters, output, children.get(0), supplement);
				}
				return new Aggregate(parameters, output, children.get(0), supplement);
			case "CTE Scan":
				return new CommonSQLTableExpressionScan(parameters, output, null, plan, supplement);
			case "Hash Join":
			case "Nested Loop":
			case "Merge Join":
				return new Join(parameters, output, children.get(0), children.get(1), supplement);
			case "Index Scan":
			case "Seq Scan":
				return new SeqScan(parameters, output, null, supplement);
			case "Sort":
				return new Sort(parameters, output, sortKeys, children.get(0), supplement);					
			case "WindowAgg":
				return new WindowAggregate(parameters, output, children.get(0), supplement);
				
			default: // skip it, only designed for 1:1 io like hash and materialize
				System.out.println("---> opType from OperatorFactory: "+opType);
				return children.get(0);
		}
		
	}
	
	public static Operator get(String opType, Map<String, String> parameters, SciDBArray output,  List<String> sortKeys, List<Operator> children, AFLQueryPlan plan) throws Exception {
		
		switch (opType) {
			case "Aggregate":
//			case "HashAggregate":
//			case "GroupAggregate":
//				if(supplement.hasDistinct()) {
//					return new Distinct(parameters, output, children.get(0), supplement);
//				}
				return new Aggregate(parameters, output, children.get(0));
//			case "CTE Scan":
//				return new CommonSQLTableExpressionScan(parameters, output, null, plan, supplement);
			case "Cross Join":
				return new Join(parameters, output, children.get(0), children.get(1));
			case "Seq Scan":
				if (children.isEmpty())
					return new SeqScan(parameters, output, null);
				else 
					return new SeqScan(parameters, output, children.get(0));
			case "Sort":
				return new Sort(parameters, output, sortKeys, children.get(0));					
			case "WindowAgg":
				return new WindowAggregate(parameters, output, children.get(0));
				
			default: // skip it, only designed for 1:1 io like hash and materialize
				System.out.println("Factory default trigger: "+opType);
				return children.get(0);
		}
		
	}
	
}
