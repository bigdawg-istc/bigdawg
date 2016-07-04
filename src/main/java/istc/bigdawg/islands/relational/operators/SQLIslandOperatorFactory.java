package istc.bigdawg.islands.relational.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.SQLQueryPlan;
import istc.bigdawg.islands.relational.SQLTableExpression;

public class SQLIslandOperatorFactory {

	public static Operator get(String opType, Map<String, String> parameters, List<String> output,  List<String> sortKeys, List<Operator> children, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception {
	
		switch (opType) {
			case "Unique":
			case "Aggregate":
				if (children.get(0) instanceof SQLIslandMerge) {
					((SQLIslandMerge)children.get(0)).setUnionAll(false);
					return children.get(0);
				}
			case "HashAggregate":
			case "GroupAggregate":
				if(supplement != null && supplement.hasDistinct()) {
					return new SQLIslandDistinct(parameters, output, (SQLIslandOperator) children.get(0), supplement);
				}
				return new SQLIslandAggregate(parameters, output, (SQLIslandOperator)children.get(0), supplement);
			case "CTE Scan":
				return new SQLIslandCommonTableExpressionScan(parameters, output, null, plan, supplement);
			case "Hash Join":
			case "Nested Loop":
			case "Merge Join":
				return new SQLIslandJoin(parameters, output, (SQLIslandOperator) children.get(0), (SQLIslandOperator) children.get(1), supplement);
			case "Index Scan":
			case "Index Only Scan":
			case "Subquery Scan":
			case "Seq Scan":
				return new SQLIslandSeqScan(parameters, output, null, supplement);
			case "Sort":
				return new SQLIslandSort(parameters, output, sortKeys, (SQLIslandOperator) children.get(0), supplement);					
			case "WindowAgg":
				return new SQLIslandWindowAggregate(parameters, output, (SQLIslandOperator)children.get(0), supplement);
			case "Limit":
				return new SQLIslandLimit(parameters, output, (SQLIslandOperator) children.get(0), supplement);
			case "Append":
				List<SQLIslandOperator> childs = new ArrayList<>();
				childs.addAll(children.stream().map(c -> (SQLIslandOperator) c).collect(Collectors.toSet()));
				return new SQLIslandMerge(parameters, output, childs, supplement);
			default: // skip it, only designed for 1:1 io like hash and materialize
//				System.out.println("---> opType from OperatorFactory: "+opType);
				return children.get(0);
		}
		
	}
	
//	public static Operator get(String opType, Map<String, String> parameters, SciDBArray output,  List<String> sortKeys, List<Operator> children, AFLQueryPlan plan) throws Exception {
//		
//		switch (opType) {
//			case "Aggregate":
////			case "HashAggregate":
////			case "GroupAggregate":
////				if(supplement.hasDistinct()) {
////					return new Distinct(parameters, output, children.get(0), supplement);
////				}
//				return new Aggregate(parameters, output, children.get(0));
////			case "CTE Scan":
////				return new CommonSQLTableExpressionScan(parameters, output, null, plan, supplement);
//			case "Cross Join":
//				return new Join(parameters, output, children.get(0), children.get(1));
//			case "Seq Scan":
//				if (children.isEmpty())
//					return new SeqScan(parameters, output, null);
//				else 
//					return new SeqScan(parameters, output, children.get(0));
//			case "Sort":
//				return new Sort(parameters, output, sortKeys, children.get(0));					
//			case "WindowAgg":
//				return new WindowAggregate(parameters, output, children.get(0));
//				
//			default: // skip it, only designed for 1:1 io like hash and materialize
//				System.out.println("Factory default trigger: "+opType);
//				return children.get(0);
//		}
//		
//	}
//	
}
