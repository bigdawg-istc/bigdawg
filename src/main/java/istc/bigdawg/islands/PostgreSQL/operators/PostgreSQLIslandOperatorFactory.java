package istc.bigdawg.islands.PostgreSQL.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import istc.bigdawg.islands.PostgreSQL.SQLQueryPlan;
import istc.bigdawg.islands.PostgreSQL.SQLTableExpression;
import istc.bigdawg.islands.operators.Operator;

public class PostgreSQLIslandOperatorFactory {

	public static Operator get(String opType, Map<String, String> parameters, List<String> output,  List<String> sortKeys, List<Operator> children, SQLQueryPlan plan, SQLTableExpression supplement) throws Exception {
	
		switch (opType) {
			case "Unique":
			case "Aggregate":
				if (children.get(0) instanceof PostgreSQLIslandMerge) {
					((PostgreSQLIslandMerge)children.get(0)).setUnionAll(false);
					return children.get(0);
				}
			case "HashAggregate":
			case "GroupAggregate":
				if(supplement != null && supplement.hasDistinct()) {
					return new PostgreSQLIslandDistinct(parameters, output, (PostgreSQLIslandOperator) children.get(0), supplement);
				}
				return new PostgreSQLIslandAggregate(parameters, output, (PostgreSQLIslandOperator)children.get(0), supplement);
			case "CTE Scan":
				return new PostgreSQLIslandCommonTableExpressionScan(parameters, output, null, plan, supplement);
			case "Hash Join":
			case "Nested Loop":
			case "Merge Join":
				return new PostgreSQLIslandJoin(parameters, output, (PostgreSQLIslandOperator) children.get(0), (PostgreSQLIslandOperator) children.get(1), supplement);
			case "Index Scan":
			case "Index Only Scan":
			case "Subquery Scan":
			case "Seq Scan":
				return new PostgreSQLIslandSeqScan(parameters, output, null, supplement);
			case "Sort":
				return new PostgreSQLIslandSort(parameters, output, sortKeys, (PostgreSQLIslandOperator) children.get(0), supplement);					
			case "WindowAgg":
				return new PostgreSQLIslandWindowAggregate(parameters, output, (PostgreSQLIslandOperator)children.get(0), supplement);
			case "Limit":
				return new PostgreSQLIslandLimit(parameters, output, (PostgreSQLIslandOperator) children.get(0), supplement);
			case "Append":
				List<PostgreSQLIslandOperator> childs = new ArrayList<>();
				childs.addAll(children.stream().map(c -> (PostgreSQLIslandOperator) c).collect(Collectors.toSet()));
				return new PostgreSQLIslandMerge(parameters, output, childs, supplement);
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
