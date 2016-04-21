package istc.bigdawg.plan.generators;

import istc.bigdawg.plan.operators.Aggregate;
import istc.bigdawg.plan.operators.CommonSQLTableExpressionScan;
import istc.bigdawg.plan.operators.Distinct;
import istc.bigdawg.plan.operators.Join;
import istc.bigdawg.plan.operators.Limit;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.Scan;
import istc.bigdawg.plan.operators.SeqScan;
import istc.bigdawg.plan.operators.Sort;
import istc.bigdawg.plan.operators.WindowAggregate;

public interface OperatorVisitor {
	public void configure(boolean isRoot, boolean stopAtJoin);
	public void visit(Operator operator) throws Exception;
	public void visit(Join join) throws Exception;
	public void visit(Sort sort) throws Exception;
	public void visit(Distinct distinct) throws Exception;
	public void visit(Scan scan) throws Exception;
	public void visit(CommonSQLTableExpressionScan cte) throws Exception;
	public void visit(SeqScan operator) throws Exception;
	public void visit(Aggregate aggregate) throws Exception;
	public void visit(WindowAggregate operator) throws Exception;
	public void visit(Limit limit) throws Exception;
	public String generateStatementString() throws Exception;
	public Join generateStatementForPresentNonJoinSegment(Operator operator, StringBuilder sb, boolean isSelect) throws Exception;
	public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception;
	
	public String generateCreateStatementLocally(Operator op, String name) throws Exception;
}
