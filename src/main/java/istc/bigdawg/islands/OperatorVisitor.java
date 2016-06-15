package istc.bigdawg.islands;

import java.util.List;

import istc.bigdawg.islands.operators.Aggregate;
import istc.bigdawg.islands.operators.CommonTableExpressionScan;
import istc.bigdawg.islands.operators.Distinct;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Limit;
import istc.bigdawg.islands.operators.Merge;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Scan;
import istc.bigdawg.islands.operators.SeqScan;
import istc.bigdawg.islands.operators.Sort;
import istc.bigdawg.islands.operators.WindowAggregate;

public interface OperatorVisitor {
	public void configure(boolean isRoot, boolean stopAtJoin);
	public void reset(boolean isRoot, boolean stopAtJoin);
	public void visit(Operator operator) throws Exception;
	public void visit(Join join) throws Exception;
	public void visit(Sort sort) throws Exception;
	public void visit(Distinct distinct) throws Exception;
	public void visit(Scan scan) throws Exception;
	public void visit(CommonTableExpressionScan cte) throws Exception;
	public void visit(SeqScan operator) throws Exception;
	public void visit(Aggregate aggregate) throws Exception;
	public void visit(WindowAggregate operator) throws Exception;
	public void visit(Limit limit) throws Exception;
	public void visit(Merge merge) throws Exception;
	public String generateStatementString() throws Exception;
	public Operator generateStatementForPresentNonMigratingSegment(Operator operator, StringBuilder sb, boolean isSelect) throws Exception;
	public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception;
	
	/*
	 * required content and order: Comparator string, left table, left column, right table, right column 
	 */
	public List<String> getJoinPredicateObjectsForBinaryExecutionNode(Join join) throws Exception;
	public String generateCreateStatementLocally(Operator op, String name) throws Exception;
}
