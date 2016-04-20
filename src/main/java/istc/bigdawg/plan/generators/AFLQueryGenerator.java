package istc.bigdawg.plan.generators;

import java.util.Set;

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

public class AFLQueryGenerator implements OperatorVisitor {

	@Override
	public void configure(boolean isRoot, boolean stopAtJoin, Set<String> allowedScans) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Operator operator) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Join join) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Sort sort) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Distinct distinct) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Scan scan) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CommonSQLTableExpressionScan cte) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SeqScan operator) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Aggregate aggregate) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WindowAggregate operator) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Limit limit) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String generateStatementString() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Join generateStatementForPresentNonJoinSegment(Operator operator, StringBuilder sb, boolean isSelect)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String generateSelectIntoStatementForExecutionTree(String destinationTable) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
