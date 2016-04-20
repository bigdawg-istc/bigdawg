package istc.bigdawg.plan.operators;

import istc.bigdawg.plan.generators.OperatorVisitor;

public interface OperatorInterface {
	public void accept(OperatorVisitor operatorVisitor) throws Exception;
}
