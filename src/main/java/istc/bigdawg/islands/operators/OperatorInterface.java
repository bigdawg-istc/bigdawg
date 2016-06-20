package istc.bigdawg.islands.operators;

import istc.bigdawg.islands.OperatorVisitor;

public interface OperatorInterface {
	public void accept(OperatorVisitor operatorVisitor) throws Exception;
}
