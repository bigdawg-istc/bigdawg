package istc.bigdawg.islands.operators;

public interface Merge extends Operator {

	public enum MergeType {Intersect, Union};
	
	public boolean setUnionAll(boolean isUnionAll);
	public boolean isUnionAll();
	
};