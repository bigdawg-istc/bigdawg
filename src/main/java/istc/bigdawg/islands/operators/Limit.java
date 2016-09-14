package istc.bigdawg.islands.operators;

public interface Limit extends Operator {

	public long getLimitCount();
	public void setLimitCount(long limitCount);

	public long getLimitOffset();
	public void setLimitOffset(long limitOffset);

	public boolean isHasOffSet();
	public void setHasOffSet(boolean hasOffSet);

};