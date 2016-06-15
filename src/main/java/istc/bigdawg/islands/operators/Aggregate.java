package istc.bigdawg.islands.operators;

public interface Aggregate extends Operator {

	static final String aggergateNamePrefix = "BIGDAWGAGGREGATE_";
	
	public String getAggregateToken();
	public void setSingledOutAggregate();
	
	public Integer getAggregateID();
	public void setAggregateID(Integer aggregateID);

};