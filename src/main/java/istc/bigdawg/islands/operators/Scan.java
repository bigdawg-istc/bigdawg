package istc.bigdawg.islands.operators;

import istc.bigdawg.exceptions.IslandException;

public interface Scan extends Operator {
	
	public String getSourceTableName();
	public void setSourceTableName(String srcTableName);
	public String generateRelevantJoinPredicate() throws IslandException;
	
}
