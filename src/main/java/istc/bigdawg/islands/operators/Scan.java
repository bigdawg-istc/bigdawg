package istc.bigdawg.islands.operators;

public interface Scan extends Operator {
	
	public String getSourceTableName();
	public void setSourceTableName(String srcTableName);
	public String generateRelevantJoinPredicate() throws Exception;
	
}
