package istc.bigdawg.islands.operators;

public interface Join extends Operator {

	public enum JoinType  {Left, Natural, Right, Cross};
	
	final String BigDAWGJoinToken = "BIGDAWGJOINTOKEN_";
	
	// creates a default and call this to create a new Join instance
	public Join construct(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws Exception;
	
//	public Join(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws JSQLParserException {
//		this.isCTERoot = false; 
//		this.isBlocking = false; 
//		this.isPruned = false;
//		this.isCopy = true;
//		
//		maxJoinSerial++;
//		this.setJoinID(maxJoinSerial);
//		 
//		
//		if (jt != null) this.joinType = jt;
//		
//		this.isQueryRoot = true;
//		
//		this.children = new ArrayList<>();
//		this.children.add(child0);
//		this.children.add(child1);
//		
//		if (child0.isCopy()) child0.parent = this;
//		if (child1.isCopy()) child1.parent = this;
//		
//		child0.isQueryRoot = false;
//		child1.isQueryRoot = false;
//	}

	public String generateJoinPredicate() throws Exception;
	public String generateJoinFilter() throws Exception;
    
	public String getJoinToken();	
	public Integer getJoinID();
	public void setJoinID(Integer joinID);

};
