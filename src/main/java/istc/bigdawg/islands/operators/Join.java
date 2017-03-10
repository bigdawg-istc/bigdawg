package istc.bigdawg.islands.operators;

import istc.bigdawg.exceptions.IslandException;

public interface Join extends Operator {

	public enum JoinType  {Left, Natural, Right, Cross};
	
	// creates a default and call this to create a new Join instance
	public Join construct(Operator child0, Operator child1, JoinType jt, String joinPred, boolean isFilter) throws Exception;

	/**
	 * Must be in SQL-style predicate, such as "(a >= b) AND ((c = d) OR (e <> f)) AND ..."
	 * @return 
	 * @throws Exception
	 */
	public String generateJoinPredicate() throws IslandException;
	
	/**
	 * Must be in SQL-style predicate, such as "(a >= b) AND ((c = d) OR (e <> f)) AND ..."
	 * @return 
	 * @throws Exception
	 */
	public String generateJoinFilter() throws IslandException;
    
	public String getJoinToken();	
	public Integer getJoinID();
	public void setJoinID(Integer joinID);

};
