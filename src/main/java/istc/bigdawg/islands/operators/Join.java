package istc.bigdawg.islands.operators;

import istc.bigdawg.exceptions.IslandException;

public interface Join extends Operator {

	// Need to suppor the following join types
	// https://github.com/postgres/postgres/blob/ab7646ff92c799303b9ee70ce88b89e07dae717c/src/backend/commands/explain.c#L1475
	public enum JoinType  {Left, Natural, Right, Cross, Inner, Full, Semi, Anti, Simple};
	
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
	public JoinType getJoinType();
};
