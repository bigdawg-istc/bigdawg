package istc.bigdawg.islands;

import java.util.List;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Operator;

public interface Island {
	
	public IntraIslandQuery getIntraIslandQuery(String islandQuery, String name, Map<String, String> transitionSchemas) throws IslandException;
	
	public void setupForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas) throws IslandException;
	public void teardownForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas) throws IslandException;
	public String getCreateStatementForTransitionTable(String tableName, String attributes);
	public int addCatalogObjectEntryForTemporaryTable(String tableName) throws IslandException;

	public Operator parseQueryAndExtractAllTableNames(String queryString, List<String> tables) throws IslandException;
	public Set<String> splitJoinPredicate(String predicates) throws IslandException;
	public Integer getDefaultCastReceptionDBID();
	public List<String> getLiteralsAndConstantsSignature(String query) throws IslandException;
	public String wrapQueryInIslandIdentifier(String query);
	
	// Should these be here as well?
	public Join constructJoin (Operator o1, Operator o2, JoinType jt, List<String> joinPred, boolean isFilter) throws IslandException;
}
