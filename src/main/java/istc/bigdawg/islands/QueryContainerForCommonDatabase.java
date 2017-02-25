package istc.bigdawg.islands;

import java.util.Map;
import java.util.Set;

import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.shims.Shim;

public class QueryContainerForCommonDatabase {
	
	private String								dbid;
	private ConnectionInfo 						databaseConnectionInfo;
	private Operator							rootOperator;
	private String								pruneToken;
	
	public QueryContainerForCommonDatabase (ConnectionInfo ci, String dbid, Operator o, String into) throws Exception {
		
		this.dbid 						= dbid;
		this.rootOperator				= o;
		this.pruneToken					= into;
		this.databaseConnectionInfo		= ci;
		
	}
	
	public String generateSelectIntoString(Scope scope) throws Exception {
		Shim gen = TheObjectThatResolvesAllDifferencesAmongTheIslands.getShim(scope, Integer.parseInt(dbid));
		return gen.getSelectIntoQuery(rootOperator, pruneToken, false);
	}
	
	public Map<String, Set<String>> generateObjectToExpressionMapping() throws Exception {
		Map<String, Set<String>> mapping = rootOperator.getObjectToExpressionMappingForSignature();
		rootOperator.removeCTEEntriesFromObjectToExpressionMapping(mapping);
		return mapping;
	}
	
	public String generateTreeExpression() throws Exception {
		return rootOperator.getTreeRepresentation(true);
	}

	public String getDBID() throws Exception {
		return dbid;
	}
	
	public ConnectionInfo getConnectionInfo() {
		return databaseConnectionInfo;
	}
	
	public String getName() {
		return pruneToken;
	}
	
	public Operator getRootOperator() {
		return rootOperator;
	}

}
