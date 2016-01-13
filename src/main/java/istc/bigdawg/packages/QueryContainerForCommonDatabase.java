package istc.bigdawg.packages;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.query.ConnectionInfo;
import net.sf.jsqlparser.statement.select.Select;

public class QueryContainerForCommonDatabase {
	
	private HashMap<String, ConnectionInfo> 	databaseConnectionInfos;
	private Operator								rootOperator;
	private Select									select;
	private String									pruneToken;
	
	public QueryContainerForCommonDatabase (Map<String, ConnectionInfo> cis, Operator o, Select srcQuery, String into) throws Exception {
		
		this.rootOperator				= o;
		this.select						= srcQuery;
		this.pruneToken					= into;
		this.databaseConnectionInfos	= new HashMap<>();
		
		this.databaseConnectionInfos.putAll(cis);
		
		System.out.println("--> Gen select into: "+generateSelectIntoString());
	}

	public Set<String> getConnectionStrings() throws Exception {
		return databaseConnectionInfos.keySet();
	}
	
	public Map<String, ConnectionInfo> getConnectionInfos() {
		return databaseConnectionInfos;
	}
	
	public String generateSelectIntoString() throws Exception {
		return rootOperator.generateSelectForExecutionTree(select, pruneToken);
	}
	
	public String getName() {
		return pruneToken;
	}
}
