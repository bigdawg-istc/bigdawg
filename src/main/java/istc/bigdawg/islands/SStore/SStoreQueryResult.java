package istc.bigdawg.islands.SStore;

import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

public class SStoreQueryResult implements QueryResult {

	private String result;
	
	public SStoreQueryResult(String result) {
		this.result = result;
	}
	
	@Override
	public String toPrettyString() {
		return result;
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return null;
	}


}
