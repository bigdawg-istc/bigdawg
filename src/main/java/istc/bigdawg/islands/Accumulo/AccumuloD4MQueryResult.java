package istc.bigdawg.islands.Accumulo;

import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

public class AccumuloD4MQueryResult implements QueryResult {

	private String result;
	
	public AccumuloD4MQueryResult(String result) {
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
