package istc.bigdawg.islands.Myria;

import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

public class MyriaQueryResult implements QueryResult {

	private String result;
	
	public MyriaQueryResult(String result) {
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
