package istc.bigdawg.executor;

import istc.bigdawg.query.ConnectionInfo;

public class IslandQueryResult implements QueryResult {

	private ConnectionInfo ci;
	
	public IslandQueryResult(ConnectionInfo ci) {
		this.ci = ci;
	}
	
	@Override
	public String toPrettyString() {
		return "Island Result placeholder; connection info: "+ci.toSimpleString();
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return ci;
	}

}
