package istc.bigdawg.executor;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.query.ConnectionInfo;

public class ConstructedQueryResult implements QueryResult {

	private ConnectionInfo ci;
	private List<List<String>> results;
	
	public ConstructedQueryResult (ConnectionInfo ci) {
		this.ci = ci;
	}
	
	public ConstructedQueryResult(List<List<String>> results, ConnectionInfo ci) {
		this(ci);
		this.results = new ArrayList<>(results);
	}
	
	@Override
	public String toPrettyString() {
		StringBuilder sb = new StringBuilder();
		for (List<String> row : results) {
			sb.append(String.join("\t", row));
			sb.append('\n');
		}
		return sb.toString();
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return ci;
	}

}
