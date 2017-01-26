package istc.bigdawg.accumulo;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.islands.TheObjectThatResolvesAllDifferencesAmongTheIslands;
import istc.bigdawg.query.ConnectionInfo;

public class AccumuloConnectionInfo implements ConnectionInfo {

	/**
	 * 
	 */
	private static final long serialVersionUID = -708473017100057496L;
	String zooKeeperHost;
	String zooKeeperPort;
	String accumuloInstanceName; // this goes to database
	String username;
	String password;

	public AccumuloConnectionInfo(String zkHost, String zkPort,
			String accumuloInstanceDatabase, String usr, String pw) {
		this.username = usr;
		this.password = pw;
		this.zooKeeperHost = zkHost;
		this.zooKeeperPort = zkPort;
		this.accumuloInstanceName = accumuloInstanceDatabase;
	}

	@Override
	public String getUrl() {
		// TODO Auto-generated method stub
		return zooKeeperHost + ':' + zooKeeperPort;
	}

	@Override
	public String getHost() {
		return zooKeeperHost;
	}

	@Override
	public String getPort() {
		return zooKeeperPort;
	}

	@Override
	public String getUser() {
		return username;
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public String getDatabase() {
		return accumuloInstanceName;
	}

	@Override
	public Collection<String> getCleanupQuery(Collection<String> objects) {
		List<String> cleanupStrings = new ArrayList<>();
		for (String s : objects) {
			cleanupStrings.add(TheObjectThatResolvesAllDifferencesAmongTheIslands.AccumuloDeleteTableCommandPrefix + s);
		}
		return cleanupStrings;
	}

	@Override
	public long[] computeHistogram(String object, String attribute,
			double start, double end, int numBuckets)
					throws LocalQueryExecutionException {
		return null;
	}

	@Override
	public Pair<Number, Number> getMinMax(String object, String attribute)
			throws LocalQueryExecutionException, ParseException {
		return null;
	}

	@Override
	public ExecutorEngine getLocalQueryExecutor()
			throws LocalQueryExecutorLookupException {
		try {
			return new AccumuloExecutionEngine(this);
		} catch (BigDawgException | AccumuloException
				| AccumuloSecurityException e) {
			e.printStackTrace();
			throw new LocalQueryExecutorLookupException("Cannot construct "
					+ AccumuloExecutionEngine.class.getName());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "AccumuloConnectionInfo [zooKeeperHost=" + zooKeeperHost
				+ ", zooKeeperPort=" + zooKeeperPort + ", accumuloInstanceName="
				+ accumuloInstanceName + ", username=" + username
				+ ", password=" + "Sorry, I cannot show it" + "]";
	}

}
