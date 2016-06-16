/**
 * 
 */
package istc.bigdawg.scidb;

import java.text.ParseException;
import java.util.Collection;
import java.util.stream.Collectors;

import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Connection information about the SciDB database.
 * 
 * @author Adam Dziedzic
 *
 */
public class SciDBConnectionInfo implements ConnectionInfo {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String host;
	private String port;
	private String user;
	private String password;
	private String binPath;
	private String database;

	/**
	 * 
	 */
	public SciDBConnectionInfo() {
		this.host = BigDawgConfigProperties.INSTANCE.getScidbHostname();
		this.port = BigDawgConfigProperties.INSTANCE.getScidbPort();
		this.user = BigDawgConfigProperties.INSTANCE.getScidbUser();
		this.password = BigDawgConfigProperties.INSTANCE.getScidbPassword();
		this.binPath = BigDawgConfigProperties.INSTANCE.getScidbBinPath();
	}

	public SciDBConnectionInfo(String host, String port, String user,
			String password, String binPath) {
		this(host, port, user, password, binPath, null);
	}

	public SciDBConnectionInfo(String host, String port, String user,
			String password, String binPath, String database) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.binPath = binPath;
		this.database = database;
	}

	public String getUrl() {
		return "jdbc:scidb://" + getHost() + ":" + getPort() + "/";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getHost()
	 */
	@Override
	public String getHost() {
		return host;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getPort()
	 */
	@Override
	public String getPort() {
		return port;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getUser()
	 */
	@Override
	public String getUser() {
		return user;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getPassword()
	 */
	@Override
	public String getPassword() {
		return password;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.query.ConnectionInfo#getCleanupQuery(java.util.Collection)
	 */
	@Override
	public Collection<String> getCleanupQuery(Collection<String> objects) {
		// TODO(ankush) Auto-generated method stub
		return objects.stream().map(o -> "remove(" + o + ")")
				.collect(Collectors.toSet());
	}

	@Override
	public long[] computeHistogram(String object, String attribute,
			double start, double end, int numBuckets) {
		// TODO(ankush) implement histogram computation
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<Number, Number> getMinMax(String object, String attribute)
			throws ExecutorEngine.LocalQueryExecutionException, ParseException {
		// TODO(ankush) implement min/max computation
		throw new UnsupportedOperationException();
	}

	public ExecutorEngine getLocalQueryExecutor()
			throws LocalQueryExecutorLookupException {
		try {
			return new SciDBHandler(this);
		} catch (Exception e) {
			throw new LocalQueryExecutorLookupException(e);
		}
	}

	/**
	 * @return the binPath
	 */
	public String getBinPath() {
		return binPath;
	}

	@Override
	public String toString() {
		return "SciDBConnectionInfo [host=" + host + ", port=" + port
				+ ", user=" + user + ", password=" + password + ", binPath="
				+ binPath + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((binPath == null) ? 0 : binPath.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result
				+ ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((port == null) ? 0 : port.hashCode());
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SciDBConnectionInfo other = (SciDBConnectionInfo) obj;
		if (binPath == null) {
			if (other.binPath != null)
				return false;
		} else if (!binPath.equals(other.binPath))
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		if (port == null) {
			if (other.port != null)
				return false;
		} else if (!port.equals(other.port))
			return false;
		if (user == null) {
			if (other.user != null)
				return false;
		} else if (!user.equals(other.user))
			return false;
		return true;
	}

	@Override
	public String getDatabase() {
		return database;
	}

}
