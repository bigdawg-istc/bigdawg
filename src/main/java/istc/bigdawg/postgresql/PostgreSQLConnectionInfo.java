/**
 * 
 */
package istc.bigdawg.postgresql;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.query.ConnectionInfo;

/**
 * Information about connection to an instance of PostgreSQL.
 * 
 * @author Adam Dziedzic
 */
public class PostgreSQLConnectionInfo implements ConnectionInfo {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String host;
	private String port;
	private String database;
	private String user;
	private String password;

	private static final String CLEANUP_STRING = "DROP TABLE %s;";

	public PostgreSQLConnectionInfo(String host, String port, String database,
			String user, String password) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.user = user;
		this.password = password;
	}

	public String getUrl() {
		return "jdbc:postgresql://" + getHost() + ":" + getPort() + "/"
				+ getDatabase();
	}

	public static String getUrl(String host, String port, String database) {
		return "jdbc:postgresql://" + host + ":" + port + "/"
				+ database;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * This is a specific property for PostgreSQL database.
	 * 
	 * @return the database name to which the connection should be established
	 */
	public String getDatabase() {
		return database;
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

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		String NEW_LINE = System.getProperty("line.separator");

		result.append(this.getClass().getName() + " Object {" + NEW_LINE);
		result.append(" Host: " + this.getHost() + NEW_LINE);
		result.append(" Port: " + this.getPort() + NEW_LINE);
		result.append(" Database: " + this.getDatabase() + NEW_LINE);
		result.append(" User: " + this.getUser() + NEW_LINE);
		result.append(" Password: This is a secret!" + NEW_LINE);
		result.append("}");

		return result.toString();
	}

	@Override
	public Collection<String> getCleanupQuery(Collection<String> objects) {
		return Collections.singleton(
				String.format(CLEANUP_STRING, String.join(", ", objects)));
	}

	@Override
	public long[] computeHistogram(String object, String attribute,
			double start, double end, int numBuckets)
					throws ExecutorEngine.LocalQueryExecutionException {
		// TODO: handle non-numerical data
		long[] result = new long[numBuckets];

		String query = "SELECT width_bucket(%s, %s, %s, %s), COUNT(*) FROM %s GROUP BY 1 ORDER BY 1;";
		List<List<String>> raw = ((JdbcQueryResult) new PostgreSQLHandler(this)
				.execute(String.format(query, attribute, start, end, numBuckets,
						object))
				.get()).getRows();

		for (int i = 0; i < raw.size(); i++) {
			List<String> row = raw.get(i);
			result[Integer.parseInt(row.get(0))] = Long.parseLong(row.get(1));
		}

		return result;
	}

	@Override
	public Pair<Number, Number> getMinMax(String object, String attribute)
			throws ExecutorEngine.LocalQueryExecutionException, ParseException {
		String query = "SELECT min(%s), max(%s) FROM %s;";
		List<String> raw = ((JdbcQueryResult) new PostgreSQLHandler(this)
				.execute(String.format(query, attribute, attribute, object))
				.get()).getRows().get(0);
		NumberFormat nf = NumberFormat.getInstance();
		return new ImmutablePair<>(nf.parse(raw.get(0)), nf.parse(raw.get(1)));
	}

	public ExecutorEngine getLocalQueryExecutor() {
		return new PostgreSQLHandler(this);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((database == null) ? 0 : database.hashCode());
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
		if (!(obj instanceof PostgreSQLConnectionInfo))
			return false;
		PostgreSQLConnectionInfo other = (PostgreSQLConnectionInfo) obj;
		if (database == null) {
			if (other.database != null)
				return false;
		} else if (!database.equals(other.database))
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
}
