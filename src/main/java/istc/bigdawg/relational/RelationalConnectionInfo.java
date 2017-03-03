/**
 * 
 */
package istc.bigdawg.relational;

import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.query.ConnectionInfo;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Information about connection to an instance of a relational engine.
 * 
 * @author Kate Yu
 */
public interface RelationalConnectionInfo extends ConnectionInfo {


	public String getUrl();

	public void setHost(String host);

	public void setPort(String port);

	public void setDatabase(String database);

	public void setUser(String user);

	public void setPassword(String password);
	/**
	 * This is a specific property for the database.
	 *
	 * @return the database name to which the connection should be established
	 */
	public String getDatabase();

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getHost()
	 */
	@Override
	public String getHost();

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getPort()
	 */
	@Override
	public String getPort();

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getUser()
	 */
	@Override
	public String getUser();

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.ConnectionInfo#getPassword()
	 */
	@Override
	public String getPassword();

	@Override
	public Collection<String> getCleanupQuery(Collection<String> objects);
	@Override
	public long[] computeHistogram(String object, String attribute,
			double start, double end, int numBuckets)
					throws ExecutorEngine.LocalQueryExecutionException;
	@Override
	public Pair<Number, Number> getMinMax(String object, String attribute)
			throws ExecutorEngine.LocalQueryExecutionException, ParseException;

	public ExecutorEngine getLocalQueryExecutor();
}
