/**
 * 
 */
package istc.bigdawg.query;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collection;

/**
 * @author Adam Dziedzic
 * 
 * 
 *
 */
public interface ConnectionInfo {

	/**
	 * 
	 * @return name of the host where the instance is running from which the
	 *         connection will be obtained
	 */
	public String getHost();

	/**
	 * 
	 * @return port number of the running instance from which the connection
	 *         will be obtained
	 */
	public String getPort();

	/**
	 * 
	 * @return user name who can can get the connection
	 */
	public String getUser();
	
	
	/**
	 * 
	 * @return password for the user
	 */
	public String getPassword();

	/**
	 *
	 * @return databases/binpath, etc
	 */
	public String getDatabase();
	
	/**
	 * @param objects a Collection of objects to be removed 
	 * @return a query that when run on the instance, removes all of the given objects.
	 */
	public String getCleanupQuery(Collection<String> objects);

	/**
	 *
	 * @param object the table for which the histogram should be computed
	 * @param attribute the attribute of the table for which the histogram should be computed
	 * @param start the minimum value contained in the histogram
	 * @param end the maximum value contained in the histogram
	 * @param numBuckets the number of buckets in the histogram
     * @return an array such that the ith value is equal to the number of elements stored in the ith bucket of the histogram
     */
	public long[] computeHistogram(String object, String attribute, double start, double end, int numBuckets) throws SQLException;

	public Pair<Number, Number> getMinMax(String object, String attribute) throws SQLException, ParseException;

	public DBHandler getHandler();
	
}
