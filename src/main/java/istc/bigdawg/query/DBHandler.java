package istc.bigdawg.query;

import javax.ws.rs.core.Response;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.database.ObjectMetaData;

public interface DBHandler {

	/**
	 * 
	 * @param queryString
	 *            the query to be executed
	 * @return result of the query
	 */
	Response executeQuery(String queryString);

	/**
	 * 
	 * @return The name of the shim in which the handler operates.
	 */
	Shim getShim();

	/**
	 * 
	 * @param name
	 *            name of the object (table, array, etc.)
	 * @return the meta data about the object (e.g. names of the attributes)
	 * @throws Exception
	 *             (probably a connection to the database failed).
	 */
	public ObjectMetaData getObjectMetaData(String name) throws Exception;

	/**
	 * 
	 * @param name
	 *            Name of the object (table/array etc.)
	 * @return true if the object with the specified name exists, false
	 *         otherwise.
	 */
	public boolean existsObject(String name) throws Exception;
}
