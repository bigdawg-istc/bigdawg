package istc.bigdawg.query;

import java.sql.Connection;
import java.sql.SQLException;

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
	ObjectMetaData getObjectMetaData(String name) throws Exception;

	/**
	 * Get a JDBC connection to a database.
	 * 
	 * @return connection to a database
	 */
	public Connection getConnection() throws SQLException;

	/**
	 * 
	 * @param name
	 *            Name of the object (table/array etc.)
	 * @return true if the object with the specified name exists, false
	 *         otherwise.
	 */
	boolean existsObject(String name) throws Exception;

	/**
	 * Release all the resources hold by the handler.
	 */
	public void close() throws Exception;

	/**
	 * 
	 * @return true if the header is added to the data in CSV format and this
	 *         option cannot be switched off
	 */
	default public boolean isCsvExportHeader() {
		return false;
	}

	/**
	 * 
	 * @return true if the header is required to load the data in CSV format and
	 *         this option cannot be switched off
	 */
	default public boolean isCsvLoadHeader() {
		return false;
	}

	/**
	 * 
	 * @return default delimiter for CSV export
	 */
	default public String getCsvExportDelimiter() {
		return "|";
	}
}
