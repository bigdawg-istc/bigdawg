/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.query.ConnectionInfo;

/**
 * @author Adam Dziedzic
 * 
 *         Check if this connection (which determines a database engine) is
 *         supported.
 */
public interface ConnectorChecker {
	/**
	 * Connection contains information from which (or to which) database we
	 * should export the data from (or load the data to). If the connection
	 * indicates the database from which the exporter can export the data (or
	 * loader can load the data to) then return true.
	 * 
	 * @param connection
	 *            A connection to a database.
	 * @return true if the exporter uses this type of the connection to connect
	 *         to the database.
	 */
	public boolean isSupportedConnector(ConnectionInfo connection);
}
