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
	 * Connection contains information from which database we should export the
	 * data from. If the connection inidcates the database from which we export
	 * data then return true.
	 * 
	 * @param connection
	 *            A connection to a database.
	 * @return true if the exported uses this type of the connection to connect
	 *         to the database.
	 */
	public boolean isSupportedConnector(ConnectionInfo connection);
}
