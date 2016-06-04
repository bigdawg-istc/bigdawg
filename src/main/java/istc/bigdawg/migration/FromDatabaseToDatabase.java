/**
 * 
 */
package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.query.ConnectionInfo;

/**
 * @author Adam Dziedzic
 * 
 *         Base class for migrations of data between databases.
 */
public abstract class FromDatabaseToDatabase
		implements MigrationNetworkRequest {

	
	abstract public ConnectionInfo getConnectionFrom();
	abstract public ConnectionInfo getConnecitonTo(); 
	
	/**
	 * For serialization.
	 */
	private static final long serialVersionUID = 1L;

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToSciDB.class);

	abstract MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException;

	/**
	 * Dispatch the request for migration to the node with destination database.
	 * 
	 * @param connectionTo
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws UnknownHostException
	 * @throws NetworkException
	 * @throws MigrationException
	 */
	MigrationResult dispatch()
			throws UnknownHostException, NetworkException, MigrationException {
		/*
		 * check if the address is not a local host
		 */
		String hostname = this.getConnectionFrom().getHost();
		log.debug("hostname from which the data is migrated: " + hostname);
		if (!isThisMyIpAddress(InetAddress.getByName(hostname))) {
			log.debug("Migration will be executed remotely.");
			Object result = NetworkOut.send(this, hostname);
			return processResult(result);
		}
		/* execute the migration locally */
		log.debug("Migration will be executed locally.");
		return execute();
	}
}
