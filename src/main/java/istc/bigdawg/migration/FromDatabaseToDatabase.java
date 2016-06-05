/**
 * 
 */
package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.zookeeper.FollowRemoteNodes;

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
	private static Logger log = Logger.getLogger(FromDatabaseToDatabase.class);

	abstract MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException;

	Callable<Object> sendNetworkRequest(String hostname) {
		return () -> {
			try {
				Object result = NetworkOut.send(this, hostname);
				return result;
			} catch (Exception e) {
				return e;
			}
		};
	}

	/** Execute the migration request; this node is the corrdinator. */
	Callable<Object> executeMigration() {
		return () -> {
			/* execute the migration locally - on this machine */
			log.debug("Migration will be executed locally.");
			try {
				return execute();
			} catch (Exception e) {
				return e;
			}
		};
	}

	/**
	 * Dispatch the request for migration to the node with destination database.
	 * 
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws UnknownHostException
	 * @throws NetworkException
	 * @throws MigrationException
	 */
	MigrationResult dispatch()
			throws UnknownHostException, NetworkException, MigrationException {
		Object result = null;
		/*
		 * check if the address is not a local host
		 */
		String hostnameFrom = this.getConnectionFrom().getHost();
		String hostnameTo = this.getConnecitonTo().getHost();
		log.debug("hostname from which the data is migrated: " + hostnameFrom);
		if (!isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
			log.debug("Migration will be executed remotely.");
			/* this node is only a coordinator/dispatcher for the migration */
			result = FollowRemoteNodes.execute(Arrays.asList(hostnameFrom),
					sendNetworkRequest(hostnameFrom));
		} else {
			if (!isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
				/** only the coordinator of the migration is local */
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameTo),
						executeMigration());
			} else {
				/* the total migration is executed locally */
				return execute();
			}
		}
		return MigrationResult.processResult(result);
	}
}
