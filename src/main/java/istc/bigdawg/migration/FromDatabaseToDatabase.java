package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.zookeeper.FollowRemoteNodes;
import istc.bigdawg.zookeeper.ZooKeeperUtils;

/**
 * @author Adam Dziedzic
 * 
 *         Base class for migrations of data between databases.
 */
public abstract class FromDatabaseToDatabase
		implements MigrationNetworkRequest {

	/** From where to migrate the data (which node/machine). */
	abstract public ConnectionInfo getConnectionFrom();

	/* To where to migrate the data (to which node/machine). */
	abstract public ConnectionInfo getConnecitonTo();

	/*
	 * List of locks acquired in ZooKeeper for data migration. Empty or null
	 * list indicates that locks for the data migration process have not been
	 * acquired as yet.
	 */
	private List<String> zooKeeperLocks = null;

	/**
	 * For serialization.
	 */
	private static final long serialVersionUID = 1L;

	/* log */
	private static Logger log = Logger.getLogger(FromDatabaseToDatabase.class);

	/**
	 * General method (interface) for other modules to call the migration.
	 * 
	 * @param connectionFrom
	 * @param objectFrom
	 *            which object/table/array to migrate from
	 * @param connectionTo
	 * @param objectTo
	 *            which object/table/array to migrate to
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws MigrationException
	 */
	abstract MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException;

	/** The method which implements the execution of data migration */
	public abstract MigrationResult executeMigration()
			throws MigrationException;

	/**
	 * This Callable object should be executed in a separate thread. The
	 * intention is that we wait for the response in a thread but in another
	 * thread we control if the machine to which we sent the request is up and
	 * running.
	 * 
	 * @param hostname
	 *            to which node/machine we should send the network request
	 * @return result (response) in reply to the request
	 */
	Callable<Object> sendNetworkRequest(String hostname) {
		return () -> {
			try {
				Object result = NetworkOut.send(this, hostname);
				return result;
			} catch (NetworkException e) {
				return e;
			}
		};
	}

	/**
	 * Execute the migration request; this node is the coordinator.
	 * 
	 */
	Callable<Object> executeMigrationFromLocalToRemote() {
		return () -> {
			/* execute the migration locally - on this machine */
			log.debug(
					"Migration will be executed from local database to a remote one.");
			try {
				return executeMigration();
			} catch (MigrationException e) {
				log.debug(e.getMessage());
				return e;
			}
		};
	}

	/**
	 * The execution for migration starts from the dispatch method which handles
	 * the request in the network.
	 */
	@Override
	public MigrationResult execute() throws MigrationException {
		return dispatch();
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
	public MigrationResult dispatch() throws MigrationException {
		Object result = null;
		/*
		 * check if the address is not a local host
		 */
		String hostnameFrom = this.getConnectionFrom().getHost();
		String hostnameTo = this.getConnecitonTo().getHost();
		String debugMessage = "current hostname is: "
				+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress()
				+ "; hostname from which the data is migrated: " + hostnameFrom
				+ "; hostname to which the data is migrated: " + hostnameTo;
		log.debug(debugMessage);
		try {
			if (!isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
				log.debug("Migration will be executed remotely (this node: "
						+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress()
						+ " only coordinates the migration).");
				/*
				 * this node is only a coordinator/dispatcher for the migration
				 */
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameFrom),
						sendNetworkRequest(hostnameFrom));
			} else {
				/*
				 * This machine contains the source database for the migration.
				 */
				/* Change the local hostname to the general ip address. */
				String thisHostname = BigDawgConfigProperties.INSTANCE
						.getGrizzlyIpAddress();
				if (zooKeeperLocks == null) {
					byte[] data = debugMessage.getBytes();
					zooKeeperLocks = ZooKeeperUtils.acquireMigrationLocks(
							Arrays.asList(thisHostname, hostnameTo), data);
				}
				if (!isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
					log.debug("Migration from a local: " + thisHostname
							+ " to remote database: " + hostnameTo);
					result = FollowRemoteNodes.execute(
							Arrays.asList(hostnameTo),
							executeMigrationFromLocalToRemote());
					log.debug("Result of migration: " + result);
				} else {
					log.debug("The total migration is executed locally.");
					return executeMigration();
				}
			}
			log.debug("Process results of the migration.");
			return MigrationResult.processResult(result);
		} catch (MigrationException e) {
			throw e;
		} catch (Exception e) {
			throw new MigrationException(e.getMessage(), e);
		} finally {
			if (zooKeeperLocks != null) {
				log.debug("Release the locks in ZooKeeper.");
				try {
					ZooKeeperUtils.releaseMigrationLocks(zooKeeperLocks);
				} catch (KeeperException | InterruptedException e) {
					log.error(
							"Could not release the following locks in ZooKeeper: "
									+ zooKeeperLocks.toString() + " "
									+ e.getMessage() + "Stack trace: "
									+ StackTrace.getFullStackTrace(e));
				}
			}
		}
	}

}
