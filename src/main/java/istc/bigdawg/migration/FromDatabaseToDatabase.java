package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;
import istc.bigdawg.zookeeper.FollowRemoteNodes;
import istc.bigdawg.zookeeper.ZooKeeperUtils;

/**
 * @author Adam Dziedzic
 * 
 *         Base class for migrations of data between databases.
 */
public class FromDatabaseToDatabase implements MigrationNetworkRequest {

	/* log */
	private static Logger log = Logger.getLogger(FromDatabaseToDatabase.class);

	/** The handler for data export from a database. */
	private Export exporter;

	/** The handler for data loading to a database. */
	private Load loader;

	/*
	 * List of locks acquired in ZooKeeper for data migration. Empty or null
	 * list indicates that locks for the data migration process have not been
	 * acquired as yet.
	 */
	private List<String> zooKeeperLocks = null;

	/**
	 * Information about the migration process: from/to connection, from/to
	 * table, etc.
	 */
	private MigrationInfo migrationInfo;

	/**
	 * For serialization.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * General method (interface) for other modules to call the migration
	 * process.
	 * 
	 * @param connectionFrom
	 *            Information about the source database (host, port, database
	 *            name, user password) from which the data should be extracted.
	 * @param objectFrom
	 *            The name of the object (e.g. table, array) which should be
	 *            extracted from the source database.
	 * @param connectionTo
	 *            Information about the destination database (host, port,
	 *            database name, user password) to which the data should be
	 *            loaded.
	 * @param objectTo
	 *            The name of the object (e.g. table, array) which should be
	 *            loaded to the destination database.
	 * @return {@link MigrationResult} Information about the results of the
	 *         migration process: number of elements (e.g. rows, items) which
	 *         were migrated, migration time and other statistics.
	 * @throws MigrationException
	 *             Informs that the migration process fails and returns details
	 *             information about the failure (general information about the
	 *             failure, what caused the failure, stack trace and other debug
	 *             information).
	 */
	public MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * see: {@link #migrate(ConnectionInfo, String, ConnectionInfo, String) }
	 * 
	 * @param migrationInfo
	 * @return {@link MigrationResult} Information about the results of the
	 *         migration process: number of elements (e.g. rows, items) which
	 *         were migrated, migration time and other statistics.
	 * @throws MigrationException
	 */
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		if (exporter.isSupportedConnector(migrationInfo.getConnectionFrom())
				&& loader.isSupportedConnector(
						migrationInfo.getConnectionTo())) {
			exporter.setMigrationInfo(migrationInfo);
			loader.setMigrationInfo(migrationInfo);
			return this.dispatch();
		}
		/**
		 * null denotes that this instance of FromDatabaseToDatabase class
		 * cannot serve the migration between the specified databases.
		 */
		return null;
	}

	/** The method which implements the execution of data migration */
	public MigrationResult executeMigrationLocalRemote()
			throws MigrationException {
		// TODO: migration via network
		return null;
	}

	/**
	 * Execute the migration on this single machine.
	 * 
	 * @return {@link MigrationResult}
	 * @throws MigrationException
	 * @throws @throws
	 *             RunShellException
	 * @throws InterruptedException
	 * @throws java.io.IOException
	 */
	public MigrationResult executeMigrationLocally() throws MigrationException {
		try {
			String pipe = Pipe.INSTANCE
					.createAndGetFullName(this.getClass().getName() + "_from_"
							+ migrationInfo.getObjectFrom() + "_to_"
							+ migrationInfo.getObjectTo());
			exporter.setExportTo(pipe);
			loader.setLoadFrom(pipe);
			ExecutorService executor = Executors.newFixedThreadPool(2);
			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(exporter);
			tasks.add(loader);
			executor = Executors.newFixedThreadPool(tasks.size());
			long startTimeMigration = System.currentTimeMillis();
			List<Future<Object>> results = TaskExecutor.execute(executor,
					tasks);
			long countExtractedElements = (Long) results.get(0).get();
			long countLoadedElements = (Long) results.get(0).get();
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			return summary(new MigrationResult(countExtractedElements,
					countLoadedElements, durationMsec, startTimeMigration,
					endTimeMigration));
		} catch (IOException | InterruptedException | RunShellException
				| ExecutionException | SQLException e) {
			String msg = e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		}
	}

	/**
	 * Generate the summary of the information about the migration process, send
	 * it to the monitor and log it.
	 * 
	 * @param migrationResult
	 * @return {@link MigrationResult}
	 * @throws SQLException
	 */
	public MigrationResult summary(MigrationResult migrationResult)
			throws SQLException {
		log.debug("migration duration time msec: "
				+ migrationResult.getDurationMsec());
		MigrationStatistics stats = new MigrationStatistics(
				migrationInfo.getConnectionFrom(),
				migrationInfo.getConnectionTo(), migrationInfo.getObjectFrom(),
				migrationInfo.getObjectTo(),
				migrationResult.getStartTimeMigration(),
				migrationResult.getEndTimeMigration(),
				migrationResult.getCountExtractedElements(),
				migrationResult.getCountLoadedElements(),
				this.getClass().getName());
		Monitor.addMigrationStats(stats);
		log.debug("Migration result,connectionFrom,"
				+ migrationInfo.getConnectionFrom().toSimpleString()
				+ ",connectionTo,"
				+ migrationInfo.getConnectionTo().toSimpleString()
				+ ",fromTable," + migrationInfo.getObjectFrom() + ",toTable,"
				+ migrationInfo.getObjectTo() + ",startTimeMigration,"
				+ migrationResult.getStartTimeMigration()
				+ ",endTi	meMigration,"
				+ migrationResult.getEndTimeMigration()
				+ ",countExtractedElements,"
				+ migrationResult.getCountExtractedElements()
				+ ",countLoadedElements,"
				+ migrationResult.getCountLoadedElements() + ",durationMsec,"
				+ migrationResult.getDurationMsec());
		return migrationResult;
	}

	/**
	 * This Callable object should be executed in a separate thread. The
	 * intention is that we wait for the response in a thread but in another
	 * thread we control if the remote machine to which we sent the request is
	 * up and running. If the remote machine fails, then we stop the migration
	 * process.
	 * 
	 * @param hostname
	 *            to which node/machine we should send the network request
	 * @return result (response) in reply to the request
	 * 
	 *         This is accessible only in this package.
	 */
	public Callable<Object> sendNetworkRequest(String hostname) {
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
	public Callable<Object> executeMigrationFromLocalToRemote() {
		return () -> {
			/*
			 * execute the migration: export from local machine, load on a
			 * remote machine
			 */
			log.debug("Migration will be executed from local "
					+ "database to a remote one.");
			try {
				return executeMigrationLocalRemote();
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
		String hostnameTo = this.getConnectionTo().getHost();
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
				/** Acquire the ZooKeeper locks for the migration. */
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
					return executeMigrationLocally();
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

	/** From where to migrate the data (which node/machine). */
	public ConnectionInfo getConnectionFrom() {
		return migrationInfo.getConnectionFrom();
	}

	/* To where to migrate the data (to which node/machine). */
	public ConnectionInfo getConnectionTo() {
		return migrationInfo.getConnectionTo();
	}

	/** Implicit constructor. */
	protected FromDatabaseToDatabase() {
	}

	/**
	 * Create instance with exporter and loader.
	 * 
	 * @param exporter
	 * @param loader
	 */
	protected FromDatabaseToDatabase(Export exporter, Load loader) {
		this.exporter = exporter;
		this.loader = loader;
	}

	/** Register migrator: Export and Load executors. */
	public static FromDatabaseToDatabase register(Export exporter,
			Load loader) {
		return new FromDatabaseToDatabase(exporter, loader);
	}

}
