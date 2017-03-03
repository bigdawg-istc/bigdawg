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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.network.DataOut;
import istc.bigdawg.network.RemoteRequest;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;
import istc.bigdawg.zookeeper.FollowRemoteNodes;
import istc.bigdawg.zookeeper.ZooKeeperUtils;

/**
 * 
 * Base class for migrations of data between databases.
 * 
 * @author Adam Dziedzic
 */
public class FromDatabaseToDatabase implements MigrationNetworkRequest {

	/* log */
	private static Logger log = Logger.getLogger(FromDatabaseToDatabase.class);

	/** Port number for data migration (for big data transfer). */
	private static final int DATA_PORT;

	static {
		DATA_PORT = BigDawgConfigProperties.INSTANCE.getNetworkDataPort();
	}

	/** The handler for data export from a database. */
	private Export exporter;

	/** The handler for data loading to a database. */
	private Load loader;

	/*
	 * List of locks acquired in ZooKeeper for data migration. Empty or null
	 * list indicates that locks for the data migration process have not been
	 * acquired as yet.
	 */
	protected List<String> zooKeeperLocks = null;

	/** Pipes to be removed after the execution of the migration. */
	private List<String> pipes = new ArrayList<>();

	/** Executor used to run many tasks for the migration process. */
	private ExecutorService executorService;

	/**
	 * Information about the migration process: from/to connection, from/to
	 * table, additional parameters for the migration process.
	 */
	protected MigrationInfo migrationInfo;

	/**
	 * For serialization.
	 */
	private static final long serialVersionUID = 1L;

	/** Implicit constructor. */
	protected FromDatabaseToDatabase() {
	}

	/**
	 * Create instance with exporter and loader.
	 * 
	 * @param exporter
	 * @param loader
	 */
	FromDatabaseToDatabase(Export exporter, Load loader) {
		this.exporter = exporter;
		this.loader = loader;
	}

	/**
	 * Create the instance of the FromDatabaseToDatabase (accessible only within
	 * the package).
	 * 
	 * @param exporter
	 *            Handler to export the data.
	 * @param loader
	 *            Handler to load the data.
	 * @param migrationInfo
	 *            information about the migration (connections, objects, etc.).
	 */
	FromDatabaseToDatabase(Export exporter, Load loader,
			MigrationInfo migrationInfo) {
		/** Check if compatible export, migration request and loading. */
		checkSupportForExportLoad(exporter, loader, migrationInfo);

		/**
		 * if compatible exporter, loader, migration info, then set the fields
		 * of the instance.
		 */
		this.migrationInfo = migrationInfo;
		this.exporter = exporter;
		this.loader = loader;
		this.exporter.setMigrationInfo(migrationInfo);
		this.loader.setMigrationInfo(migrationInfo);

	}

	/** Register migrator: Export and Load executors. */
	public static FromDatabaseToDatabase register(Export exporter,
			Load loader) {
		return new FromDatabaseToDatabase(exporter, loader);
	}

	/**
	 * 
	 * @return From where to migrate the data (which node/machine).
	 * 
	 */
	public ConnectionInfo getConnectionFrom() {
		return migrationInfo.getConnectionFrom();
	}

	/**
	 * 
	 * @return the name of the object from which we should export the data
	 */
	public String getObjectFrom() {
		return migrationInfo.getObjectFrom();
	}

	/**
	 * @return To where to migrate the data (to which node/machine).
	 */
	public ConnectionInfo getConnectionTo() {
		return migrationInfo.getConnectionTo();
	}

	/**
	 * 
	 * @return the name of the object to which we should load the data
	 */
	public String getObjectTo() {
		return migrationInfo.getObjectTo();
	}

	/**
	 * 
	 * @return
	 */
	public MigrationInfo getMigrationInfo() {
		return migrationInfo;
	}

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
	 * Check if migration is compatible between migrationInfo (info about
	 * connections and objects to be moved), exporter, and loader.
	 * 
	 * Throws no exception if the exporter supports export from the database
	 * indicated by migrationInfo (connectionFrom) and loader supports data
	 * loading to the database indicated by migrationInfo (connectionTo), throw
	 * the IllegalStateException otherwise.
	 * 
	 * @param migrationInfo
	 *            information about the migration (connections, objects, etc.).
	 */
	private static void checkSupportForExportLoad(Export exporter, Load loader,
			MigrationInfo migrationInfo) {
		if (!exporter.isSupportedConnector(migrationInfo.getConnectionFrom())) {
			throw new IllegalStateException("Exporter: "
					+ exporter.getClass().getCanonicalName()
					+ " does not support export from the database "
					+ "represented by the following connection: "
					+ migrationInfo.getConnectionFrom().toSimpleString());
		}
		if (!loader.isSupportedConnector(migrationInfo.getConnectionTo())) {
			throw new IllegalStateException(
					"Loader: " + loader.getClass().getCanonicalName()
							+ " does not support data loading to the database "
							+ "represented by the following connection: "
							+ migrationInfo.getConnectionTo().toSimpleString());
		}
		return;
	}

	/**
	 * see: {@link #migrate(ConnectionInfo, String, ConnectionInfo, String) }
	 * 
	 * @param migrationInfo
	 *            information about the migration (connections, objects, etc.).
	 *            {@link MigrationInfo}
	 * @return {@link MigrationResult} Information about the results of the
	 *         migration process: number of elements (e.g. rows, items) which
	 *         were migrated, migration time and other statistics.
	 * @throws MigrationException
	 */
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		try {
			checkSupportForExportLoad(exporter, loader, migrationInfo);
		} catch (IllegalStateException e) {
			/**
			 * null denotes that this instance of FromDatabaseToDatabase class
			 * cannot serve the migration between the specified databases.
			 */
			return null;
		}
		this.migrationInfo = migrationInfo;
		exporter.setMigrationInfo(migrationInfo);
		loader.setMigrationInfo(migrationInfo);
		return this.dispatch();
	}

	/**
	 * The method which implements the execution of data migration from this
	 * local machine to a remote machine.
	 */
	public MigrationResult executeMigrationLocalRemote()
			throws MigrationException {
		try {
			String pipe = Pipe.INSTANCE
					.createAndGetFullName(this.getClass().getName() + "_from_"
							+ migrationInfo.getObjectFrom() + "_to_"
							+ migrationInfo.getObjectTo());
							/* pipe = "/tmp/adam"; */

			/* Add the pipe to be removed when cleaning the resources. */
			pipes.add(pipe);

			/* Set output for exporter and input for DataOut. */
			exporter.setExportTo(pipe);
			DataOut dataOut = new DataOut(
					migrationInfo.getConnectionTo().getHost(), DATA_PORT, pipe);

			/* set migration information for exporter and loader */
			exporter.setMigrationInfo(migrationInfo);
			loader.setMigrationInfo(migrationInfo);

			/*
			 * Set the handler from for the loader - this is to get meta
			 * information from the source object/array/table. To the same for
			 * exporter.
			 */
			exporter.setHandlerTo(loader.getHandler());
			loader.setHandlerFrom(exporter.getHandler());

			LoadRemote loadRemote = new LoadRemote(DATA_PORT, exporter, loader);

			/* activate the tasks */
			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(new RemoteRequest(loadRemote,
					migrationInfo.getConnectionTo().getHost()));
			tasks.add(exporter);
			tasks.add(dataOut);
			executorService = Executors.newFixedThreadPool(tasks.size());
			long startTimeMigration = System.currentTimeMillis();
			List<Future<Object>> results = TaskExecutor.execute(executorService,
					tasks);
			Object remoteLoadResult = results.get(0).get();
			if (remoteLoadResult instanceof Exception) {
				Exception ex = (Exception) remoteLoadResult;
				throw new MigrationException("Remote data loading failed.", ex);
			}
			LoadRemoteResult loadingResult = (LoadRemoteResult) remoteLoadResult;
			Long countLoadedElements = loadingResult.getCountLoadedElements();
			Long countExtractedElements = (Long) results.get(1).get();
			Long bytesSent = (Long) results.get(2).get();
			Long bytesReceived = (Long) loadingResult.getBytesReceived();
			if (!bytesSent.equals(bytesReceived)) {
				throw new MigrationException(
						"Problem with data transfer via network. "
								+ "Not all bytes sent (" + bytesSent
								+ ") were received on the remote host "
								+ "(bytes received: " + bytesReceived + ").");
			}
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, countLoadedElements, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (Exception e) {
			String msg = e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		} finally {
			cleanResources();
		}
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
							/* pipe = "/tmp/adam"; */

			/* add the pipe to be removed when cleaning the resources */
			pipes.add(pipe);

			/* set output for exporter and input for importer */
			exporter.setExportTo(pipe);
			loader.setLoadFrom(pipe);

			/* set migration information for exporter and loader */
			exporter.setMigrationInfo(migrationInfo);
			loader.setMigrationInfo(migrationInfo);

			/*
			 * set the handler from for the loader - this is to get meta
			 * information from the source table
			 */
			loader.setHandlerFrom(exporter.getHandler());
			exporter.setHandlerTo(loader.getHandler());

			/* activate the tasks */
			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(exporter);
			tasks.add(loader);
			executorService = Executors.newFixedThreadPool(tasks.size());
			long startTimeMigration = System.currentTimeMillis();
			List<Future<Object>> results = TaskExecutor.execute(executorService,
					tasks);
			Long countExtractedElements = (Long) results.get(0).get();
			Long countLoadedElements = (Long) results.get(1).get();
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, countLoadedElements, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (Exception e) {
			String msg = e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		} finally {
			cleanResources();
		}
	}

	/**
	 * Clean the resources that were allocated for the migration process.
	 */
	private void cleanResources() {
		for (String pipe : pipes) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(pipe);
			} catch (IOException e) {
				log.error("Problem when removing the pipe/file: " + pipe + " "
						+ e.getMessage() + StackTrace.getFullStackTrace(e), e);
			}
		}
		pipes.clear();
		if (executorService != null) {
			if (!executorService.isShutdown()) {
				executorService.shutdownNow();
			}
			executorService = null;
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
	public static MigrationResult summary(MigrationResult migrationResult,
			MigrationInfo migrationInfo, String message) throws SQLException {
		log.debug("migration duration time msec: "
				+ migrationResult.getDurationMsec());
		MigrationStatistics stats = new MigrationStatistics(
				migrationInfo.getConnectionFrom(),
				migrationInfo.getConnectionTo(), migrationInfo.getObjectFrom(),
				migrationInfo.getObjectTo(),
				migrationResult.getStartTimeMigration(),
				migrationResult.getEndTimeMigration(),
				migrationResult.getCountExtractedElements(),
				migrationResult.getCountLoadedElements(), message);
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
			// Todo: fix this check so that docker works
			//if (!isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
			if (isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
				log.debug("Source and target hosts are on different IPs. "
						+ "Migration will be executed remotely (this node: "
						+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress()
						+ " only coordinates the migration).");
				/*
				 * this node is only a coordinator/dispatcher for the migration
				 */
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameFrom),
						new RemoteRequest(this, hostnameFrom));
			} else {
				/*
				 * This machine contains the source database for the migration.
				 */
				/* Change the local hostname to the general ip address. */
				String thisHostname = BigDawgConfigProperties.INSTANCE
						.getGrizzlyIpAddress();
				/** Acquire the ZooKeeper locks for the migration. */
				// if (zooKeeperLocks == null) {
				// byte[] data = debugMessage.getBytes();
				// zooKeeperLocks = ZooKeeperUtils.acquireMigrationLocks(
				// Arrays.asList(thisHostname, hostnameTo), data);
				// }
				// if (!isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
				if (isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
					log.debug("Migration from a local: " + thisHostname
							+ " to remote database: " + hostnameTo);
					/*
					 * The execution through the FollowRemoteNodes will ensure
					 * that we track the remote node if it is alive and can
					 * carry out the remaining tasks.
					 */
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

}
