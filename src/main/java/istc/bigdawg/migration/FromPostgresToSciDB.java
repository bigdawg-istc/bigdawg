/**
 * 
 */
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
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.network.RemoteRequest;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;
import istc.bigdawg.zookeeper.FollowRemoteNodes;
import istc.bigdawg.zookeeper.ZooKeeperUtils;

/**
 * Migrate data from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 *
 */
class FromPostgresToSciDB extends FromDatabaseToDatabase
		implements MigrationImplementation {

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToSciDB.class);

	/**
	 * The object of the class is serializable.
	 */
	private static final long serialVersionUID = 1L;

	/* General message about the action in the class. */
	private static String generalMessage = "Data migration from PostgreSQL to SciDB";

	/* General error message when the migration fails in the class. */
	private static String errMessage = generalMessage + " failed! ";

	/** Meta-data about the table in PostgreSQL. */
	private PostgreSQLTableMetaData fromObjectMetaData;

	/* Resources that have to be cleaned at the end of the migration process. */
	private String postgresPipe = null;
	private String scidbPipe = null;
	private ExecutorService executor = null;

	/* constants or static variables */

	/**
	 * Always put extractor as the first task to be executed (while migrating
	 * data from PostgreSQL to SciDB).
	 */
	private static final int EXPORT_INDEX = 0;

	/**
	 * Always put transformation as the second task be executed (while migrating
	 * data from PostgreSQL to SciDB)
	 */
	private static final int TRANSFORMATION_INDEX = 1;

	/**
	 * Always put loader as the third task to be executed (while migrating data
	 * from PostgreSQL to SciDB)
	 */
	private static final int LOAD_INDEX = 2;

	public FromPostgresToSciDB() {
	}

	/**
	 * Provide the migration info for the new instance.
	 * 
	 * @param migrationInfo
	 *            information for this migration from PostgreSQL to SciDB
	 * @throws MigrationException
	 * 
	 */
	FromPostgresToSciDB(MigrationInfo migrationInfo) throws MigrationException {
		this.migrationInfo = migrationInfo;
		setPostgreSQLMetaData();
	}

	public FromPostgresToSciDB(PostgreSQLTableMetaData fromObjectMetaData,
			String postgresPipe, String scidbPipe, ExecutorService executor) {
		this.fromObjectMetaData = fromObjectMetaData;
		this.postgresPipe = postgresPipe;
		this.scidbPipe = scidbPipe;
		this.executor = executor;
	}

	/**
	 * Set the meta data for PostgreSQL.
	 * 
	 * @throws MigrationException
	 */
	private void setPostgreSQLMetaData() throws MigrationException {
		try {
			this.fromObjectMetaData = new PostgreSQLHandler(
					migrationInfo.getConnectionFrom())
							.getObjectMetaData(migrationInfo.getObjectFrom());
		} catch (SQLException postgresException) {
			MigrationException migrateException = handleException(
					postgresException,
					"Extraction of meta data from the table: "
							+ migrationInfo.getObjectFrom()
							+ " in PostgreSQL failed. ");
			throw migrateException;
		}
	}

	/**
	 * This is migration from PostgreSQL to SciDB.
	 * 
	 * @param connectionFrom the connection to PostgreSQL
	 * 
	 * @param fromTable the name of the table in PostgreSQL to be migrated
	 * 
	 * @param connectionTo the connection to SciDB database
	 * 
	 * @param arrayTo the name of the array in SciDB
	 * 
	 * @see
	 * istc.bigdawg.migration.FromDatabaseToDatabase#migrate(istc.bigdawg.query.
	 * ConnectionInfo, java.lang.String, istc.bigdawg.query.ConnectionInfo,
	 * java.lang.String
	 */
	@Override
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (migrationInfo
				.getConnectionFrom() instanceof PostgreSQLConnectionInfo
				&& migrationInfo
						.getConnectionTo() instanceof SciDBConnectionInfo) {
			this.migrationInfo = migrationInfo;
			setPostgreSQLMetaData();
			return this.dispatch();
		}
		return null;

	}

	@Override
	public MigrationResult executeMigrationLocally() throws MigrationException {
		return this.executeMigration();
	}

	/**
	 * Execute the migration.
	 * 
	 * @return MigrationResult
	 * @throws MigrationException
	 */
	public MigrationResult executeMigration() throws MigrationException {
		if (migrationInfo.getConnectionFrom() == null
				|| migrationInfo.getObjectFrom() == null
				|| migrationInfo.getConnectionTo() == null
				|| migrationInfo.getObjectTo() == null) {
			throw new MigrationException("The object was not initialized");
		}
		return migrate();
	}

	/**
	 * Binary migration.
	 * 
	 * @return {@link MigrationResult }
	 * @throws MigrationException
	 * @throws LocalQueryExecutionException
	 */
	public MigrationResult migrateBin() throws MigrationException {
		log.info(generalMessage + " Mode: binary migration.");
		long startTimeMigration = System.currentTimeMillis();
		SciDBArrays arrays = null;
		try {
			postgresPipe = Pipe.INSTANCE
					.createAndGetFullName(this.getClass().getName()
							+ "_fromPostgres_" + getObjectFrom());
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toSciDB_" + getObjectTo());

			executor = Executors.newFixedThreadPool(3/* 3 */);

			String copyFromCommand = PostgreSQLHandler
					.getExportBinCommand(getObjectFrom());
			// String copyFromCommand = "copy from " + fromTable + " to " +
			// postgresPipe + " with (format binary, freeze)";
			ExportPostgres exportExecutor = new ExportPostgres(
					getConnectionFrom(), copyFromCommand, postgresPipe,
					SciDBHandler.getInstance());
			FutureTask<Object> exportTask = new FutureTask<Object>(
					exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					postgresPipe, scidbPipe,
					MigrationUtils.getSciDBBinFormat(fromObjectMetaData),
					TransformBinExecutor.TYPE.FromPostgresToSciDB);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);

			LoadSciDB loadExecutor = new LoadSciDB(migrationInfo,
					new PostgreSQLHandler(migrationInfo.getConnectionFrom()),
					MigrationUtils.getSciDBBinFormat(fromObjectMetaData),
					scidbPipe);
			FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
			executor.submit(loadTask);

			long countExtractedElements = (Long) exportTask.get();
			long transformationResult = transformTask.get();
			String loadMessage = (String) loadTask.get();
			log.debug("load message: " + loadMessage);

			String transformationMessage;
			if (transformationResult != 0) {
				String message = "Check the C++ migrator! "
						+ "It might need to be compiled and checked separately!";
				log.error(message);
				throw new MigrationException(message);
			} else {
				transformationMessage = "Transformation finished successfuly!";
			}
			log.debug("transformation message: " + transformationMessage);

			MigrationUtils.removeIntermediateArrays(arrays, migrationInfo);

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, null, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (SQLException | UnsupportedTypeException | InterruptedException
				| ExecutionException | IOException
				| RunShellException exception) {
			MigrationException migrationException = handleException(exception,
					"Migration in binary format failed. ");
			throw migrationException;
		} finally {
			cleanResources();
		}
	}

	/**
	 * This is migration from PostgreSQL to SciDB based on CSV format and
	 * carried out in a single thread.
	 * 
	 * 
	 * 
	 * @return MigrationRestult information about the executed migration
	 * @throws SQLException
	 * @throws MigrationException
	 * @throws LocalQueryExecutionException
	 */
	public MigrationResult migrateSingleThreadCSV() throws MigrationException {
		log.info(generalMessage + " Mode: migrateSingleThreadCSV");
		long startTimeMigration = System.currentTimeMillis();
		String delimiter = FileFormat.getCsvDelimiter();
		try {
			postgresPipe = Pipe.INSTANCE
					.createAndGetFullName(this.getClass().getName()
							+ "_fromPostgres_" + getObjectFrom());
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toSciDB_" + getObjectTo());

			String typesPattern = SciDBHandler
					.getTypePatternFromPostgresTypes(fromObjectMetaData);

			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(new ExportPostgres(getConnectionFrom(),
					PostgreSQLHandler.getExportCsvCommand(getObjectFrom(),
							delimiter, FileFormat.getQuoteCharacter(),
							SciDBHandler.getIsCsvLoadHeader()),
					postgresPipe, SciDBHandler.getInstance()));
			tasks.add(new TransformFromCsvToSciDBExecutor(typesPattern,
					postgresPipe, delimiter, scidbPipe,
					BigDawgConfigProperties.INSTANCE.getScidbBinPath()));
			tasks.add(new LoadSciDB(migrationInfo, scidbPipe,
					new PostgreSQLHandler(migrationInfo.getConnectionFrom())));
			executor = Executors.newFixedThreadPool(tasks.size());
			List<Future<Object>> results = TaskExecutor.execute(executor,
					tasks);

			Long countExtractedElements = (Long) results.get(EXPORT_INDEX)
					.get();
			Long shellScriptReturnCode = (Long) results
					.get(TRANSFORMATION_INDEX).get();
			Long countLoadedElements = (Long) results.get(LOAD_INDEX).get();

			log.debug("Extracted elements from PostgreSQL: "
					+ countExtractedElements);
			if (shellScriptReturnCode != 0L) {
				throw new MigrationException(
						"Error while transforming data from CSV format to"
								+ " scidb dense format.");
			}
			if (countLoadedElements != null) {
				throw new MigrationException(
						"SciDB does not return any information about "
								+ "loading / "
								+ "export but there was an unkown data "
								+ "returned.");
			}

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, null, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (Exception exception) {
			throw handleException(exception, "Migration failed.");
		} finally {
			cleanResources();
		}

	}

	/**
	 * Clean resources of this instance of the migrator at the end of migration.
	 * 
	 * @throws MigrationException
	 */
	private void cleanResources() throws MigrationException {
		if (postgresPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(postgresPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: "
						+ postgresPipe + " " + e.getMessage());
			}
		}
		if (scidbPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(scidbPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: "
						+ scidbPipe + " " + e.getMessage());
			}
		}
		if (executor != null && !executor.isShutdown()) {
			executor.shutdownNow();
		}
	}

	/**
	 * Handler the exception for the migration.
	 * 
	 * @param exception
	 *            the exception that was raised during migration
	 * @return the MigrationException
	 */
	private MigrationException handleException(Exception exception,
			String message) {
		/* this log with stack trace is for UnsupportedTypeException */
		log.error(StackTrace.getFullStackTrace(exception));
		String msg = message + errMessage + exception.getMessage()
				+ " PostgreSQL connection: "
				+ getConnectionFrom().toSimpleString() + " fromTable: "
				+ getObjectFrom() + " SciDBConnection: "
				+ getConnectionTo().toSimpleString() + " to array:"
				+ getObjectTo();
		log.error(msg);
		return new MigrationException(msg);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.MigrationImplementation#migrate()
	 */
	@Override
	public MigrationResult migrate() throws MigrationException {
		/*
		 * The CSV migration is used for debugging and development, if you want
		 * to go faster then change it to migrateBin() but then the C++ migrator
		 * has to be compiled on each machine where BigDAWG is running.
		 */
		return migrateSingleThreadCSV();
	}

	/**
	 * Execute the migration request; the node with SciDB database should manage
	 * the migration (the main migration task runs on SciDB's node and the data
	 * is fetched from the node on which the PostgreSQL instance resides).
	 * 
	 */
	private Callable<Object> executeMigrationToLocal() {
		return () -> {
			/*
			 * execute the migration: export from local/remote machine, load to
			 * the local machine
			 */
			log.debug("Migration will be executed to local database.");
			try {
				return executeMigrationLocally();
			} catch (MigrationException e) {
				log.debug(e.getMessage());
				return e;
			}
		};
	}

	/**
	 * Dispatch the request for migration to the node with destination database.
	 * 
	 * Execute the request on remote SciDB node, so that the node with the SciDB
	 * database pulls the data from remote or local PostgreSQL.
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
		 * Check if the address of SciDB is not a local host.
		 */
		String hostnameFrom = this.getConnectionFrom().getHost();
		String hostnameTo = this.getConnectionTo().getHost();
		String debugMessage = "current hostname is: "
				+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress()
				+ "; hostname from which the data is migrated: " + hostnameFrom
				+ "; hostname to which the data is migrated: " + hostnameTo;
		log.debug(debugMessage);
		try {
			if (!isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
				log.debug("Migration will be executed remotely (this node: "
						+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress()
						+ " only coordinates the migration).");
				List<String> followIPs = new ArrayList<>();
				followIPs.add(hostnameTo);
				if (!isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
					followIPs.add(hostnameFrom);
				}
				/*
				 * This node is only a coordinator/dispatcher for the migration.
				 */
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameFrom),
						new RemoteRequest(this, hostnameTo));
			} else {
				/*
				 * This machine contains the destination database for the
				 * migration.
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
				List<String> followIPs = new ArrayList<>();
				if (!isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
					log.debug("Migration from: " + thisHostname
							+ " to local database: " + hostnameTo);
					followIPs.add(hostnameFrom);
				}
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameTo),
						executeMigrationToLocal());
				log.debug("Result of migration: " + result);
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

	/**
	 * This is only for fast tests.
	 * 
	 * @param args
	 * @throws IOException
	 * @throws MigrationException
	 */
	public static void main(String[] args)
			throws MigrationException, IOException {
		LoggerSetup.setLogging();
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "postgres", "test");
		// PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo(
		// "localhost", "5431", "test", "postgres", "test");
		String fromTable = "region";
		SciDBConnectionInfo conTo = new SciDBConnectionInfo("localhost", "1239",
				"scidb", "mypassw", "/opt/scidb/14.12/bin/");
		String toArray = "region";
		FromPostgresToSciDB migrator = new FromPostgresToSciDB();
		MigrationResult result = migrator.migrate(conFrom, fromTable, conTo,
				toArray);
		System.out.println("migration result: " + result);
	}

}
