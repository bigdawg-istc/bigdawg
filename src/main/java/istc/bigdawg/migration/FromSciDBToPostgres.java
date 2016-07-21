/**
 * 
 */
package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.network.RemoteRequest;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;
import istc.bigdawg.zookeeper.FollowRemoteNodes;
import istc.bigdawg.zookeeper.ZooKeeperUtils;

/**
 * Migrate data from SciDB to PostgreSQL.
 * 
 * @author Adam Dziedzic
 */
public class FromSciDBToPostgres extends FromDatabaseToDatabase
		implements MigrationImplementation {

	/* log */
	private static Logger log = Logger.getLogger(FromSciDBToPostgres.class);

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;

	/** Migration either from a flat or multi-dimensional array. */
	public enum MigrationType {
		FULL /* export dimensions and attributes from SciDB */, FLAT
		/* export only the attributes from SciDB */}

	/** General message about the action in the class. */
	private String generalMessage = "Data migration from SciDB to PostgreSQL";

	/** General error message when the migration fails in the class. */
	private String errMessage = generalMessage + " failed! ";

	/** Meta-data about array in SciDB, e.g. attributes, types, etc. */
	private SciDBArrayMetaData scidbArrayMetaData;

	/* Resources that have to be cleaned at the end of the migration process. */
	private String postgresPipe = null;
	private String scidbPipe = null;
	private ExecutorService executor = null;
	private Connection connectionPostgres = null;

	/** The SQL statement to create a target table in PostgreSQL. */
	private String createTableStatement;

	/*
	 * These are the intermediate (additional) arrays that were created during
	 * migration of data between PostgreSQL and SciDB. If something fails on the
	 * way or at the end of the migration process, the arrays should be removed.
	 * 
	 * On the other hand, the tables in PostgreSQL are created within a
	 * transaction so if something goes wrong in PostgreSQL, then the database
	 * itself takes care of cleaning the created but not loaded tables.
	 */
	private Set<String> intermediateArrays = new HashSet<>();

	/**
	 * Always put extractor as the first task to be executed.
	 */
	private static final int EXPORT_INDEX = 0;

	/**
	 * Always put loader as the second task to be executed.
	 */
	private static final int LOAD_INDEX = 1;

	/**
	 * This constructor is required if we provide other constructors with
	 * parameters.
	 */
	FromSciDBToPostgres() {

	}

	/**
	 * Provide the migration info for the new instance.
	 * 
	 * @param migrationInfo
	 *            information for this migration from PostgreSQL to SciDB
	 * @throws MigrationException
	 * 
	 */
	FromSciDBToPostgres(MigrationInfo migrationInfo) throws MigrationException {
		this.migrationInfo = migrationInfo;
		try {
			this.scidbArrayMetaData = SciDBHandler.getArrayMetaData(
					migrationInfo.getConnectionFrom(),
					migrationInfo.getObjectFrom());
		} catch (Exception e) {
			throw new MigrationException(e.getMessage(), e);
		}
	}

	@Override
	/**
	 * Migrate data from SciDB to PostgreSQL.
	 */
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (migrationInfo.getConnectionFrom() instanceof SciDBConnectionInfo
				&& migrationInfo
						.getConnectionTo() instanceof PostgreSQLConnectionInfo) {
			this.migrationInfo = migrationInfo;
			try {
				this.scidbArrayMetaData = SciDBHandler.getArrayMetaData(
						migrationInfo.getConnectionFrom(),
						migrationInfo.getObjectFrom());
				return this.dispatch();
			} catch (Exception e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	@Override
	public MigrationResult executeMigrationLocally() throws MigrationException {
		return this.executeMigration();
	}

	public MigrationResult executeMigration() throws MigrationException {
		if (getConnectionFrom() == null || getObjectFrom() == null
				|| getConnectionTo() == null || getObjectTo() == null) {
			throw new MigrationException("The object was not initialized");
		}
		return migrate();
	}

	/**
	 * This works only for a flat array.
	 * 
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws MigrationException
	 *             exception with info what went wrong during data migration
	 */
	public MigrationResult migrateBin() throws MigrationException {
		generalMessage += "Mode: binary migration.";
		log.info(generalMessage);

		long startTimeMigration = System.currentTimeMillis();
		String fromArray = getObjectFrom();
		String toTable = getObjectTo();
		try {
			MigrationType migrationType = MigrationUtils.getMigrationType(
					migrationInfo,
					new PostgreSQLHandler(migrationInfo.getConnectionTo()));
			SciDBArrays arrays = null;
			String format = null;
			createTableStatement = MigrationUtils
					.getUserCreateStatement(migrationInfo);
			List<AttributeMetaData> attributes = scidbArrayMetaData
					.getAttributesOrdered();
			if (migrationType == MigrationType.FLAT) {
				/*
				 * this is a flat array so we have to export only the attributes
				 */
				arrays = new SciDBArrays(
						new SciDBArray(fromArray, false, false), null);
				format = ExportSciDB.getSciDBBinFormat(migrationInfo,
						fromArray);
				if (createTableStatement == null) {
					createTableStatement = PostgreSQLHandler
							.getCreatePostgreSQLTableStatement(toTable,
									attributes);
				}
			} else {
				String newFlatIntermediateArray = fromArray
						+ "__bigdawg__flat__"
						+ SessionIdentifierGenerator.INSTANCE
								.nextRandom26CharString();
				try {
					SciDBHandler.createFlatArrayFromMultiDimArray(
							migrationInfo.getConnectionFrom(),
							migrationInfo.getObjectFrom(),
							newFlatIntermediateArray);
				} catch (Exception e) {
					throw new MigrationException(e.getMessage(), e);
				}
				intermediateArrays.add(newFlatIntermediateArray);
				arrays = new SciDBArrays(
						new SciDBArray(newFlatIntermediateArray, true, true),
						new SciDBArray(fromArray, false, false));
				format = ExportSciDB.getSciDBBinFormat(migrationInfo,
						newFlatIntermediateArray);
				List<AttributeMetaData> dimensionsAttributes = scidbArrayMetaData
						.getDimensionsOrdered();
				dimensionsAttributes.addAll(attributes);
				if (createTableStatement == null) {
					createTableStatement = PostgreSQLHandler
							.getCreatePostgreSQLTableStatement(toTable,
									dimensionsAttributes);
				}
			}

			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromSciDB_" + fromArray);
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toPostgres_" + toTable);

			executor = Executors.newFixedThreadPool(3);
			ExportSciDB exportExecutor = new ExportSciDB(migrationInfo, arrays,
					scidbPipe, FileFormat.BIN_SCIDB, format);
			FutureTask<Object> exportTask = new FutureTask<Object>(
					exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					scidbPipe, postgresPipe, format,
					TransformBinExecutor.TYPE.FromSciDBToPostgres);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);

			connectionPostgres = PostgreSQLHandler
					.getConnection(getConnectionTo());
			connectionPostgres.setAutoCommit(false);
			PostgreSQLHandler.createTargetTableSchema(connectionPostgres,
					migrationInfo.getObjectTo(), createTableStatement);
			/* The statement was used. */
			createTableStatement = null;
			String copyToCommand = PostgreSQLHandler.getLoadBinCommand(toTable);
			LoadPostgres loadExecutor = new LoadPostgres(connectionPostgres,
					copyToCommand, postgresPipe);
			FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = (String) exportTask.get();
			long transformationResult = transformTask.get();
			long countLoadedElements = (long) loadTask.get();

			String transformationMessage = transformationResult == 0 ? "correct"
					: "incorrect";
			MigrationUtils.removeArrays(getConnectionFrom(),
					"clean the intermediate arrays", intermediateArrays);
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationStatistics stats = new MigrationStatistics(
					getConnectionFrom(), getConnectionTo(), fromArray, toTable,
					startTimeMigration, endTimeMigration, null,
					countLoadedElements, this.getClass().getName());
			Monitor.addMigrationStats(stats);
			log.debug("Migration result,connectionFrom,"
					+ getConnectionFrom().toSimpleString() + ",connectionTo,"
					+ getConnectionTo().toSimpleString() + ",fromArray,"
					+ fromArray + ",toTable," + toTable + ",startTimeMigration,"
					+ startTimeMigration + ",endTimeMigration,"
					+ endTimeMigration + ",countExtractedElements," + "N/A"
					+ ",countLoadedElements," + countLoadedElements
					+ ",durationMsec," + durationMsec + ","
					+ Thread.currentThread().getStackTrace()[1].getMethodName()
					+ "," + migrationType.toString());

			return new MigrationResult(null, countLoadedElements,
					exportMessage
							+ " No information about the number of extracted rows."
							+ " Result of transformation: "
							+ transformationMessage,
					false);
		} catch (SQLException | InterruptedException | ExecutionException
				| IOException | NoTargetArrayException
				| RunShellException exception) {
			MigrationException migrationException = handleException(
					migrationInfo, exception,
					"Migration in binary format failed. ", null);
			throw migrationException;
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
		if (connectionPostgres != null) {
			try {
				connectionPostgres.close();
			} catch (SQLException e) {
				e.printStackTrace();
				String msg = "Could not close connection to PostgreSQL!"
						+ e.getMessage() + " " + generalMessage;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
	}

	/** Handle the internal exception in the migrator. */
	private static MigrationException handleException(
			MigrationInfo migrationInfo, Exception exception, String message,
			Set<String> intermediateArrays) {
		/* this log with stack trace is for UnsupportedTypeException */
		log.error(StackTrace.getFullStackTrace(exception));
		String msg = message + exception.getMessage() + " " + message
				+ " SciDB connection: "
				+ migrationInfo.getConnectionFrom().toSimpleString()
				+ " from array: " + migrationInfo.getObjectFrom()
				+ " PostgreSQL Connection: "
				+ migrationInfo.getConnectionTo().toSimpleString()
				+ " to table:" + migrationInfo.getObjectTo();
		log.error(msg);
		try {
			/** SciDB was the source database. */
			MigrationUtils.removeArrays(migrationInfo.getConnectionFrom(), msg,
					intermediateArrays);
		} catch (MigrationException ex) {
			return ex;
		}
		return new MigrationException(msg, exception);
	}

	/**
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws MigrationException
	 * 
	 */
	public MigrationResult migrateSingleThreadCSV() throws MigrationException {
		try {
			long startTimeMigration = System.currentTimeMillis();

			String fromArray = migrationInfo.getObjectFrom();
			String toTable = migrationInfo.getObjectTo();

			DBHandler handlerTo = new PostgreSQLHandler(
					migrationInfo.getConnectionTo());
			MigrationType migrationType = MigrationUtils
					.getMigrationType(migrationInfo, handlerTo);
			SciDBArrays arrays = null;

			String createStatement = MigrationUtils
					.getUserCreateStatement(migrationInfo);
			List<AttributeMetaData> attributes = scidbArrayMetaData
					.getAttributesOrdered();
			if (migrationType == MigrationType.FLAT) {
				arrays = new SciDBArrays(
						new SciDBArray(fromArray, false, false), null);
				if (createStatement == null) {
					createStatement = PostgreSQLHandler
							.getCreatePostgreSQLTableStatement(toTable,
									attributes);
				}
			} else { /* multidimensional array - MigrationType.FULL */
				arrays = new SciDBArrays(null,
						new SciDBArray(fromArray, false, false));
				List<AttributeMetaData> dimensionsAttributes = scidbArrayMetaData
						.getDimensionsOrdered();
				dimensionsAttributes.addAll(attributes);
				if (createStatement == null) {
					createStatement = PostgreSQLHandler
							.getCreatePostgreSQLTableStatement(toTable,
									dimensionsAttributes);
				}
			}
			executor = Executors.newFixedThreadPool(2);

			/* we use only one pipe here */
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromSciDB_" + fromArray);

			connectionPostgres = PostgreSQLHandler
					.getConnection(getConnectionTo());
			connectionPostgres.setAutoCommit(false);
			PostgreSQLHandler.createTargetTableSchema(connectionPostgres,
					toTable, createStatement);

			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(new ExportSciDB(migrationInfo, arrays, scidbPipe,
					FileFormat.CSV, null));
			tasks.add(new LoadPostgres(connectionPostgres,
					PostgreSQLHandler.getLoadCsvCommand(
							migrationInfo.getObjectTo(),
							SciDBHandler.getGeneralCsvExportDelimiter(),
							FileFormat.getQuoteCharacter(),
							SciDBHandler.getIsCsvExportHeader()),
					scidbPipe));
			executor = Executors.newFixedThreadPool(tasks.size());
			List<Future<Object>> results = TaskExecutor.execute(executor,
					tasks);

			Long countExtractedElements = (Long) results.get(EXPORT_INDEX)
					.get();
			Long countLoadedElements = (Long) results.get(LOAD_INDEX).get();

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			log.debug("migration duration time msec: " + durationMsec);
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, countLoadedElements, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (Exception ex) {
			log.error(errMessage + " " + ex.getMessage()
					+ StackTrace.getFullStackTrace(ex));
			throw new MigrationException(errMessage + " " + ex.getMessage());
		} finally {
			cleanResources();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.MigrationImplementation#migrate()
	 */
	@Override
	public MigrationResult migrate() throws MigrationException {
		return migrateSingleThreadCSV();
	}

	/**
	 * Execute the migration request; the node with SciDB database should manage
	 * the migration (the main migration task runs on SciDB's node and the data
	 * is pushed to the node on which the PostgreSQL instance resides).
	 * 
	 */
	private Callable<Object> executeMigrationFromLocal() {
		return () -> {
			/*
			 * execute the migration: export from local/remote machine, load to
			 * the local machine
			 */
			log.debug(
					"Migration will be executed from local database with SciDB.");
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
	 * Execute the request on the SciDB node, so that the node with the SciDB
	 * database pushes the data to a remote or local PostgreSQL instance.
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
			if (!isThisMyIpAddress(InetAddress.getByName(hostnameFrom))) {
				log.debug("Migration will be executed remotely (this node: "
						+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress()
						+ " only coordinates the migration).");
				List<String> followIPs = new ArrayList<>();
				followIPs.add(hostnameFrom);
				if (!isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
					followIPs.add(hostnameTo);
				}
				/*
				 * This node is only a coordinator/dispatcher for the migration.
				 */
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameFrom),
						new RemoteRequest(this, hostnameFrom));
				log.debug("Result of migration: " + result);
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
				List<String> followIPs = new ArrayList<>();
				if (!isThisMyIpAddress(InetAddress.getByName(hostnameTo))) {
					log.debug("Migration from: " + thisHostname
							+ " to local database: " + hostnameTo);
					followIPs.add(hostnameTo);
				}
				result = FollowRemoteNodes.execute(Arrays.asList(hostnameTo),
						executeMigrationFromLocal());
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
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		FromSciDBToPostgres migrator = new FromSciDBToPostgres();
		SciDBConnectionInfo conFrom = new SciDBConnectionInfo("localhost",
				"1239", "scidb", "mypassw", "/opt/scidb/14.12/bin/");
		String arrayFrom = "region2";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "postgres", "test");
		String tableTo = "region";
		MigrationResult result = migrator.migrate(conFrom, arrayFrom, conTo,
				tableTo);
		System.out.println(result);
	}

}
