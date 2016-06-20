/**
 * 
 */
package istc.bigdawg.migration.direct;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.migration.ExportPostgres;
import istc.bigdawg.migration.FileFormat;
import istc.bigdawg.migration.FromDatabaseToDatabase;
import istc.bigdawg.migration.LoadSciDB;
import istc.bigdawg.migration.MigrationImplementation;
import istc.bigdawg.migration.MigrationInfo;
import istc.bigdawg.migration.MigrationResult;
import istc.bigdawg.migration.PostgreSQLSciDBMigrationUtils;
import istc.bigdawg.migration.SciDBArrays;
import istc.bigdawg.migration.TransformBinExecutor;
import istc.bigdawg.migration.TransformFromCsvToSciDBExecutor;
import istc.bigdawg.migration.datatypes.FromSQLTypesToSciDB;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;

/**
 * Migrate data from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 *
 */
public class FromPostgresToSciDB extends FromDatabaseToDatabase
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

	/**
	 * Stores the create statement array which was passed directly by a user.
	 */
	private String createArrayStatement;

	/*
	 * These are the arrays that were created during migration of data from
	 * PostgreSQL to SciDB. If something fails on the way, then the arrays
	 * should be removed.
	 */
	private Set<String> createdArrays = new HashSet<>();

	/**
	 * These are intermediate arrays (flat) created for migration and should be
	 * always removed at the end of the migration process.
	 */
	private Set<String> intermediateArrays = new HashSet<>();

	/** Meta-data about the table in PostgreSQL. */
	private PostgreSQLTableMetaData postgresqlTableMetaData;

	/* Resources that have to be cleaned at the end of the migration process. */
	private String postgresPipe = null;
	private String scidbPipe = null;
	private ExecutorService executor = null;

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
	public FromPostgresToSciDB(MigrationInfo migrationInfo)
			throws MigrationException {
		this.migrationInfo = migrationInfo;
		setPostgreSQLMetaData();
	}

	/**
	 * Set the meta data for PostgreSQL.
	 * 
	 * @throws MigrationException
	 */
	private void setPostgreSQLMetaData() throws MigrationException {
		try {
			this.postgresqlTableMetaData = new PostgreSQLHandler(
					migrationInfo.getConnectionFrom())
							.getColumnsMetaData(migrationInfo.getObjectFrom());
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

		try {
			postgresPipe = Pipe.INSTANCE
					.createAndGetFullName(this.getClass().getName()
							+ "_fromPostgres_" + getObjectFrom());
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toSciDB_" + getObjectTo());

			SciDBArrays arrays = prepareFlatTargetArrays();
			executor = Executors.newFixedThreadPool(3/* 3 */);

			String copyFromCommand = PostgreSQLHandler
					.getExportBinCommand(getObjectFrom());
			// String copyFromCommand = "copy from " + fromTable + " to " +
			// postgresPipe + " with (format binary, freeze)";
			ExportPostgres exportExecutor = new ExportPostgres(
					getConnectionFrom(), copyFromCommand, postgresPipe);
			FutureTask<Object> exportTask = new FutureTask<Object>(
					exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					postgresPipe, scidbPipe,
					PostgreSQLSciDBMigrationUtils
							.getSciDBBinFormat(postgresqlTableMetaData),
					TransformBinExecutor.TYPE.FromPostgresToSciDB);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);

			LoadSciDB loadExecutor = new LoadSciDB(getConnectionTo(), arrays,
					scidbPipe, PostgreSQLSciDBMigrationUtils
							.getSciDBBinFormat(postgresqlTableMetaData));
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

			/**
			 * the migration was successful so only clear the intermediate
			 * arrays
			 */
			PostgreSQLSciDBMigrationUtils.removeArrays(getConnectionTo(),
					"clean the intermediate arrays", intermediateArrays);
			createdArrays.removeAll(intermediateArrays);

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, null, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (SQLException | UnsupportedTypeException | InterruptedException
				| ExecutionException | IOException | RunShellException
				| LocalQueryExecutionException exception) {
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
			executor = Executors.newFixedThreadPool(3);

			ExportPostgres exportExecutor = new ExportPostgres(
					getConnectionFrom(), PostgreSQLHandler.getExportCsvCommand(
							getObjectFrom(), delimiter),
					postgresPipe);
			FutureTask<Object> exportTask = new FutureTask<Object>(
					exportExecutor);
			executor.submit(exportTask);

			String typesPattern = SciDBHandler
					.getTypePatternFromPostgresTypes(postgresqlTableMetaData);
			TransformFromCsvToSciDBExecutor csvSciDBExecutor = new TransformFromCsvToSciDBExecutor(
					typesPattern, postgresPipe, delimiter, scidbPipe,
					BigDawgConfigProperties.INSTANCE.getScidbBinPath());
			FutureTask<Integer> csvSciDBTask = new FutureTask<Integer>(
					csvSciDBExecutor);
			executor.submit(csvSciDBTask);

			SciDBArrays arrays = prepareFlatTargetArrays();
			LoadSciDB loadExecutor = new LoadSciDB(getConnectionTo(), arrays,
					scidbPipe);
			FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
			executor.submit(loadTask);

			long countExtractedElements = (long) exportTask.get();
			csvSciDBTask.get();
			String loadMessage = (String) loadTask.get();
			log.debug("load message: " + loadMessage);

			/**
			 * the migration was successful so only clear the intermediate
			 * arrays
			 */
			PostgreSQLSciDBMigrationUtils.removeArrays(getConnectionTo(),
					"clean the intermediate arrays", intermediateArrays);
			createdArrays.removeAll(intermediateArrays);

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, null, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (SQLException | UnsupportedTypeException | ExecutionException
				| InterruptedException | MigrationException | IOException
				| RunShellException | LocalQueryExecutionException exception) {
			MigrationException migrationException = handleException(exception,
					"Migration in CSV format failed. ");
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
		try {
			/**
			 * there was an exception so the migration failed and the created
			 * arrays should be removed
			 */
			PostgreSQLSciDBMigrationUtils.removeArrays(getConnectionTo(), msg,
					createdArrays);
		} catch (MigrationException ex) {
			return ex;
		}
		return new MigrationException(msg);
	}

	/**
	 * Create a flat array in SciDB from the meta info about the table in
	 * PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 * @throws MigrationException
	 */
	private void createFlatArray(String arrayName)
			throws SQLException, UnsupportedTypeException, MigrationException {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + arrayName + " <");
		List<AttributeMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getAttributesOrdered();
		for (AttributeMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String attributeName = postgresColumnMetaData.getName();
			String postgresColumnType = postgresColumnMetaData.getDataType();
			String attributeType = FromSQLTypesToSciDB
					.getSciDBTypeFromSQLType(postgresColumnType);
			String attributeNULL = "";
			if (postgresColumnMetaData.isNullable()) {
				attributeNULL = " NULL";
			}
			createArrayStringBuf.append(
					attributeName + ":" + attributeType + attributeNULL + ",");
		}
		/* delete the last comma "," */
		createArrayStringBuf.deleteCharAt(createArrayStringBuf.length() - 1);
		/* " r_regionkey:int64,r_name:string,r_comment:string> );" */
		/* this is by default 1 mln cells in a chunk */
		createArrayStringBuf.append("> [_flat_dimension_=0:*,1000000,0]");
		SciDBHandler handler = new SciDBHandler(
				migrationInfo.getConnectionTo());
		handler.executeStatement(createArrayStringBuf.toString());
		handler.commit();
		handler.close();
		createdArrays.add(arrayName);
	}

	/**
	 * Get the create table statement from the parameters to the migration (the
	 * create statement was passed directly by a user).
	 * 
	 * @throws SQLException
	 * @throws LocalQueryExecutionException
	 */
	private void createArrayFromUserStatement()
			throws SQLException, LocalQueryExecutionException {
		String toArray = getObjectTo();
		migrationInfo.getMigrationParams()
				.ifPresent(migrationParams -> migrationParams
						.getCreateStatement().ifPresent(statement -> {
							createArrayStatement = statement;
						}));
		if (createArrayStatement != null) {
			log.debug("create the array from the statement provided by a user: "
					+ createArrayStatement);
			if (!createArrayStatement.contains(toArray)) {
				throw new IllegalArgumentException(
						"The object to which we have "
								+ "to load the data has a different name "
								+ "than the object specified in the create statement.");
			}
			SciDBHandler localHandler = new SciDBHandler(getConnectionTo());
			localHandler.execute(createArrayStatement);
			localHandler.commit();
			localHandler.close();
			createdArrays.add(toArray);
		}
	}

	/**
	 * Prepare flat and target arrays in SciDB to load the data.
	 * 
	 * @throws SQLException
	 * @throws MigrationException
	 * @throws UnsupportedTypeException
	 * @throws LocalQueryExecutionException
	 * 
	 */
	private SciDBArrays prepareFlatTargetArrays()
			throws MigrationException, SQLException, UnsupportedTypeException,
			LocalQueryExecutionException {
		SciDBHandler handler = new SciDBHandler(getConnectionTo());
		String toArray = getObjectTo();
		SciDBArrayMetaData arrayMetaData = null;
		createArrayFromUserStatement();
		try {
			arrayMetaData = handler.getArrayMetaData(toArray);
		} catch (NoTargetArrayException e) {
			/*
			 * When only a name of array in SciDB was given, but the array does
			 * not exist in SciDB then we have to create the target array which
			 * by default is flat.
			 */
			createFlatArray(getObjectTo());
			/* the data should be loaded to the default flat array */
			return new SciDBArrays(toArray, null);
		}
		handler.close();
		if (PostgreSQLSciDBMigrationUtils.isFlatArray(arrayMetaData,
				postgresqlTableMetaData)) {
			return new SciDBArrays(toArray, null);
		}
		/*
		 * the target array is multidimensional so we have to build the
		 * intermediate flat array
		 */
		/*
		 * check if every column from Postgres is mapped to a column/attribute
		 * in SciDB's arrays (the attributes from the flat array can change to
		 * dimensions in the multi-dimensional array, thus we cannot verify the
		 * match of columns in PostgreSQL and dimensions/attributes in SciDB)
		 */
		Map<String, SciDBColumnMetaData> dimensionsMap = arrayMetaData
				.getDimensionsMap();
		Map<String, SciDBColumnMetaData> attributesMap = arrayMetaData
				.getAttributesMap();
		List<AttributeMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getAttributesOrdered();
		for (AttributeMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String postgresColumnName = postgresColumnMetaData.getName();
			if (!dimensionsMap.containsKey(postgresColumnName)
					&& !attributesMap.containsKey(postgresColumnName)) {
				throw new MigrationException(
						"The attribute " + postgresColumnName
								+ " from PostgreSQL's table: " + getObjectFrom()
								+ " is not matched with any attribute/dimension in the array in SciDB: "
								+ getObjectTo());
			}
		}
		String newFlatIntermediateArrayName = getObjectTo()
				+ "__bigdawg__flat__"
				+ SessionIdentifierGenerator.INSTANCE.nextRandom26CharString();
		createFlatArray(newFlatIntermediateArrayName);
		intermediateArrays.add(newFlatIntermediateArrayName);
		return new SciDBArrays(newFlatIntermediateArrayName, getObjectTo());
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
		 * has to be compiled on each machine where bigdawg is running.
		 */
		return migrateSingleThreadCSV();
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
