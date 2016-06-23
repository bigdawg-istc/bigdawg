/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import istc.bigdawg.migration.datatypes.FromSciDBToSQLTypes;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;

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
	private enum MigrationType {
		FULL /* export dimensions and attributes from SciDB */, FLAT
		/* export only the attributes from SciDB */}

	private MigrationType migrationType;

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
	 * migration of data from PostgreSQL to SciDB. If something fails on the way
	 * or at the end of the migration process, the arrays should be removed.
	 * 
	 * On the other hand, the tables in PostgreSQL are created within a
	 * transaction so if something goes wrong in PostgreSQL, then the database
	 * itself takes care of cleaning the created but not loaded tables.
	 */
	private Set<String> intermediateArrays = new HashSet<>();

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
		setArrayMetaData();
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
			setArrayMetaData();
			try {
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
	 * Set the meta data for the SciDB array.
	 * 
	 * @throws MigrationException
	 */
	private void setArrayMetaData() throws MigrationException {
		String fromArray = getObjectFrom();
		try {
			SciDBHandler handler = new SciDBHandler(getConnectionFrom());
			this.scidbArrayMetaData = handler.getArrayMetaData(fromArray);
			handler.close();
		} catch (SQLException | NoTargetArrayException scidbException) {
			MigrationException migrateException = handleException(
					scidbException, "Extraction of meta data on the array: "
							+ fromArray + " in SciDB failed. ");
			throw migrateException;
		}
	}

	/**
	 * Get the copy to command to PostgreSQL.
	 * 
	 * example: copy region from '/tmp/adam_test.csv' with (format 'csv',
	 * delimiter ',', header true, quote "'");
	 *
	 * @return the copy command
	 */
	private String getCopyToPostgreSQLCsvCommand(String table) {
		StringBuilder copyTo = new StringBuilder("copy ");
		copyTo.append(table);
		copyTo.append(" from STDIN with ");
		copyTo.append("(format csv, delimiter ',', header true, quote \"'\")");
		String copyCommand = copyTo.toString();
		log.debug(LogUtils.replace(copyCommand));
		return copyCommand;
	}

	/**
	 * No dimensions;
	 * 
	 * @return
	 * @throws NoTargetArrayException
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 */
	private String getCreatePostgreSQLTableStatementFromSciDBAttributes()
			throws NoTargetArrayException, SQLException,
			UnsupportedTypeException {
		List<AttributeMetaData> attributes = scidbArrayMetaData
				.getAttributesOrdered();
		List<AttributeMetaData> columns = new ArrayList<>();
		columns.addAll(attributes);
		StringBuilder createTableStringBuf = new StringBuilder();
		String toTable = getObjectTo();
		createTableStringBuf
				.append("create table if not exists " + toTable + " (");
		for (AttributeMetaData column : columns) {
			String colName = column.getName();
			String scidbType = column.getDataType();
			String postgresType = FromSciDBToSQLTypes
					.getSQLTypeFromSciDBType(scidbType);
			createTableStringBuf.append(colName + " " + postgresType + ",");
		}
		createTableStringBuf.deleteCharAt(createTableStringBuf.length() - 1);
		createTableStringBuf.append(")");
		log.debug("create table command: " + createTableStringBuf.toString());
		return createTableStringBuf.toString();
	}

	/**
	 * Get a string representing the "create table" command in PostgreSQL. The
	 * create table command is created based on an existing array in SciDB from
	 * its dimensions and attributes.
	 * 
	 * @return the create table command
	 * 
	 * @throws SQLException
	 * @throws NoTargetArrayException
	 * @throws UnsupportedTypeException
	 *             the given type is not supported
	 * @throws MigrationException
	 */
	private String getCreatePostgreSQLTableStatementFromSciDBAttributesAndDimensions()
			throws NoTargetArrayException, SQLException,
			UnsupportedTypeException {
		List<AttributeMetaData> dimensions = scidbArrayMetaData
				.getDimensionsOrdered();
		List<AttributeMetaData> attributes = scidbArrayMetaData
				.getAttributesOrdered();
		List<AttributeMetaData> columns = new ArrayList<>();
		columns.addAll(dimensions);
		columns.addAll(attributes);
		StringBuilder createTableStringBuf = new StringBuilder();
		String toTable = getObjectTo();
		createTableStringBuf
				.append("create table if not exists " + toTable + " (");
		for (AttributeMetaData column : columns) {
			String colName = column.getName();
			String scidbType = column.getDataType();
			String postgresType = FromSciDBToSQLTypes
					.getSQLTypeFromSciDBType(scidbType);
			createTableStringBuf.append(colName + " " + postgresType + ",");
		}
		createTableStringBuf.deleteCharAt(createTableStringBuf.length() - 1);
		createTableStringBuf.append(")");
		log.debug("create table command: " + createTableStringBuf.toString());
		return createTableStringBuf.toString();
	}

	/**
	 * Create a new schema and table in the connectionTo if they not exist. The
	 * table definition has to be inferred from the array in SciDB.
	 * 
	 * @param connectionFrom
	 *            from which database we fetch the data
	 * @param fromTable
	 *            from which table we fetch the data
	 * @param connectionTo
	 *            to which database we connect to
	 * @param toTable
	 *            to which table we want to load the data
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 * @throws NoTargetArrayException
	 */
	private void createTargetTableSchema(Connection postgresCon,
			String createTableStatement) throws SQLException {
		String toTable = getObjectTo();
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
				toTable);
		PostgreSQLHandler.executeStatement(postgresCon,
				"create schema if not exists " + schemaTable.getSchemaName());
		PostgreSQLHandler.executeStatement(postgresCon, createTableStatement);
	}

	/**
	 * Example of the binary format for SciDB: (string, int64, int64 null)
	 * 
	 * @return the string representing a binary format for SciDB
	 * @throws NoTargetArrayException
	 * @throws SQLException
	 */
	private String getSciDBBinFormat(String array)
			throws NoTargetArrayException, SQLException {
		SciDBHandler handler = new SciDBHandler(getConnectionFrom());
		SciDBArrayMetaData arrayMetaData = handler.getArrayMetaData(array);
		handler.close();
		List<AttributeMetaData> attributes = arrayMetaData
				.getAttributesOrdered();
		StringBuilder binBuf = new StringBuilder();
		for (AttributeMetaData attribute : attributes) {
			binBuf.append(attribute.getDataType());
			if (attribute.isNullable()) {
				binBuf.append(" null");
			}
			binBuf.append(",");
		}
		// remove the last comma ,
		binBuf.deleteCharAt(binBuf.length() - 1);
		return binBuf.toString();
	}

	/**
	 * Decide the migration type (transfer (only the attributes) or (attributes
	 * and dimensions)).
	 * 
	 * @throws MigrationException
	 *             {@link MigrationException}
	 */
	private void establishMigrationType() throws MigrationException {
		PostgreSQLHandler postgresHandler = new PostgreSQLHandler(
				getConnectionTo());
		try {
			String toTable = getObjectTo();
			if (postgresHandler
					.existsTable(new PostgreSQLSchemaTableName(toTable))) {
				PostgreSQLTableMetaData tableMetaData = postgresHandler
						.getColumnsMetaData(toTable);
				// can we migrate only the attributes from the SciDB array
				List<AttributeMetaData> scidbColumnsOrdered = scidbArrayMetaData
						.getAttributesOrdered();
				List<AttributeMetaData> postgresColumnsOrdered = tableMetaData
						.getAttributesOrdered();
				if (postgresColumnsOrdered.size() == scidbColumnsOrdered.size()
						&& MigrationUtils.areAttributesTheSame(
								scidbArrayMetaData, tableMetaData)) {
					migrationType = MigrationType.FLAT;
				} /*
					 * check if the dimensions and the attributes in the array
					 * match the columns in the table
					 */
				else {
					/*
					 * verify the dimensions and attributes in the array with
					 * the columns in the table
					 */
					MigrationUtils.areDimensionsAndAttributesSameAsColumns(
							scidbArrayMetaData, tableMetaData);
					migrationType = MigrationType.FULL;
				}
			} else {
				migrationType = MigrationType.FULL;
			}
		} catch (SQLException ex) {
			String message = "Problem with data extraction from PostgreSQL. "
					+ ex.getMessage();
			throw new MigrationException(message);
		}
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
			establishMigrationType();
			SciDBArrays arrays = null;
			String format = null;
			setCreateTableStatementIfGiven();
			if (migrationType == MigrationType.FULL) {
				String newFlatIntermediateArray = fromArray
						+ "__bigdawg__flat__"
						+ SessionIdentifierGenerator.INSTANCE
								.nextRandom26CharString();
				createFlatArray(newFlatIntermediateArray);
				intermediateArrays.add(newFlatIntermediateArray);
				arrays = new SciDBArrays(newFlatIntermediateArray, fromArray);
				format = getSciDBBinFormat(newFlatIntermediateArray);
				if (createTableStatement == null) {
					createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributesAndDimensions();
				}
			} else {
				/*
				 * this is a flat array so we have to export only the attributes
				 */
				arrays = new SciDBArrays(fromArray, null);
				format = getSciDBBinFormat(fromArray);
				if (createTableStatement == null) {
					createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributes();
				}
			}

			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromSciDB_" + fromArray);
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toPostgres_" + toTable);

			executor = Executors.newFixedThreadPool(3);
			ExportSciDB exportExecutor = new ExportSciDB(getConnectionFrom(),
					arrays, scidbPipe, format, true);
			FutureTask<String> exportTask = new FutureTask<String>(
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
			createTargetTableSchema(connectionPostgres, createTableStatement);
			String copyToCommand = PostgreSQLHandler.getLoadBinCommand(toTable);
			LoadPostgres loadExecutor = new LoadPostgres(connectionPostgres,
					copyToCommand, postgresPipe);
			FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = exportTask.get();
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
		} catch (SQLException | UnsupportedTypeException | InterruptedException
				| ExecutionException | IOException | NoTargetArrayException
				| RunShellException exception) {
			MigrationException migrationException = handleException(exception,
					"Migration in binary format failed. ");
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

	/** Handle the internal exception in the migrator */
	private MigrationException handleException(Exception exception,
			String message) {
		/* this log with stack trace is for UnsupportedTypeException */
		log.error(StackTrace.getFullStackTrace(exception));
		String msg = message + exception.getMessage() + " " + errMessage
				+ " SciDB connection: " + getConnectionFrom().toSimpleString()
				+ " from array: " + getObjectFrom() + " PostgreSQL Connection: "
				+ getConnectionTo().toSimpleString() + " to table:"
				+ getObjectTo();
		log.error(msg);
		try {
			MigrationUtils.removeArrays(getConnectionFrom(), msg,
					intermediateArrays);
		} catch (MigrationException ex) {
			return ex;
		}
		return new MigrationException(msg);
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

			String fromArray = getObjectFrom();
			String toTable = getObjectTo();

			establishMigrationType();
			SciDBArrays arrays = null;

			setCreateTableStatementIfGiven();
			if (migrationType == MigrationType.FLAT) {
				arrays = new SciDBArrays(fromArray, null);
				if (createTableStatement == null) {
					createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributes();
				}
			} else { /* multidimensional array - MigrationType.FULL */
				arrays = new SciDBArrays(null, fromArray);
				if (createTableStatement == null) {
					createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributesAndDimensions();
				}
			}
			executor = Executors.newFixedThreadPool(2);

			/* we use only one pipe here */
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromSciDB_" + fromArray);

			String defultCsvFormat = "csv+";
			ExportSciDB exportExecutor = new ExportSciDB(getConnectionFrom(),
					arrays, scidbPipe, defultCsvFormat, false);
			FutureTask<String> exportTask = new FutureTask<String>(
					exportExecutor);
			executor.submit(exportTask);

			connectionPostgres = PostgreSQLHandler
					.getConnection(getConnectionTo());
			connectionPostgres.setAutoCommit(false);
			createTargetTableSchema(connectionPostgres, createTableStatement);
			LoadPostgres loadExecutor = new LoadPostgres(connectionPostgres,
					getCopyToPostgreSQLCsvCommand(toTable), scidbPipe);
			FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = exportTask.get();
			Long countLoadedElements = (Long) loadTask.get();

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
							+ " No information about the number of extracted items from SciDB.",
					false);
		} catch (SQLException | NoTargetArrayException
				| UnsupportedTypeException | InterruptedException
				| ExecutionException | RunShellException | IOException ex) {
			log.error(errMessage + " " + ex.getMessage()
					+ StackTrace.getFullStackTrace(ex));
			throw new MigrationException(errMessage + " " + ex.getMessage());
		} finally {
			cleanResources();
		}
	}

	/**
	 * Set the create table statement if it was passed as a parameter from a
	 * user.
	 */
	private void setCreateTableStatementIfGiven() {
		/*
		 * Get the create table statement from the parameters to the migration.
		 * (the create statement was passed directly by a user).
		 */
		migrationInfo.getMigrationParams()
				.ifPresent(migrationParams -> migrationParams
						.getCreateStatement().ifPresent(statement -> {
							createTableStatement = statement;
						}));
		if (createTableStatement != null) {
			log.debug("Create table statement: " + createTableStatement);
		}
	}

	/**
	 * Create a flat array in SciDB from the meta info about the
	 * multidimensional array.
	 * 
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 * @throws MigrationException
	 */
	private void createFlatArray(String flatArrayName)
			throws SQLException, UnsupportedTypeException, MigrationException {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + flatArrayName + " <");
		List<AttributeMetaData> scidbColumnsOrdered = new ArrayList<AttributeMetaData>();
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getDimensionsOrdered());
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getAttributesOrdered());
		for (AttributeMetaData column : scidbColumnsOrdered) {
			String attributeName = column.getName();
			String attributeType = column.getDataType();
			String attributeNULL = "";
			if (column.isNullable()) {
				attributeNULL = " NULL";
			}
			createArrayStringBuf.append(
					attributeName + ":" + attributeType + attributeNULL + ",");
		}

		/* delete the last comma "," */
		createArrayStringBuf.deleteCharAt(createArrayStringBuf.length() - 1);
		/* " r_regionkey:int64,r_name:string,r_comment:string> );" */
		/* this is by default 1 mln cells in a chunk */
		createArrayStringBuf
				.append("> [_flat_dimension_=0:*," + Long.MAX_VALUE + ",0]");
		SciDBHandler handler = new SciDBHandler(getConnectionFrom());
		handler.executeStatement(createArrayStringBuf.toString());
		handler.commit();
		handler.close();
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
