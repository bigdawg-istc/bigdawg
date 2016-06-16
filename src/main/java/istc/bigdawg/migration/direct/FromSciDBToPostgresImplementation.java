/**
 * 
 */
package istc.bigdawg.migration.direct;

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
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.ExportSciDB;
import istc.bigdawg.migration.LoadPostgres;
import istc.bigdawg.migration.MigrationImplementation;
import istc.bigdawg.migration.MigrationResult;
import istc.bigdawg.migration.MigrationStatistics;
import istc.bigdawg.migration.PostgreSQLSciDBMigrationUtils;
import istc.bigdawg.migration.SciDBArrays;
import istc.bigdawg.migration.TransformBinExecutor;
import istc.bigdawg.migration.datatypes.DataTypesFromSciDBToPostgreSQL;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;

/**
 * Implementation of the migration from SciDB to PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 */
public class FromSciDBToPostgresImplementation
		implements MigrationImplementation {
	/* log */
	private static Logger log = Logger
			.getLogger(FromSciDBToPostgresImplementation.class);

	enum MigrationType {
		FULL /* export dimensions and attributes from SciDB */, FLAT
		/* export only the attributes from SciDB */}

	private MigrationType migrationType;

	/* General message about the action in the class. */
	private String generalMessage = "Data migration from SciDB to PostgreSQL";

	/* General error message when the migration fails in the class. */
	private String errMessage = generalMessage + " failed! ";

	private SciDBConnectionInfo connectionFrom;
	private String fromArray;
	private PostgreSQLConnectionInfo connectionTo;
	private String toTable;
	private SciDBArrayMetaData scidbArrayMetaData;

	/* Resources that have to be cleaned at the end of the migration process. */
	private String postgresPipe = null;
	private String scidbPipe = null;
	private ExecutorService executor = null;
	private Connection connectionPostgres = null;

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

	public FromSciDBToPostgresImplementation(SciDBConnectionInfo connectionFrom,
			String fromArray, PostgreSQLConnectionInfo connectionTo,
			String toTable) throws MigrationException {
		this.connectionFrom = connectionFrom;
		this.fromArray = fromArray;
		this.connectionTo = connectionTo;
		this.toTable = toTable;
		/* by default export all the data from SciDB */
		this.migrationType = MigrationType.FULL;
		try {
			SciDBHandler handler = new SciDBHandler(connectionFrom);
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
		List<SciDBColumnMetaData> attributes = scidbArrayMetaData
				.getAttributesOrdered();
		List<SciDBColumnMetaData> columns = new ArrayList<>();
		columns.addAll(attributes);
		StringBuilder createTableStringBuf = new StringBuilder();
		createTableStringBuf
				.append("create table if not exists " + toTable + " (");
		for (SciDBColumnMetaData column : columns) {
			String colName = column.getColumnName();
			String scidbType = column.getColumnType();
			String postgresType = DataTypesFromSciDBToPostgreSQL
					.getPostgreSQLTypeFromSciDBType(scidbType);
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
		List<SciDBColumnMetaData> dimensions = scidbArrayMetaData
				.getDimensionsOrdered();
		List<SciDBColumnMetaData> attributes = scidbArrayMetaData
				.getAttributesOrdered();
		List<SciDBColumnMetaData> columns = new ArrayList<>();
		columns.addAll(dimensions);
		columns.addAll(attributes);
		StringBuilder createTableStringBuf = new StringBuilder();
		createTableStringBuf
				.append("create table if not exists " + toTable + " (");
		for (SciDBColumnMetaData column : columns) {
			String colName = column.getColumnName();
			String scidbType = column.getColumnType();
			String postgresType = DataTypesFromSciDBToPostgreSQL
					.getPostgreSQLTypeFromSciDBType(scidbType);
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
		SciDBHandler handler = new SciDBHandler(connectionFrom);
		SciDBArrayMetaData arrayMetaData = handler.getArrayMetaData(array);
		handler.close();
		List<SciDBColumnMetaData> attributes = arrayMetaData
				.getAttributesOrdered();
		StringBuilder binBuf = new StringBuilder();
		for (SciDBColumnMetaData attribute : attributes) {
			binBuf.append(attribute.getColumnType());
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
		PostgreSQLHandler postgresHandler = new PostgreSQLHandler(connectionTo);
		try {
			if (postgresHandler
					.existsTable(new PostgreSQLSchemaTableName(toTable))) {
				PostgreSQLTableMetaData tableMetaData = postgresHandler
						.getColumnsMetaData(toTable);
				// can we migrate only the attributes from the SciDB array
				List<SciDBColumnMetaData> scidbColumnsOrdered = scidbArrayMetaData
						.getAttributesOrdered();
				List<PostgreSQLColumnMetaData> postgresColumnsOrdered = tableMetaData
						.getColumnsOrdered();
				if (postgresColumnsOrdered.size() == scidbColumnsOrdered.size()
						&& PostgreSQLSciDBMigrationUtils
								.areAttributesSameAsColumns(scidbArrayMetaData,
										tableMetaData)) {
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
					PostgreSQLSciDBMigrationUtils
							.areDimensionsAndAttributesSameAsColumns(
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
		try {
			establishMigrationType();
			SciDBArrays arrays = null;
			String format = null;
			String createTableStatement = null;
			if (migrationType == MigrationType.FULL) {
				String newFlatIntermediateArray = fromArray
						+ "__bigdawg__flat__"
						+ SessionIdentifierGenerator.INSTANCE
								.nextRandom26CharString();
				createFlatArray(newFlatIntermediateArray);
				intermediateArrays.add(newFlatIntermediateArray);
				arrays = new SciDBArrays(newFlatIntermediateArray, fromArray);
				format = getSciDBBinFormat(newFlatIntermediateArray);
				createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributesAndDimensions();
			} else {
				/*
				 * this is a flat array so we have to export only the attributes
				 */
				arrays = new SciDBArrays(fromArray, null);
				format = getSciDBBinFormat(fromArray);
				createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributes();
			}

			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromSciDB_" + fromArray);
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toPostgres_" + toTable);

			executor = Executors.newFixedThreadPool(3);
			ExportSciDB exportExecutor = new ExportSciDB(connectionFrom, arrays,
					scidbPipe, format, true);
			FutureTask<String> exportTask = new FutureTask<String>(
					exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					scidbPipe, postgresPipe, format,
					TransformBinExecutor.TYPE.FromSciDBToPostgres);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);

			connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
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
			PostgreSQLSciDBMigrationUtils.removeArrays(connectionFrom,
					"clean the intermediate arrays", intermediateArrays);
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationStatistics stats = new MigrationStatistics(connectionFrom,
					connectionTo, fromArray, toTable, startTimeMigration,
					endTimeMigration, null, countLoadedElements,
					this.getClass().getName());
			Monitor.addMigrationStats(stats);
			log.debug("Migration result,connectionFrom,"
					+ connectionFrom.toString() + ",connectionTo,"
					+ connectionTo.toSimpleString() + ",fromArray," + fromArray
					+ ",toTable," + toTable + ",startTimeMigration,"
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
				+ " SciDB connection: " + connectionFrom.toString()
				+ " from array: " + fromArray + " PostgreSQL Connection: "
				+ connectionTo.toString() + " to table:" + toTable;
		log.error(msg);
		try {
			PostgreSQLSciDBMigrationUtils.removeArrays(connectionFrom, msg,
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

			establishMigrationType();
			SciDBArrays arrays = null;
			String createTableStatement = null;
			if (migrationType == MigrationType.FLAT) {
				arrays = new SciDBArrays(fromArray, null);
				createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributes();
			} else { /* multidimensional array - MigrationType.FULL */
				arrays = new SciDBArrays(null, fromArray);
				createTableStatement = getCreatePostgreSQLTableStatementFromSciDBAttributesAndDimensions();
			}
			executor = Executors.newFixedThreadPool(2);

			/* we use only one pipe here */
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromSciDB_" + fromArray);

			String defultCsvFormat = "csv+";
			ExportSciDB exportExecutor = new ExportSciDB(connectionFrom, arrays,
					scidbPipe, defultCsvFormat, false);
			FutureTask<String> exportTask = new FutureTask<String>(
					exportExecutor);
			executor.submit(exportTask);

			connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
			connectionPostgres.setAutoCommit(false);
			createTargetTableSchema(connectionPostgres, createTableStatement);
			LoadPostgres loadExecutor = new LoadPostgres(connectionPostgres,
					getCopyToPostgreSQLCsvCommand(toTable), scidbPipe);
			FutureTask<Object> loadTask = new FutureTask<Object>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = exportTask.get();
			Long countLoadedElements = (long) loadTask.get();

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationStatistics stats = new MigrationStatistics(connectionFrom,
					connectionTo, fromArray, toTable, startTimeMigration,
					endTimeMigration, null, countLoadedElements,
					this.getClass().getName());
			Monitor.addMigrationStats(stats);
			log.debug("Migration result,connectionFrom,"
					+ connectionFrom.toString() + ",connectionTo,"
					+ connectionTo.toSimpleString() + ",fromArray," + fromArray
					+ ",toTable," + toTable + ",startTimeMigration,"
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
		List<SciDBColumnMetaData> scidbColumnsOrdered = new ArrayList<SciDBColumnMetaData>();
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getDimensionsOrdered());
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getAttributesOrdered());
		for (SciDBColumnMetaData column : scidbColumnsOrdered) {
			String attributeName = column.getColumnName();
			String attributeType = column.getColumnType();
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
		SciDBHandler handler = new SciDBHandler(connectionFrom);
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
	 * @throws IOException
	 * @throws MigrationException
	 */
	public static void main(String[] args)
			throws MigrationException, IOException {
		LoggerSetup.setLogging();
		SciDBConnectionInfo conFrom = new SciDBConnectionInfo("localhost",
				"1239", "scidb", "mypassw", "/opt/scidb/14.12/bin/");
		String fromArray = "waveform";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "pguser", "test");
		String toTable = "waveformFromSciDB";

		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(
				conFrom, fromArray, conTo, toTable);
		migrator.migrateBin();
	}

}
