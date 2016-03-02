/**
 * 
 */
package istc.bigdawg.migration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import istc.bigdawg.migration.datatypes.DataTypesFromSciDBToPostgreSQL;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.SystemUtilities;

/**
 * Implementation of the migration from SciDB to PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 */
public class FromSciDBToPostgresImplementation {
	/* log */
	private static Logger log = Logger.getLogger(FromSciDBToPostgres.class);

	/* General message about the action in the class. */
	private String generalMessage = "Data migration from SciDB to PostgreSQL";

	/* General error message when the migration fails in the class. */
	private String errMessage = generalMessage + " failed! ";

	private SciDBConnectionInfo connectionFrom;
	private String fromArray;
	private PostgreSQLConnectionInfo connectionTo;
	private String toTable;
	private SciDBArrayMetaData scidbArrayMetaData;

	public FromSciDBToPostgresImplementation(SciDBConnectionInfo connectionFrom,
			String fromArray, PostgreSQLConnectionInfo connectionTo,
			String toTable) throws MigrationException {
		this.connectionFrom = connectionFrom;
		this.fromArray = fromArray;
		this.connectionTo = connectionTo;
		this.toTable = toTable;
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
		log.debug(copyCommand.replace("'", ""));
		return copyCommand;
	}

	/**
	 * Get a string representing the "create table" command in PostgreSQL. The
	 * create table command is created based on an existing array in SciDB.
	 * 
	 * @return the create table command
	 * 
	 * @throws SQLException
	 * @throws NoTargetArrayException
	 * @throws UnsupportedTypeException
	 *             the given type is not supported
	 */
	private String getCreatePostgreSQLTableStatementFromSciDBArray()
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
					.getSciDBTypeFromPostgreSQLType(scidbType);
			createTableStringBuf.append(colName + " " + postgresType + ",");
		}
		createTableStringBuf.deleteCharAt(createTableStringBuf.length() - 1);
		createTableStringBuf.append(")");
		log.debug("create table command: " + createTableStringBuf.toString());
		return createTableStringBuf.toString();
	}

	/**
	 * No dimensions;
	 * 
	 * @return
	 * @throws NoTargetArrayException
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 */
	private String getCreatePostgreSQLTableStatementFromSciDBArrayForBin()
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
					.getSciDBTypeFromPostgreSQLType(scidbType);
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
	 * Export data from SciDB to a csv file.
	 * 
	 * @param connectionFrom
	 *            connection to SciDB
	 * @param fromArray
	 *            the array from which we export the data
	 * @param csvFilePath
	 *            the path to the csv file where we export the data
	 * @throws SQLException
	 */
	private void exportDataFromSciDB(SciDBConnectionInfo connectionFrom,
			String fromArray, String csvFilePath) throws SQLException {
		SciDBHandler scidbHandler = new SciDBHandler(connectionFrom);
		/*
		 * Example: save(region,'/tmp/adam_test.csv',-2,'csv+'); -2: Save all
		 * data on the coordinator instance of the query. csv+ Comma-separated
		 * values with dimension indices (we need it for multidimensional
		 * arrays)
		 */
		scidbHandler.executeStatementAFL(
				"save(" + fromArray + ",'" + csvFilePath + "',-2,'csv+')");
		scidbHandler.commit();
		scidbHandler.close();
	}

	/**
	 * Load data to postgres from the given input.
	 * 
	 * @param conTo
	 *            connection to postgres
	 * @param toTable
	 *            table in postgres to load the data to
	 * @param input
	 *            the input data
	 * @return the number of loaded rows
	 * 
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private long loadDataToPostgres(Connection conTo, InputStream input)
			throws SQLException, FileNotFoundException, InterruptedException,
			ExecutionException {
		CopyToPostgresExecutor copyToExecutor = new CopyToPostgresExecutor(
				conTo, getCopyToPostgreSQLCsvCommand(toTable), input);
		FutureTask<Long> taskCopyToExecutor = new FutureTask<Long>(
				copyToExecutor);
		Thread copyToThread = new Thread(taskCopyToExecutor);

		copyToThread.start();
		copyToThread.join();
		long countLoadedElements = taskCopyToExecutor.get();
		return countLoadedElements;
	}

	private String getSciDBBinFormat()
			throws NoTargetArrayException, SQLException {
		SciDBHandler handler = new SciDBHandler(connectionFrom);
		SciDBArrayMetaData arrayMetaData = handler.getArrayMetaData(fromArray);
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
		log.info(System.getProperty("user.dir"));
		String scidbBinPath = System.getProperty("user.dir")
				+ "/src/main/resources/tmp/bigdawg_to_" + fromArray
				+ "_scidb.bin";
		String postgresBinPath = System.getProperty("user.dir")
				+ "/src/main/resources/tmp/bigdawg_from_" + toTable
				+ "_postgres.bin";

		ExecutorService executor = null;
		Connection postgresCon = null;
		try {
			// problem with rights to the mkfifo
			SystemUtilities.deleteFileIfExists(scidbBinPath);
			SystemUtilities.deleteFileIfExists(postgresBinPath);
			RunShell.mkfifo(scidbBinPath);
			RunShell.mkfifo(postgresBinPath);
			/* SciDB has to have right to write to the pipe */
			RunShell.runShell(new ProcessBuilder("chmod", "a+w", scidbBinPath));

			executor = Executors.newFixedThreadPool(3);

			ExportBinFromSciDBExecutor exportExecutor = new ExportBinFromSciDBExecutor(
					connectionFrom, fromArray, scidbBinPath,
					getSciDBBinFormat());
			FutureTask<String> exportTask = new FutureTask<String>(
					exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					scidbBinPath, postgresBinPath, getSciDBBinFormat(),
					TransformBinExecutor.TYPE.FromSciDBToPostgres);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);

			/* TODO you'll have to preapre the target array */
			postgresCon = PostgreSQLHandler.getConnection(connectionTo);
			postgresCon.setAutoCommit(false);
			String createTableStatement = getCreatePostgreSQLTableStatementFromSciDBArrayForBin();
			createTargetTableSchema(postgresCon, createTableStatement);
			String copyToCommand = PostgreSQLHandler.getLoadBinCommand(toTable);
			CopyToPostgresExecutor loadExecutor = new CopyToPostgresExecutor(
					postgresCon, copyToCommand, postgresBinPath);
			FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = exportTask.get();
			long loadedRowsCount = loadTask.get();
			long transformationResult = transformTask.get();
			String transformationMessage = transformationResult == 0 ? "correct"
					: "incorrect";

			return new MigrationResult(null, loadedRowsCount,
					exportMessage
							+ " No information about number of extracted rows."
							+ " Result of transformation: "
							+ transformationMessage,
					false);
		} catch (SQLException | UnsupportedTypeException | InterruptedException
				| ExecutionException | IOException | NoTargetArrayException
				| RunShellException exception) {
			MigrationException migrationException = handleException(exception,
					"Migration in binry format failed. ");
			throw migrationException;
		} finally {
			SystemUtilities.deleteFileIfExists(postgresBinPath);
			SystemUtilities.deleteFileIfExists(scidbBinPath);
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
			if (postgresCon != null) {
				try {
					postgresCon.close();
				} catch (SQLException e) {
					e.printStackTrace();
					String msg = "Could not close connection to PostgreSQL!"
							+ e.getMessage() + " " + generalMessage;
					log.error(msg);
					throw new MigrationException(msg);
				}
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
		return new MigrationException(msg);
	}

	/**
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws MigrationException
	 * 
	 */
	public MigrationResult migrateSingleThreadCSV() throws MigrationException {
		String csvFilePath = SystemUtilities.getSystemTempDir() + "/bigdawg_"
				+ fromArray + ".csv";
		Connection postgresCon = null;
		try {
			long startTimeMigration = System.currentTimeMillis();
			exportDataFromSciDB(connectionFrom, fromArray, csvFilePath);
			postgresCon = PostgreSQLHandler.getConnection(connectionTo);
			postgresCon.setAutoCommit(false);
			String createTableStatement = getCreatePostgreSQLTableStatementFromSciDBArray();
			createTargetTableSchema(postgresCon, createTableStatement);
			InputStream input = new FileInputStream(csvFilePath);
			long countLoadedRows = loadDataToPostgres(postgresCon, input);
			postgresCon.commit();
			postgresCon.close();
			long endTimeMigration = System.currentTimeMillis();
			String message = "migration from SciDB to PostgreSQL execution time: "
					+ (endTimeMigration - startTimeMigration);
			log.debug(message);
			return new MigrationResult(0L, countLoadedRows);
		} catch (SQLException | NoTargetArrayException
				| UnsupportedTypeException | FileNotFoundException
				| InterruptedException | ExecutionException ex) {
			log.error(errMessage + " " + ex.getMessage()
					+ StackTrace.getFullStackTrace(ex));
			throw new MigrationException(errMessage + " " + ex.getMessage());
		} finally {
			if (postgresCon != null) {
				try {
					postgresCon.close();
				} catch (SQLException e) {
					e.printStackTrace();
					throw new MigrationException(e.getMessage());
				}
			}
		}
		/*
		 * the file was created by SciDB and we cannot remove it (another user)
		 */
		// SystemUtilities.deleteFileIfExists(csvFilePath);
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
				"localhost", "5431", "test", "postgres", "test");
		String toTable = "waveform";

		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(
				conFrom, fromArray, conTo, toTable);
		migrator.migrateBin();
	}

}
