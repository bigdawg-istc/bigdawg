/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
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
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;

/**
 * Implementation of the migration from SciDB to PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 */
public class FromSciDBToPostgresImplementation implements MigrationImplementation {
	/* log */
	private static Logger log = Logger.getLogger(FromSciDBToPostgresImplementation.class);

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

	public FromSciDBToPostgresImplementation(SciDBConnectionInfo connectionFrom, String fromArray,
			PostgreSQLConnectionInfo connectionTo, String toTable) throws MigrationException {
		this.connectionFrom = connectionFrom;
		this.fromArray = fromArray;
		this.connectionTo = connectionTo;
		this.toTable = toTable;
		try {
			SciDBHandler handler = new SciDBHandler(connectionFrom);
			this.scidbArrayMetaData = handler.getArrayMetaData(fromArray);
			handler.close();
		} catch (SQLException | NoTargetArrayException scidbException) {
			MigrationException migrateException = handleException(scidbException,
					"Extraction of meta data on the array: " + fromArray + " in SciDB failed. ");
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
	private String getCreatePostgreSQLTableStatementFromSciDBArrayForCsv()
			throws NoTargetArrayException, SQLException, UnsupportedTypeException {
		List<SciDBColumnMetaData> dimensions = scidbArrayMetaData.getDimensionsOrdered();
		List<SciDBColumnMetaData> attributes = scidbArrayMetaData.getAttributesOrdered();
		List<SciDBColumnMetaData> columns = new ArrayList<>();
		columns.addAll(dimensions);
		columns.addAll(attributes);
		StringBuilder createTableStringBuf = new StringBuilder();
		createTableStringBuf.append("create table if not exists " + toTable + " (");
		for (SciDBColumnMetaData column : columns) {
			String colName = column.getColumnName();
			String scidbType = column.getColumnType();
			String postgresType = DataTypesFromSciDBToPostgreSQL.getSciDBTypeFromPostgreSQLType(scidbType);
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
			throws NoTargetArrayException, SQLException, UnsupportedTypeException {
		List<SciDBColumnMetaData> attributes = scidbArrayMetaData.getAttributesOrdered();
		List<SciDBColumnMetaData> columns = new ArrayList<>();
		columns.addAll(attributes);
		StringBuilder createTableStringBuf = new StringBuilder();
		createTableStringBuf.append("create table if not exists " + toTable + " (");
		for (SciDBColumnMetaData column : columns) {
			String colName = column.getColumnName();
			String scidbType = column.getColumnType();
			String postgresType = DataTypesFromSciDBToPostgreSQL.getSciDBTypeFromPostgreSQLType(scidbType);
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
	private void createTargetTableSchema(Connection postgresCon, String createTableStatement) throws SQLException {
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(toTable);
		PostgreSQLHandler.executeStatement(postgresCon, "create schema if not exists " + schemaTable.getSchemaName());
		PostgreSQLHandler.executeStatement(postgresCon, createTableStatement);
	}

	/**
	 * Example of the binary format for SciDB: (string, int64, int64 null)
	 * 
	 * @return the string representing a binary format for SciDB
	 * @throws NoTargetArrayException
	 * @throws SQLException
	 */
	private String getSciDBBinFormat() throws NoTargetArrayException, SQLException {
		SciDBHandler handler = new SciDBHandler(connectionFrom);
		SciDBArrayMetaData arrayMetaData = handler.getArrayMetaData(fromArray);
		handler.close();
		List<SciDBColumnMetaData> attributes = arrayMetaData.getAttributesOrdered();
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
		try {
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_fromSciDB_" + fromArray);
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_toPostgres_" + toTable);

			executor = Executors.newFixedThreadPool(3);

			ExportFromSciDBExecutor exportExecutor = new ExportFromSciDBExecutor(connectionFrom, fromArray, scidbPipe,
					getSciDBBinFormat(), true);
			FutureTask<String> exportTask = new FutureTask<String>(exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(scidbPipe, postgresPipe,
					getSciDBBinFormat(), TransformBinExecutor.TYPE.FromSciDBToPostgres);
			FutureTask<Long> transformTask = new FutureTask<Long>(transformExecutor);
			executor.submit(transformTask);

			/* TODO you'll have to prepare the target array */
			connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
			connectionPostgres.setAutoCommit(false);
			String createTableStatement = getCreatePostgreSQLTableStatementFromSciDBArrayForBin();
			createTargetTableSchema(connectionPostgres, createTableStatement);
			String copyToCommand = PostgreSQLHandler.getLoadBinCommand(toTable);
			CopyToPostgresExecutor loadExecutor = new CopyToPostgresExecutor(connectionPostgres, copyToCommand,
					postgresPipe);
			FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = exportTask.get();
			long transformationResult = transformTask.get();
			long loadedRowsCount = loadTask.get();

			String transformationMessage = transformationResult == 0 ? "correct" : "incorrect";

			return new MigrationResult(null, loadedRowsCount,
					exportMessage + " No information about number of extracted rows." + " Result of transformation: "
							+ transformationMessage,
					false);
		} catch (SQLException | UnsupportedTypeException | InterruptedException | ExecutionException | IOException
				| NoTargetArrayException | RunShellException exception) {
			MigrationException migrationException = handleException(exception, "Migration in binry format failed. ");
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
				throw new MigrationException("Could not remove pipe: " + postgresPipe + " " + e.getMessage());
			}
		}
		if (scidbPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(scidbPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: " + scidbPipe + " " + e.getMessage());
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
				String msg = "Could not close connection to PostgreSQL!" + e.getMessage() + " " + generalMessage;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
	}

	/** Handle the internal exception in the migrator */
	private MigrationException handleException(Exception exception, String message) {
		/* this log with stack trace is for UnsupportedTypeException */
		log.error(StackTrace.getFullStackTrace(exception));
		String msg = message + exception.getMessage() + " " + errMessage + " SciDB connection: "
				+ connectionFrom.toString() + " from array: " + fromArray + " PostgreSQL Connection: "
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
		try {
			long startTimeMigration = System.currentTimeMillis();
			executor = Executors.newFixedThreadPool(2);

			/* we use only one pipe here */
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_fromSciDB_" + fromArray);

			String defultCsvFormat = "csv+";
			ExportFromSciDBExecutor exportExecutor = new ExportFromSciDBExecutor(connectionFrom, fromArray, scidbPipe,
					defultCsvFormat, false);
			FutureTask<String> exportTask = new FutureTask<String>(exportExecutor);
			executor.submit(exportTask);

			/* TODO you'll have to prepare the source array */
			connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
			connectionPostgres.setAutoCommit(false);
			String createTableStatement = getCreatePostgreSQLTableStatementFromSciDBArrayForCsv();
			createTargetTableSchema(connectionPostgres, createTableStatement);
			CopyToPostgresExecutor loadExecutor = new CopyToPostgresExecutor(connectionPostgres,
					getCopyToPostgreSQLCsvCommand(toTable), scidbPipe);
			FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
			executor.submit(loadTask);

			String exportMessage = exportTask.get();
			long loadedRowsCount = loadTask.get();

			long endTimeMigration = System.currentTimeMillis();
			String message = "migration from SciDB to PostgreSQL execution time: "
					+ (endTimeMigration - startTimeMigration);
			log.info(message);
			return new MigrationResult(null, loadedRowsCount,
					exportMessage + " No information about number of extracted rows.", false);
		} catch (SQLException | NoTargetArrayException | UnsupportedTypeException | InterruptedException
				| ExecutionException | RunShellException | IOException ex) {
			log.error(errMessage + " " + ex.getMessage() + StackTrace.getFullStackTrace(ex));
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
	 * @param args
	 * @throws IOException
	 * @throws MigrationException
	 */
	public static void main(String[] args) throws MigrationException, IOException {
		LoggerSetup.setLogging();
		SciDBConnectionInfo conFrom = new SciDBConnectionInfo("localhost", "1239", "scidb", "mypassw",
				"/opt/scidb/14.12/bin/");
		String fromArray = "waveform";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo("localhost", "5431", "test", "pguser", "test");
		String toTable = "waveform";

		FromSciDBToPostgresImplementation migrator = new FromSciDBToPostgresImplementation(conFrom, fromArray, conTo,
				toTable);
		migrator.migrateBin();
	}

}
