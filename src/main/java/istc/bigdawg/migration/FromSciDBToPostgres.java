/**
 * 
 */
package istc.bigdawg.migration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.datatypes.DataTypesFromSciDBToPostgreSQL;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.SystemUtilities;

/**
 * Migrate data from SciDB to PostgreSQL.
 * 
 * @author Adam Dziedzic
 */
public class FromSciDBToPostgres implements FromDatabaseToDatabase {

	/* log */
	private static Logger log = Logger.getLogger(FromSciDBToPostgres.class);

	/* General message about the action in the class. */
	private String generalMessage = "Data migration from SciDB to PostgreSQL";

	/* General error message when the migration fails in the class. */
	private String errMessage = generalMessage + " failed! ";

	/**
	 * Get the copy to command to PostgreSQL.
	 * 
	 * example: copy region from '/tmp/adam_test.csv' with (format 'csv',
	 * delimiter ',', header true, quote "'");
	 *
	 * @return the copy command
	 */
	private String getCopyToPostgreSQLCommand(String table) {
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
	private String getCreatePostgreSQLTableStatementFromSciDBArray(
			SciDBConnectionInfo connectionFrom, String fromArray,
			String toTable) throws NoTargetArrayException, SQLException,
					UnsupportedTypeException {
		SciDBHandler handler = new SciDBHandler(connectionFrom);
		SciDBArrayMetaData arrayMetaData = handler.getArrayMetaData(fromArray);
		handler.close();
		List<SciDBColumnMetaData> dimensions = arrayMetaData
				.getDimensionsOrdered();
		List<SciDBColumnMetaData> attributes = arrayMetaData
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
	private void createTargetTableSchema(SciDBConnectionInfo connectionFrom,
			String fromArray, Connection postgresCon, String toTable)
					throws SQLException, NoTargetArrayException,
					UnsupportedTypeException {
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
				toTable);
		PostgreSQLHandler.executeStatement(postgresCon,
				"create schema if not exists " + schemaTable.getSchemaName());
		String createTableStatement = getCreatePostgreSQLTableStatementFromSciDBArray(
				connectionFrom, fromArray, toTable);
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
	private long loadDataToPostgres(Connection conTo, String toTable,
			InputStream input) throws SQLException, FileNotFoundException,
					InterruptedException, ExecutionException {
		CopyManager cpTo = new CopyManager((BaseConnection) conTo);
		CopyToPostgresExecutor copyToExecutor = new CopyToPostgresExecutor(cpTo,
				getCopyToPostgreSQLCommand(toTable), input);
		FutureTask<Long> taskCopyToExecutor = new FutureTask<Long>(
				copyToExecutor);
		Thread copyToThread = new Thread(taskCopyToExecutor);

		copyToThread.start();
		copyToThread.join();
		long countLoadedElements = taskCopyToExecutor.get();
		return countLoadedElements;
	}

	/**
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws MigrationException
	 * 
	 */
	public MigrationResult migrateSingleThreadCSV(
			SciDBConnectionInfo connectionFrom, String fromArray,
			PostgreSQLConnectionInfo connectionTo, String toTable)
					throws MigrationException {
		String csvFilePath = SystemUtilities.getSystemTempDir() + "/bigdawg_"
				+ fromArray + ".csv";
		try {
			long startTimeMigration = System.currentTimeMillis();
			exportDataFromSciDB(connectionFrom, fromArray, csvFilePath);
			Connection postgresCon = PostgreSQLHandler
					.getConnection(connectionTo);
			postgresCon.setAutoCommit(false);
			createTargetTableSchema(connectionFrom, fromArray, postgresCon,
					toTable);
			InputStream input = new FileInputStream(csvFilePath);
			long countLoadedRows = loadDataToPostgres(postgresCon, toTable,
					input);
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
			/*
			 * the file was created by SciDB and we cannot remove it (another
			 * user)
			 */
			// SystemUtilities.deleteFileIfExists(csvFilePath);
		}
	}

	@Override
	/**
	 * Migrate data from SciDB to PostgreSQL.
	 */
	public MigrationResult migrate(ConnectionInfo connectionFrom,
			String objectFrom, ConnectionInfo connectionTo, String objectTo)
					throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof SciDBConnectionInfo
				&& connectionTo instanceof PostgreSQLConnectionInfo) {
			try {
				return this.migrate((SciDBConnectionInfo) connectionFrom,
						objectFrom, (PostgreSQLConnectionInfo) connectionTo,
						objectTo);
			} catch (Exception e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
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
		String arrayFrom = "region";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "tpch", "postgres", "test");
		String tableTo = "region";
		migrator.migrate(conFrom, arrayFrom, conTo, tableTo);
	}

}
