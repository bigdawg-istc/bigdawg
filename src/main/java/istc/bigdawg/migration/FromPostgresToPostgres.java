/**
 * 
 */
package istc.bigdawg.migration;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.StackTrace;

/**
 * Data migration between instances of PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToPostgres extends FromDatabaseToDatabase {

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToPostgres.class);

	private static final int numberOfThreads = 2;

	private PostgreSQLConnectionInfo connectionFrom;
	private String fromTable;
	private PostgreSQLConnectionInfo connectionTo;
	private String toTable;

	/**
	 * Migrate data between instances of PostgreSQL.
	 */
	public MigrationResult migrate(ConnectionInfo connectionFrom,
			String fromTable, ConnectionInfo connectionTo, String toTable)
					throws MigrationException {
		logger.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof PostgreSQLConnectionInfo
				&& connectionTo instanceof PostgreSQLConnectionInfo) {
			this.connectionFrom = (PostgreSQLConnectionInfo) connectionFrom;
			this.fromTable = fromTable;
			this.connectionTo = (PostgreSQLConnectionInfo) connectionTo;
			this.toTable = toTable;
			try {
				return this.dispatch();
			} catch (Exception e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * Create a new schema and table in the connectionTo if they not exist. Get
	 * the table definition from connectionFrom.
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
	 */
	private void createTargetTableSchema(Connection connectionFrom,
			String fromTable, Connection connectionTo, String toTable)
					throws SQLException {
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
				toTable);
		PostgreSQLHandler.executeStatement(connectionTo,
				"create schema if not exists " + schemaTable.getSchemaName());
		String createTableStatement = PostgreSQLHandler
				.getCreateTable(connectionFrom, fromTable);
		PostgreSQLHandler.executeStatement(connectionTo, createTableStatement);
	}

	/**
	 * @see istc.bigdawg.migration.MigrationNetworkRequest#execute()
	 * 
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * 
	 */
	@Override
	public MigrationResult execute() throws MigrationException {
		long startTimeMigration = System.currentTimeMillis();
		TimeStamp startTimeStamp = TimeStamp.getCurrentTime();
		logger.debug("start migration: " + startTimeStamp.toDateString());
		String copyFromCommand = PostgreSQLHandler
				.getExportBinCommand(fromTable);
		String copyToCommand = PostgreSQLHandler.getLoadBinCommand(toTable);

		Connection conFrom = null;
		Connection conTo = null;
		ExecutorService executor = null;
		try {
			conFrom = PostgreSQLHandler.getConnection(connectionFrom);
			conTo = PostgreSQLHandler.getConnection(connectionTo);

			conFrom.setReadOnly(true);
			conFrom.setAutoCommit(false);
			conTo.setAutoCommit(false);
			createTargetTableSchema(conFrom, fromTable, conTo, toTable);

			final PipedOutputStream output = new PipedOutputStream();
			final PipedInputStream input = new PipedInputStream(output);

			CopyFromPostgresExecutor copyFromExecutor = new CopyFromPostgresExecutor(
					conFrom, copyFromCommand, output);
			FutureTask<Long> taskCopyFromExecutor = new FutureTask<Long>(
					copyFromExecutor);

			CopyToPostgresExecutor copyToExecutor = new CopyToPostgresExecutor(
					conTo, copyToCommand, input);
			FutureTask<Long> taskCopyToExecutor = new FutureTask<Long>(
					copyToExecutor);

			executor = Executors.newFixedThreadPool(numberOfThreads);
			executor.submit(taskCopyFromExecutor);
			executor.submit(taskCopyToExecutor);
			long countExtractedElements = taskCopyFromExecutor.get();
			long countLoadedElements = taskCopyToExecutor.get();

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			logger.debug("migration duration time msec: " + durationMsec);
			MigrationStatistics stats = new MigrationStatistics(connectionFrom,
					connectionTo, fromTable, toTable, startTimeMigration,
					endTimeMigration, countExtractedElements,
					countLoadedElements, this.getClass().getName());
			Monitor.addMigrationStats(stats);
			logger.debug("Migration result,connectionFrom,"
					+ connectionFrom.toSimpleString() + ",connectionTo,"
					+ connectionTo.toSimpleString() + ",fromTable," + fromTable
					+ ",toTable," + toTable + ",startTimeMigration,"
					+ startTimeMigration + ",endTi	meMigration,"
					+ endTimeMigration + ",countExtractedElements,"
					+ countExtractedElements + ",countLoadedElements,"
					+ countLoadedElements + ",durationMsec," + durationMsec);
			/**
			 * log table query: copy (select time,message from logs where
			 * message like 'Migration result,%' order by time desc) to
			 * '/tmp/migration_log.csv' with (format csv);
			 */
			return new MigrationResult(countExtractedElements,
					countLoadedElements);
		} catch (Exception e) {
			e.printStackTrace();
			String msg = e.getMessage()
					+ " Migration failed. Task did not finish correctly.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
			try {
				conTo.rollback();
			} catch (SQLException ex) {
				String message = "Could not roll back transactions in the source database after failure in data migration: "
						+ ex.getMessage();
				logger.error(message);
				throw new MigrationException(message, ex);
			}
			try {
				conFrom.rollback();
			} catch (SQLException ex) {
				String message = "Could not roll back transactions in the destination database after failure in data migration: "
						+ ex.getMessage();
				logger.error(message);
				throw new MigrationException(message, ex);
			}
			throw new MigrationException(e.getMessage(), e);
		} finally {
			if (conFrom != null) {
				/*
				 * calling closed on an already closed connection has no effect
				 */
				try {
					conFrom.close();
				} catch (SQLException e) {
					String msg = "Could not close the source database connection.";
					logger.error(msg + StackTrace.getFullStackTrace(e), e);
				}
				conFrom = null;
			}
			if (conTo != null) {
				try {
					conTo.close();
				} catch (SQLException e) {
					String msg = "Could not close the destination database connection.";
					logger.error(msg + StackTrace.getFullStackTrace(e), e);
				}
				conTo = null;
			}
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#getConnectionFrom()
	 */
	@Override
	public ConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/* (non-Javadoc)
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#getConnecitonTo()
	 */
	@Override
	public ConnectionInfo getConnecitonTo() {
		return connectionTo;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		FromPostgresToPostgres migrator = new FromPostgresToPostgres();
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "mimic2", "pguser", "test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				"localhost", "5430", "mimic2_copy", "pguser", "test");
		MigrationResult result;
		try {
			result = migrator.migrate(conInfoFrom, "mimic2v26.d_patients",
					conInfoTo, "mimic2v26.d_patients");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		logger.debug("Number of extracted rows: "
				+ result.getCountExtractedElements()
				+ " Number of loaded rows: " + result.getCountLoadedElements());

		ConnectionInfo conFrom = conInfoFrom;
		ConnectionInfo conTo = conInfoTo;
		MigrationResult result1;
		try {
			result1 = migrator.migrate(conFrom, "mimic2v26.d_patients", conTo,
					"mimic2v26.d_patients");
			logger.debug("Number of extracted rows: "
					+ result1.getCountExtractedElements()
					+ " Number of loaded rows: "
					+ result1.getCountLoadedElements());
		} catch (MigrationException e) {
			String msg = "Problem with general data migration.";
			logger.error(msg);
			e.printStackTrace();
		}

	}

}
