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

	public FromPostgresToPostgres() {
		super();
	}

	public FromPostgresToPostgres(PostgreSQLConnectionInfo connectionFrom,
			String fromTable, PostgreSQLConnectionInfo connectionTo,
			String toTable) {
		super();
		this.connectionFrom = connectionFrom;
		this.fromTable = fromTable;
		this.connectionTo = connectionTo;
		this.toTable = toTable;
	}

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
				logger.error(StackTrace.getFullStackTrace(e));
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * Create a new schema and table in the connectionTo if they do not exist.
	 * 
	 * Get the table definition from the connectionFrom.
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
		/* separate schema name from the table name */
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
				toTable);
		/* create the target schema if it is not already there */
		PostgreSQLHandler.executeStatement(connectionTo,
				"create schema if not exists " + schemaTable.getSchemaName());
		/*
		 * get the create table statement for the source table from the source
		 * database
		 */
		String createTableStatement = PostgreSQLHandler
				.getCreateTable(connectionFrom, fromTable);
		PostgreSQLHandler.executeStatement(connectionTo, createTableStatement);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#execute()
	 */
	@Override
	public MigrationResult executeMigration() throws MigrationException {
		TimeStamp startTimeStamp = TimeStamp.getCurrentTime();
		logger.debug("start migration: " + startTimeStamp.toDateString());

		long startTimeMigration = System.currentTimeMillis();
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

			ExportPostgres copyFromExecutor = new ExportPostgres(conFrom,
					copyFromCommand, output);
			FutureTask<Long> taskCopyFromExecutor = new FutureTask<Long>(
					copyFromExecutor);

			LoadPostgres copyToExecutor = new LoadPostgres(conTo, copyToCommand,
					input);
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
			String message = e.getMessage()
					+ " Migration failed. Task did not finish correctly. ";
			logger.error(message + " Stack Trace: "
					+ StackTrace.getFullStackTrace(e), e);
			if (conTo != null) {
				try {
					conTo.rollback();
				} catch (SQLException ex) {
					String messageRollbackConTo = " Could not roll back "
							+ "transactions in the source database after "
							+ "failure in data migration: " + ex.getMessage();
					logger.error(messageRollbackConTo);
					message += messageRollbackConTo;
				}
			}
			if (conFrom != null) {
				try {

					conFrom.rollback();
				} catch (SQLException ex) {
					String messageRollbackConFrom = " Could not roll back "
							+ "transactions in the destination database "
							+ "after failure in data migration: "
							+ ex.getMessage();
					logger.error(messageRollbackConFrom);
					message += messageRollbackConFrom;
				}
			}
			throw new MigrationException(message, e);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#getConnectionFrom()
	 */
	@Override
	public ConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/*
	 * (non-Javadoc)
	 * 
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
		FromDatabaseToDatabase migrator = new FromPostgresToPostgres();
		ConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost",
				"5431", "mimic2", "pguser", "test");
		ConnectionInfo conInfoTo = new PostgreSQLConnectionInfo("localhost",
				"5430", "mimic2", "pguser", "test");
		MigrationResult result;
		try {
			result = migrator.migrate(conInfoFrom, "mimic2v26.d_patients",
					conInfoTo, "patients2");
		} catch (MigrationException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw e;
		}
		logger.debug("Number of extracted rows: "
				+ result.getCountExtractedElements()
				+ " Number of loaded rows: " + result.getCountLoadedElements());
	}

}
