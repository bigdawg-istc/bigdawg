/**
 * 
 */
package istc.bigdawg.migration;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;

/**
 * Data migration between instances of PostgreSQL.
 * 
 * 
 * log table query: copy (select time,message from logs where message like
 * 'Migration result,%' order by time desc) to '/tmp/migration_log.csv' with
 * (format csv);
 * 
 * 
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToPostgres extends FromDatabaseToDatabase {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToPostgres.class);

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Always put extractor as the first task to be executed (while migrating
	 * data from PostgreSQL to PostgreSQL).
	 */
	private static final int EXPORT_INDEX = 0;

	/**
	 * Always put loader as the second task to be executed (while migrating data
	 * from PostgreSQL to PostgreSQL)
	 */
	private static final int LOAD_INDEX = 1;

	public FromPostgresToPostgres(PostgreSQLConnectionInfo connectionFrom,
			String fromTable, PostgreSQLConnectionInfo connectionTo,
			String toTable) {
		this.migrationInfo = new MigrationInfo(connectionFrom, fromTable,
				connectionTo, toTable, null);
	}

	/**
	 * Create default instance of the class.
	 */
	public FromPostgresToPostgres() {
		super();
	}

	/**
	 * Migrate data between instances of PostgreSQL.
	 */
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		logger.debug("General data migration: " + this.getClass().getName());
		if (migrationInfo
				.getConnectionFrom() instanceof PostgreSQLConnectionInfo
				&& migrationInfo
						.getConnectionTo() instanceof PostgreSQLConnectionInfo) {
			try {
				this.migrationInfo = migrationInfo;
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
	private PostgreSQLSchemaTableName createTargetTableSchema(
			Connection connectionFrom, Connection connectionTo)
					throws SQLException {
		/* separate schema name from the table name */
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
				migrationInfo.getObjectTo());
		/* create the target schema if it is not already there */
		PostgreSQLHandler.executeStatement(connectionTo,
				"create schema if not exists " + schemaTable.getSchemaName());

		String createTableStatement = MigrationUtils
				.getUserCreateStatement(migrationInfo);
		/*
		 * get the create table statement for the source table from the source
		 * database
		 */
		if (createTableStatement == null) {
			logger.debug(
					"Get the create statement for target table from the source database.");
			createTableStatement = PostgreSQLHandler.getCreateTable(
					connectionFrom, migrationInfo.getObjectFrom(),
					migrationInfo.getObjectTo());
		}
		PostgreSQLHandler.executeStatement(connectionTo, createTableStatement);
		return schemaTable;
	}

	@Override
	/**
	 * Migrate data from a local instance of the database to a remote one.
	 */
	public MigrationResult executeMigrationLocalRemote()
			throws MigrationException {
		return this.executeMigration();
	}

	@Override
	/**
	 * Migrate data between local instances of PostgreSQL.
	 */
	public MigrationResult executeMigrationLocally() throws MigrationException {
		return this.executeMigration();
	}

	public MigrationResult executeMigration() throws MigrationException {
		TimeStamp startTimeStamp = TimeStamp.getCurrentTime();
		logger.debug("start migration: " + startTimeStamp.toDateString());

		long startTimeMigration = System.currentTimeMillis();
		String copyFromCommand = PostgreSQLHandler
				.getExportBinCommand(getObjectFrom());
		String copyToCommand = PostgreSQLHandler
				.getLoadBinCommand(getObjectTo());
		Connection conFrom = null;
		Connection conTo = null;
		ExecutorService executor = null;
		try {
			conFrom = PostgreSQLHandler.getConnection(getConnectionFrom());
			conTo = PostgreSQLHandler.getConnection(getConnectionTo());

			conFrom.setReadOnly(true);
			conFrom.setAutoCommit(false);
			conTo.setAutoCommit(false);
			createTargetTableSchema(conFrom, conTo);

			final PipedOutputStream output = new PipedOutputStream();
			final PipedInputStream input = new PipedInputStream(output);

			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(new ExportPostgres(conFrom, copyFromCommand, output,
					new PostgreSQLHandler(getConnectionTo())));
			tasks.add(new LoadPostgres(conTo, migrationInfo, copyToCommand,
					input));
			executor = Executors.newFixedThreadPool(tasks.size());
			List<Future<Object>> results = TaskExecutor.execute(executor,
					tasks);
			Long countExtractedElements = (Long) results.get(EXPORT_INDEX)
					.get();
			Long countLoadedElements = (Long) results.get(LOAD_INDEX).get();
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			logger.debug("migration duration time msec: " + durationMsec);
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, countLoadedElements, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (Exception e) {
			String message = e.getMessage()
					+ " Migration failed. Task did not finish correctly. ";
			logger.error(message + " Stack Trace: "
					+ StackTrace.getFullStackTrace(e), e);
			if (conTo != null) {
				ExecutorService executorTerminator = null;
				try {
					conTo.abort(executorTerminator);
				} catch (SQLException ex) {
					String messageRollbackConTo = " Could not roll back "
							+ "transactions in the destination database after "
							+ "failure in data migration: " + ex.getMessage();
					logger.error(messageRollbackConTo);
					message += messageRollbackConTo;
				} finally {
					if (executorTerminator != null) {
						executorTerminator.shutdownNow();
					}
				}
			}
			if (conFrom != null) {
				ExecutorService executorTerminator = null;
				try {
					executorTerminator = Executors.newCachedThreadPool();
					conFrom.abort(executorTerminator);
				} catch (SQLException ex) {
					String messageRollbackConFrom = " Could not roll back "
							+ "transactions in the source database "
							+ "after failure in data migration: "
							+ ex.getMessage();
					logger.error(messageRollbackConFrom);
					message += messageRollbackConFrom;
				} finally {
					if (executorTerminator != null) {
						executorTerminator.shutdownNow();
					}
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
