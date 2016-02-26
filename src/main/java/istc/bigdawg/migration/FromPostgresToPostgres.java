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

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToPostgres implements FromDatabaseToDatabase {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(FromPostgresToPostgres.class);
	
	private static final int minNumberOfThreads = 2;

	/**
	 * Direction for the PostgreSQL copy command.
	 * 
	 * @author Adam Dziedzic
	 */
	private enum DIRECTION {
		TO, FROM
	};

	/**
	 * Copy out to STDOUT. Copy in from STDIN.
	 * 
	 * @author Adam Dziedzic
	 */
	private enum STDIO {
		STDOUT, STDIN
	}

	/**
	 * Get the postgresql command to copy data.
	 * 
	 * @param table
	 *            table from/to which you want to copy the data
	 * @param direction
	 *            to/from STDOUT
	 * @return the command to copy data
	 */
	private String getCopyCommand(String table, DIRECTION direction, STDIO stdio) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append(direction.toString() + " ");
		copyFromStringBuf
				.append(stdio.toString() + " with binary");/* with binary */
		return copyFromStringBuf.toString();
	}

	/**
	 * Migrate data between instances of PostgreSQL.
	 */
	public MigrationResult migrate(ConnectionInfo connectionFrom, String fromTable, ConnectionInfo connectionTo,
			String toTable) throws MigrationException {
		logger.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof PostgreSQLConnectionInfo && connectionTo instanceof PostgreSQLConnectionInfo) {
			try {
				return this.migrate((PostgreSQLConnectionInfo) connectionFrom, fromTable,
						(PostgreSQLConnectionInfo) connectionTo, toTable);
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
	private void createTargetTableSchema(Connection connectionFrom, String fromTable, Connection connectionTo,
			String toTable) throws SQLException {
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(toTable);
		PostgreSQLHandler.executeStatement(connectionTo, "create schema if not exists " + schemaTable.getSchemaName());
		String createTableStatement = PostgreSQLHandler.getCreateTable(connectionFrom, fromTable);
		PostgreSQLHandler.executeStatement(connectionTo, createTableStatement);
	}

	/**
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws Exception
	 * 
	 */
	public MigrationResult migrate(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			PostgreSQLConnectionInfo connectionTo, String toTable) throws Exception {
		long startTimeMigration = System.currentTimeMillis();

		String copyFromString = getCopyCommand(fromTable, DIRECTION.TO, STDIO.STDOUT);
		String copyToString = getCopyCommand(toTable, DIRECTION.FROM, STDIO.STDIN);

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

			// Statement st = conTo.createStatement();
			// st.execute("CREATE temporary TABLE d_patients (subject_id integer
			// NOT NULL,sex character varying(1),dob timestamp without time zone
			// NOT NULL,dod timestamp without time zone,hospital_expire_flg
			// character varying(1) DEFAULT 'N'::character varying)");

			CopyManager cpFrom = new CopyManager((BaseConnection) conFrom);
			CopyManager cpTo = new CopyManager((BaseConnection) conTo);

			final PipedOutputStream output = new PipedOutputStream();
			final PipedInputStream input = new PipedInputStream(output);

			CopyFromPostgresExecutor copyFromExecutor = new CopyFromPostgresExecutor(cpFrom, copyFromString, output);
			FutureTask<Long> taskCopyFromExecutor = new FutureTask<Long>(copyFromExecutor);

			CopyToPostgresExecutor copyToExecutor = new CopyToPostgresExecutor(cpTo, copyToString, input);
			FutureTask<Long> taskCopyToExecutor = new FutureTask<Long>(copyToExecutor);
			
			executor = Executors.newFixedThreadPool(minNumberOfThreads);
			executor.submit(taskCopyFromExecutor);
			executor.submit(taskCopyToExecutor);
			long countExtractedElements = taskCopyFromExecutor.get();
			long countLoadedElements = taskCopyToExecutor.get();

			conTo.commit();
			conFrom.commit();

			long endTimeMigration = System.currentTimeMillis();
			MigrationStatistics stats = new MigrationStatistics(connectionFrom, connectionTo, fromTable, toTable,
					startTimeMigration, endTimeMigration, countExtractedElements, countLoadedElements,
					this.getClass().getName());
			return new MigrationResult(countExtractedElements, countLoadedElements);
		} catch (Exception e) {
			e.printStackTrace();
			String msg = e.getMessage() + " Migration failed. Task did not finish correctly.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
			conTo.rollback();
			conFrom.rollback();
			throw e;
		} finally {
			if (conFrom != null) {
				// calling closed on an already closed connection has no effect
				conFrom.close();
				conFrom = null;
			}
			if (conTo != null) {
				conTo.close();
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
		FromPostgresToPostgres migrator = new FromPostgresToPostgres();
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost", "5431", "mimic2", "pguser",
				"test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo("localhost", "5430", "mimic2_copy", "pguser",
				"test");
		MigrationResult result;
		try {
			result = migrator.migrate(conInfoFrom, "mimic2v26.d_patients", conInfoTo, "mimic2v26.d_patients");
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		logger.debug("Number of extracted rows: " + result.getCountExtractedElements() + " Number of loaded rows: "
				+ result.getCountLoadedElements());

		ConnectionInfo conFrom = conInfoFrom;
		ConnectionInfo conTo = conInfoTo;
		MigrationResult result1;
		try {
			result1 = migrator.migrate(conFrom, "mimic2v26.d_patients", conTo, "mimic2v26.d_patients");
			logger.debug("Number of extracted rows: " + result1.getCountExtractedElements() + " Number of loaded rows: "
					+ result1.getCountLoadedElements());
		} catch (MigrationException e) {
			String msg = "Problem with general data migration.";
			logger.error(msg);
			e.printStackTrace();
		}

	}

}
