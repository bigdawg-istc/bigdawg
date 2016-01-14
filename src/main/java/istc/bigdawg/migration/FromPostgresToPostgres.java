/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
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

/**
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToPostgres implements FromDatabaseToDatabase {

	private static Logger logger = Logger.getLogger(FromPostgresToPostgres.class);

	/**
	 * Direction for the PostgreSQL copy command.
	 * 
	 * @author Adam Dziedzic
	 * 
	 */
	private enum DIRECTION {
		TO, FROM
	};

	private String getCopyCommand(String table, DIRECTION direction) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append(direction.toString() + " ");
		copyFromStringBuf.append("STDOUT with binary");/* with binary */
		return copyFromStringBuf.toString();
	}

	private class CopyFromExecutor implements Callable<Long> {

		private CopyManager cpFrom;
		private String copyFromString;
		private final PipedOutputStream output;

		private CopyFromExecutor(final CopyManager cpFrom, final String copyFromString,
				final PipedOutputStream output) {
			this.cpFrom = cpFrom;
			this.copyFromString = copyFromString;
			this.output = output;
		}

		public Long call() {
			Long countLoadedRows = 0L;
			try {
				countLoadedRows = cpFrom.copyOut(copyFromString, output);
				output.close();
			} catch (IOException e) {
				String msg = "Problem with thread for PostgreSQL copy manager " + "while copying data from PostgreSQL.";
				logger.error(msg);
				e.printStackTrace();
			} catch (SQLException e) {
				String msg = "SQL problem for copy data from PostgreSQL.";
				logger.error(msg);
				e.printStackTrace();
			}
			return countLoadedRows;
		}
	}

	private class CopyToExecutor implements Callable<Long> {

		private CopyManager cpTo;
		private String copyToString;
		private final PipedInputStream input;

		public CopyToExecutor(final CopyManager cpTo, final String copyToString, final PipedInputStream input) {
			this.cpTo = cpTo;
			this.copyToString = copyToString;
			this.input = input;
		}

		public Long call() {
			/* Number of extracted rows. */
			Long countExtractedRows = 0L;
			try {
				countExtractedRows = cpTo.copyIn(copyToString, input);
				input.close();
			} catch (IOException e) {
				String msg = "Problem with thread for PostgreSQL copy manager " + "while copying data to PostgreSQL.";
				logger.error(msg);
				e.printStackTrace();
			} catch (SQLException e) {
				String msg = "SQL problem for copy data from PostgreSQL.";
				logger.error(msg);
				e.printStackTrace();
			}
			return countExtractedRows;
		}
	}

	public MigrationResult migrate(ConnectionInfo connectionFrom, String fromTable, ConnectionInfo connectionTo,
			String toTable) throws MigrationException {
		logger.debug("General data migration.");
		if (connectionFrom instanceof PostgreSQLConnectionInfo && connectionTo instanceof PostgreSQLConnectionInfo) {
			try {
				return this.migrate((PostgreSQLConnectionInfo) connectionFrom, fromTable,
						(PostgreSQLConnectionInfo) connectionTo, toTable);
			} catch (SQLException | IOException e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	private class TargetSchemaTable {
		private String schemaName;
		private boolean wasSchemaCreated;
		private String tableName;
		private boolean wasTableCreated;

		public TargetSchemaTable(String schemaName, boolean wasSchemaCreated, String tableName,
				boolean wasTableCreated) {
			this.schemaName = schemaName;
			this.wasSchemaCreated = wasSchemaCreated;
			this.tableName = tableName;
			this.wasTableCreated = wasTableCreated;
		}

		public String getSchemaName() {
			return schemaName;
		}

		public boolean isWasSchemaCreated() {
			return wasSchemaCreated;
		}

		public String getTableName() {
			return tableName;
		}

		public boolean isWasTableCreated() {
			return wasTableCreated;
		}
	}

	private TargetSchemaTable createTargetSchemaTableIfNotExist(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			PostgreSQLConnectionInfo connectionTo, String toTable) {
		Connection conFrom = null;
		Connection conTo = null;
		PostgreSQLSchemaTableName schemaTableTo = new PostgreSQLSchemaTableName(toTable);
		try {	
			PostgreSQLHandler postgresToHanlder = new PostgreSQLHandler(connectionTo);
			conTo = PostgreSQLHandler.getConnection(connectionTo);
			
		} catch (SQLException e) {
			e.printStackTrace();
			
		}
		
		return null;
	}

	/**
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 * 
	 */
	public MigrationResult migrate(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			PostgreSQLConnectionInfo connectionTo, String toTable) throws SQLException, IOException {
		logger.debug("Specific data migration");

		String copyFromString = getCopyCommand(fromTable, DIRECTION.TO/* STDOUT */);
		String copyToString = getCopyCommand(toTable, DIRECTION.FROM/* STDIN */);

		Connection conFrom = null;
		Connection conTo = null;
		try {
			conFrom = PostgreSQLHandler.getConnection(connectionFrom);
			conTo = PostgreSQLHandler.getConnection(connectionTo);

			// Statement st = conTo.createStatement();
			// st.execute("CREATE temporary TABLE d_patients (subject_id integer
			// NOT NULL,sex character varying(1),dob timestamp without time zone
			// NOT NULL,dod timestamp without time zone,hospital_expire_flg
			// character varying(1) DEFAULT 'N'::character varying)");

			CopyManager cpFrom = new CopyManager((BaseConnection) conFrom);
			CopyManager cpTo = new CopyManager((BaseConnection) conTo);

			final PipedOutputStream output = new PipedOutputStream();
			final PipedInputStream input = new PipedInputStream(output);

			CopyFromExecutor copyFromExecutor = new CopyFromExecutor(cpFrom, copyFromString, output);
			FutureTask<Long> taskCopyFromExecutor = new FutureTask<Long>(copyFromExecutor);
			Thread copyFromThread = new Thread(taskCopyFromExecutor);

			CopyToExecutor copyToExecutor = new CopyToExecutor(cpTo, copyToString, input);
			FutureTask<Long> taskCopyToExecutor = new FutureTask<Long>(copyToExecutor);
			Thread copyToThread = new Thread(taskCopyToExecutor);

			copyFromThread.start();
			copyToThread.start();
			try {
				copyFromThread.join();
			} catch (InterruptedException e1) {
				String msg = "Not possible to join the thread to copy data from PostgreSQL.";
				logger.error(msg);
				e1.printStackTrace();
			}
			try {
				copyToThread.join();
			} catch (InterruptedException e) {
				String msg = "Not possible to join the thread to copy data to PostgreSQL.";
				logger.error(msg);
				e.printStackTrace();
			}
			try {
				return new MigrationResult(taskCopyFromExecutor.get(), taskCopyToExecutor.get());
			} catch (InterruptedException | ExecutionException e) {
				String msg = "Migration failed. Task did not finish correctly.";
				logger.error(msg);
				e.printStackTrace();
			}
		} finally {
			if (conFrom != null) {
				conFrom.close();
			}
			if (conTo != null) {
				conTo.close();
			}
		}
		return null;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		LoggerSetup.setLogging();
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		FromPostgresToPostgres migrator = new FromPostgresToPostgres();
		PostgreSQLConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost", "5431", "mimic2", "pguser",
				"test");
		PostgreSQLConnectionInfo conInfoTo = new PostgreSQLConnectionInfo("localhost", "5430", "mimic2_copy", "pguser",
				"test");
		try {
			MigrationResult result = migrator.migrate(conInfoFrom, "mimic2v26.d_patients", conInfoTo,
					"mimic2v26.d_patients");
			logger.debug("Number of extracted rows: " + result.getCountExtractedRows() + " Number of loaded rows: "
					+ result.getCountLoadedRows());
		} catch (SQLException | IOException e) {
			String msg = "Problem with specific data migration.";
			logger.error(msg);
			e.printStackTrace();
		}

		ConnectionInfo conFrom = conInfoFrom;
		ConnectionInfo conTo = conInfoTo;
		MigrationResult result;
		try {
			result = migrator.migrate(conFrom, "mimic2v26.d_patients", conTo, "mimic2v26.d_patients");
			logger.debug("Number of extracted rows: " + result.getCountExtractedRows() + " Number of loaded rows: "
					+ result.getCountLoadedRows());
		} catch (MigrationException e) {
			String msg = "Problem with general data migration.";
			logger.error(msg);
			e.printStackTrace();
		}

	}

}
