/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import jline.internal.Log;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToPostgres {

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

	public class MigrationResult {
		private Long countExtractedRows;
		private Long countLoadedRows;

		public MigrationResult(Long countExtractedRows, Long countLoadedRows) {
			this.countExtractedRows = countExtractedRows;
			this.countLoadedRows = countLoadedRows;
		}

		public Long getCountExtractedRows() {
			return countExtractedRows;
		}

		public Long getCountLoadedRows() {
			return countLoadedRows;
		}

	}

	private String getCopyCommand(String table, DIRECTION direction) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append(direction.toString() + " ");
		copyFromStringBuf.append("STDOUT with binary");/* with binary */
		return copyFromStringBuf.toString();
	}

	Connection getConnection(PostgreSQLConnectionInfo conInfo) throws SQLException {
		Connection con;
		String url = conInfo.getUrl();
		String user = conInfo.getUser();
		String password = conInfo.getPassword();
		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			String msg = "Could not connect to the PostgreSQL instance: Url: " + url + " User: " + user + " Password: "
					+ password;
			logger.error(msg);
			e.printStackTrace();
			throw e;
		}
		return con;
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

	/**
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 * 
	 */
	public MigrationResult migrate(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			PostgreSQLConnectionInfo connectionTo, String toTable) throws SQLException, IOException {

		String copyFromString = getCopyCommand(fromTable, DIRECTION.TO/* STDOUT */);
		String copyToString = getCopyCommand(toTable, DIRECTION.FROM/* STDOUT */);

		Connection conFrom = null;
		Connection conTo = null;
		try {
			conFrom = getConnection(connectionFrom);
			conTo = getConnection(connectionTo);

			//Statement st = conTo.createStatement();
			//st.execute("CREATE temporary TABLE d_patients (subject_id integer NOT NULL,sex character varying(1),dob timestamp without time zone NOT NULL,dod timestamp without time zone,hospital_expire_flg character varying(1) DEFAULT 'N'::character varying)");

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
				Log.error(msg);
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
			MigrationResult result = migrator.migrate(conInfoFrom, "mimic2v26.d_patients", conInfoTo, "d_patients");
			logger.debug("Number of extracted rows: " + result.getCountExtractedRows() + " Number of loaded rows: "
					+ result.getCountLoadedRows());
		} catch (SQLException | IOException e) {
			String msg = "Problem with data migration.";
			logger.error(msg);
			e.printStackTrace();
		}

	}

}
