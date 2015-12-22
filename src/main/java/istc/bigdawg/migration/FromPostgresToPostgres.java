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

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;

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

	private String getCopyCommand(String table, DIRECTION direction) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append(direction.toString() + " ");
		copyFromStringBuf.append("STDOUT with binary");/* with binary */
		return copyFromStringBuf.toString();
	}

	Connection getConnection(String url, String user, String password) throws SQLException {
		Connection con;
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
	
	private Thread getCpFromThread(final CopyManager cpFrom, final String copyFromString, final PipedOutputStream output) {
		return new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					cpFrom.copyOut(copyFromString, output);
					output.close();
				} catch (IOException e) {
					String msg = "Problem with thread for PostgreSQL copy manager "
							+ "while copying data from PostgreSQL.";
					logger.error(msg);
					e.printStackTrace();
				} catch (SQLException e) {
					String msg = "SQL problem for copy data from PostgreSQL.";
					logger.error(msg);
					e.printStackTrace();
				}
			}
		});
	}

	private Thread getCpToThread(final CopyManager cpTo, final String copyToString, final PipedInputStream input) {
		return new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					cpTo.copyIn(copyToString, input);
				} catch (IOException e) {
					String msg = "Problem with thread for PostgreSQL copy manager "
							+ "while copying data to PostgreSQL.";
					logger.error(msg);
					e.printStackTrace();
				} catch (SQLException e) {
					String msg = "SQL problem for copy data from PostgreSQL.";
					logger.error(msg);
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * @throws SQLException
	 * @throws IOException
	 * 
	 */
	public void migrate(String fromUrl, String fromUser, String fromPassword, String fromTable, String toUrl,
			String toUser, String toPassword, String toTable) throws SQLException, IOException {

		String copyFromString = getCopyCommand(fromTable, DIRECTION.TO/* STDOUT */);
		String copyToString = getCopyCommand(toTable, DIRECTION.FROM/* STDOUT */);

		Connection conFrom = getConnection(fromUrl, fromUser, fromPassword);
		Connection conTo = getConnection(toUrl, toUser, toPassword);

		CopyManager cpFrom = new CopyManager((BaseConnection) conFrom);
		CopyManager cpTo = new CopyManager((BaseConnection) conTo);

		final PipedOutputStream output = new PipedOutputStream();
		final PipedInputStream input = new PipedInputStream(output);

		Thread cpFromThread = getCpFromThread(cpFrom,copyFromString,output);
		Thread cpToThread = getCpToThread(cpTo, copyToString, input);

		cpFromThread.start();
		cpToThread.start();

		try {
			cpFromThread.join();
		} catch (InterruptedException e1) {
			String msg = "Not possibe to join the thread to copy data from PostgreSQL.";
			logger.error(msg);
			e1.printStackTrace();
		}
		try {
			cpToThread.join();
		} catch (InterruptedException e) {
			String msg = "Not possibe to join the thread to copy data to PostgreSQL.";
			logger.error(msg);
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		LoggerSetup.setLogging();
		System.out.println("Migrating data from PostgreSQL to PostgreSQL");
		FromPostgresToPostgres migrator = new FromPostgresToPostgres();
		try {
			migrator.migrate("jdbc:postgresql://localhost:5431/mimic2", "pguser", "test", "mimic2v26.d_patients",
					"jdbc:postgresql://localhost:5432/mimic2_copy", "pguser", "test", "mimic2v26.d_patients");
		} catch (SQLException | IOException e) {
			String msg = "Problem with data migration.";
			logger.error(msg);
			e.printStackTrace();
		}

	}

}
