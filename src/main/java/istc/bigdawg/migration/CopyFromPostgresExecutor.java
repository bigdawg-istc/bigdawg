/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.utils.StackTrace;

/**
 * This is run as a separate thread to copy data from PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:06:36 PM
 */
public class CopyFromPostgresExecutor implements Callable<Long> {

	private static Logger log = Logger.getLogger(CopyFromPostgresExecutor.class);

	private final CopyManager cpFrom;
	private String copyFromString;
	private String outputFile;
	private OutputStream output = null;
	private Connection connection;
	
	public CopyFromPostgresExecutor(Connection connectionPostgreSQL, final String copyFromString,
			OutputStream output) throws SQLException {
		this.connection = connectionPostgreSQL;
		this.copyFromString = copyFromString;
		this.output = output;		
		this.cpFrom = new CopyManager((BaseConnection) connection);
	}

	public CopyFromPostgresExecutor(PostgreSQLConnectionInfo connectionPostgreSQL, final String copyFromString,
			final String outputFile) throws SQLException {
		connection = PostgreSQLHandler.getConnection(connectionPostgreSQL);
		connection.setAutoCommit(false);
		connection.setReadOnly(true);
		this.copyFromString = copyFromString;
		this.outputFile = outputFile;
		this.cpFrom = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Copy data from PostgreSQL.
	 * 
	 * @return number of extracted rows
	 * @throws FileNotFoundException 
	 */
	public Long call() {
		log.debug("start call: Copy from PostgreSQL (Executor)");
		if (output == null) {
			try {
				output = new BufferedOutputStream(new FileOutputStream(outputFile));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				String msg = e.getMessage() + " Problem with thread for PostgreSQL copy manager "
					+ "while copying (extracting) data from PostgreSQL.";
				log.error(msg + StackTrace.getFullStackTrace(e), e);
				return -1L;
			}
		}
		Long countExtractedRows = 0L;
		log.debug("issue command to PostgreSQL: Copy from PostgreSQL (Executor)");
		try {
			countExtractedRows = cpFrom.copyOut(copyFromString, output);
			//log.debug("psql statement: " + copyFromString);
			//PostgreSQLHandler.executeStatement(connection, copyFromString);
			connection.commit();
			output.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage() + " Problem with thread for PostgreSQL copy manager "
					+ "while copying (extracting) data from PostgreSQL.";
			log.error(msg + StackTrace.getFullStackTrace(e), e);
			e.printStackTrace();
		}
		log.info("Number of rows extracted from Postgres: " + countExtractedRows);
		return countExtractedRows;
	}
}