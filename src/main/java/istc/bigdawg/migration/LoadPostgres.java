/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.utils.StackTrace;

/**
 * This is run in a separate thread to copy data to PostgreSQL.
 * 
 * The input data to be loaded to PostgreSQL can be fetched either directly from
 * a file (create an instance of the LoadPostgres class providing the name of
 * the input file) or from an InputStream.
 * 
 * This class is accessible only package-wise.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:07:05 PM
 */
public class LoadPostgres implements Callable<Object> {

	/** For internal logging in the class. */
	private static Logger log = Logger.getLogger(LoadPostgres.class);

	/** Internally we keep the handler for the copy manager for PostgreSQL. */
	private CopyManager cpTo;

	/** SQL statement which represents the copy command. */
	private String copyToString;

	/** Input stream from which the data for loading can be read. */
	private InputStream input;

	/** The name of the input file from where the data should be loaded. */
	private String inputFile;

	/** Connection to a PostgreSQL instance. */
	private Connection connection;

	/**
	 * Check: {@link #LoadPostgres(Connection, String, InputStream)}
	 * 
	 * Create task for loading data to PostgreSQL directly from a file (or named
	 * pipe).
	 * 
	 * @param connection
	 *            Connection to a PostgreSQL instance.
	 * @param copyToString
	 *            SQL statement which represent the copy command for PostgreSQL.
	 * @param inputFile
	 *            The name of the input file (or named pipe) from which the data
	 *            should be loaded.
	 * @throws SQLException
	 *             If something was wrong when the copy manager for PostgreSQL
	 *             was trying to connect to the database (for example, wrong
	 *             encoding).
	 */
	public LoadPostgres(Connection connection, final String copyToString,
			final String inputFile) throws SQLException {
		this.connection = connection;
		this.copyToString = copyToString;
		this.inputFile = inputFile;
		this.input = null;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Check: {@link #LoadPostgres(Connection, String, String)}
	 * 
	 * Create task for loading data to PostgreSQL directly from a file (or named
	 * pipe).
	 * 
	 * @param connection
	 *            Connection to a PostgreSQL instance.
	 * @param copyToString
	 *            SQL statement which represent the copy command for PostgreSQL.
	 * @param inputFile
	 *            The name of the input file (or named pipe) from which the data
	 *            should be loaded.
	 * @throws SQLException
	 *             If something was wrong when the copy manager for PostgreSQL
	 *             was trying to connect to the database (for example, wrong
	 *             encoding).
	 */
	public LoadPostgres(Connection connection, final String copyToString,
			InputStream input) throws SQLException {
		this.connection = connection;
		this.copyToString = copyToString;
		this.input = input;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Copy data to PostgreSQL.
	 * 
	 * @return number of loaded rows or -2 if there was any error during
	 *         execution
	 * @throws Exception
	 */
	public Long call() throws Exception {
		if (input == null) {
			try {
				input = new BufferedInputStream(new FileInputStream(inputFile));
			} catch (FileNotFoundException e) {
				String msg = e.getMessage()
						+ " Problem with thread for PostgreSQL copy manager "
						+ "while loading data from PostgreSQL.";
				log.error(msg + StackTrace.getFullStackTrace(e), e);
				throw e;
			}
		}
		Long countLoadedRows = 0L;
		try {
			countLoadedRows = cpTo.copyIn(copyToString, input);
			input.close();
			connection.commit();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for PostgreSQL copy manager "
					+ "while copying data to PostgreSQL.";
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			throw e;
		}
		return countLoadedRows;
	}
}
