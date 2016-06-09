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

import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * This is run in a separate thread to copy data to PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:07:05 PM
 */
public class LoadPostgres implements Callable<Long> {

	private static Logger log = Logger.getLogger(LoadPostgres.class);

	private CopyManager cpTo;
	private String copyToString;
	private InputStream input;
	private String inputFile;
	private Connection connection;

	public LoadPostgres(Connection connection,
			final String copyToString, final String inputFile)
					throws SQLException {
		this.connection = connection;
		this.copyToString = copyToString;
		this.inputFile = inputFile;
		this.input = null;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	public LoadPostgres(Connection connection,
			final String copyToString, InputStream input) throws SQLException {
		this.connection = connection;
		this.copyToString = copyToString;
		this.input = input;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Copy data to PostgreSQL.
	 * 
	 * @return number of loaded rows
	 */
	public Long call() {
		if (input == null) {
			try {
				input = new BufferedInputStream(new FileInputStream(inputFile));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				String msg = e.getMessage()
						+ " Problem with thread for PostgreSQL copy manager "
						+ "while loading data from PostgreSQL.";
				log.error(msg + StackTrace.getFullStackTrace(e), e);
				return -1L;
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
			/*
			 * remove the quotes - our postgresql database for logs cannot accept
			 * such input
			 */
			log.error(LogUtils.replace(msg));
			e.printStackTrace();
		}
		return countLoadedRows;
	}
}
