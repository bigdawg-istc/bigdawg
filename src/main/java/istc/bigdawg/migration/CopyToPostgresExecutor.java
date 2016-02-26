/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * This is run in a separate thread to copy data to PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:07:05 PM
 */
public class CopyToPostgresExecutor implements Callable<Long> {

	private static Logger log = Logger.getLogger(FromPostgresToPostgres.class);

	private CopyManager cpTo;
	private String copyToString;
	private final InputStream input;
	private Connection connection;

	public CopyToPostgresExecutor(Connection connection,
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
		Long countLoadedRows = 0L;
		try {
			countLoadedRows = cpTo.copyIn(copyToString, input);
			connection.commit();
			input.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for PostgreSQL copy manager "
					+ "while copying data to PostgreSQL.";
			log.error(msg);
			e.printStackTrace();
		}
		return countLoadedRows;
	}
}
