/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * This is run as a separate thread to copy data from PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:06:36 PM
 */
public class CopyFromPostgresExecutor implements Callable<Long> {

	private static Logger log = Logger.getLogger(FromPostgresToPostgres.class);

	private final CopyManager cpFrom;
	private String copyFromString;
	private final OutputStream output;
	private Connection connection;

	public CopyFromPostgresExecutor(Connection connection,
			final String copyFromString,
			OutputStream output) throws SQLException {
		this.connection = connection;
		this.copyFromString = copyFromString;
		this.output = output;
		this.cpFrom = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Copy data from PostgreSQL.
	 * 
	 * @return number of extracted rows
	 */
	public Long call() {
		Long countExtractedRows = 0L;
		try {
			countExtractedRows = cpFrom.copyOut(copyFromString, output);
			connection.commit();
			output.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for PostgreSQL copy manager "
					+ "while copying (extracting) data from PostgreSQL.";
			log.error(msg, e);
			e.printStackTrace();
		}
		return countExtractedRows;
	}
}