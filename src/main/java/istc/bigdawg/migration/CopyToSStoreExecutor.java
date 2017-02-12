/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

public class CopyToSStoreExecutor implements Callable<Long> {

	private static Logger log = Logger.getLogger(CopyToSStoreExecutor.class);

	private String copyToString;
	private InputStream input;
	private String inputFile;
	private Connection connection;
	final String tableName;
	final String trim;

	public CopyToSStoreExecutor(SStoreSQLConnectionInfo connectionSStoreSQL,
			final String copyToString, final String inputFile, 
			final String tableName, final String trim) throws SQLException {
		this.connection = SStoreSQLHandler.getConnection(connectionSStoreSQL);
		this.copyToString = copyToString;
		this.inputFile = inputFile;
		this.tableName = tableName;
		this.trim = trim;
		this.input = null;
	}

	public CopyToSStoreExecutor(SStoreSQLConnectionInfo connectionSStoreSQL,
			final String copyToString, InputStream input,
			final String tableName, final String trim) throws SQLException {
		this.connection = SStoreSQLHandler.getConnection(connectionSStoreSQL);
		this.copyToString = copyToString;
		this.input = input;
		this.tableName = tableName;
		this.trim = trim;
	}

	/**
	 * Copy data to S-Store.
	 * 
	 * @return number of loaded rows
	 */
	public Long call() {
		log.info("start call: Copy to S-Store (Executor)");
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
		Long countLoadedRows = -2L;
		try {
			countLoadedRows = 
					SStoreSQLHandler.executePreparedImportStatement(connection, copyToString, tableName, input, trim, inputFile);
			input.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for S-Store copy manager "
					+ "while copying data to S-Store.";
			log.error(LogUtils.replace(msg));
			e.printStackTrace();
		}
		return countLoadedRows;
	}
	
}
