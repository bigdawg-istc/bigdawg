package istc.bigdawg.migration;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.utils.StackTrace;

public class CopyFromSStoreExecutor implements Callable<Long> {
    
    private static Logger log = Logger.getLogger(CopyFromSStoreExecutor.class);

	final String copyFromString;
	final String tableName;
	final String trim;
	private String outputFile;
	private OutputStream output = null;
	private Connection connection;
	final boolean caching;	// Whether caching in S-Store is turned on
	private String serverAddress;
	private int port; // Used to communicate with S-Store
	
	public CopyFromSStoreExecutor(Connection connectionSStoreSQL, final String copyFromString,
		final String tableName, final String trim,  OutputStream output,
		final boolean caching, final String serverAddress, final int port) throws SQLException {
		this.connection = connectionSStoreSQL;
		this.copyFromString = copyFromString;
		this.tableName = tableName;
		this.trim = trim;
		this.output = output;
		this.caching = caching;
		this.serverAddress = serverAddress;
		this.port = port;
	}

	public CopyFromSStoreExecutor(Connection connectionSStore, final String copyFromString, 
			final String tableName,  final String trim, final String outputFile, 
			final boolean caching, final String serverAddress, final int port) throws SQLException {
		connection = connectionSStore;
		this.copyFromString = copyFromString;
		this.tableName = tableName;
		this.trim = trim;
		this.outputFile = outputFile;
		this.caching = caching;
		this.serverAddress = serverAddress;
		this.port = port;
	}
	
	/**
	 * Copy data from SStoreSQL.
	 * 
	 * @return number of extracted rows
	 * @throws FileNotFoundException 
	 */
	public Long call() {
		log.info("start call: Copy from S-StoreSQL (Executor)");
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
		try {
			countExtractedRows =
					SStoreSQLHandler.executePreparedStatement(connection, copyFromString, tableName, trim, outputFile,
							caching, serverAddress, port);
			output.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage() + " Problem with thread for PostgreSQL copy manager "
					+ "while copying (extracting) data from PostgreSQL.";
			log.error(msg + StackTrace.getFullStackTrace(e), e);
			e.printStackTrace();
		}
		return countExtractedRows;
	}

}
