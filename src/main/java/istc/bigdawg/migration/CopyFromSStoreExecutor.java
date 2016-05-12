package istc.bigdawg.migration;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.utils.StackTrace;

public class CopyFromSStoreExecutor implements Callable<Long> {
    
    private static Logger log = Logger.getLogger(CopyFromSStoreExecutor.class);

//	private final CopyManager cpFrom;
	final String copyFromString;
	final String tableName;
	final String trim;
	private String outputFile;
	private OutputStream output = null;
	private Connection connection;
	
	public CopyFromSStoreExecutor(Connection connectionSStoreSQL, final String copyFromString,
		final String tableName, final String trim,  OutputStream output) throws SQLException {
		this.connection = connectionSStoreSQL;
		this.copyFromString = copyFromString;
		this.tableName = tableName;
		this.trim = trim;
		this.output = output;		
//		this.cpFrom = new CopyManager((BaseConnection) connection);
	}

	public CopyFromSStoreExecutor(SStoreSQLConnectionInfo connectionSStoreSQL, final String copyFromString, 
			final String tableName,  final String trim, final String outputFile) throws SQLException {
		connection = SStoreSQLHandler.getConnection(connectionSStoreSQL);
//		connection.setAutoCommit(false);	
		this.copyFromString = copyFromString;
		this.tableName = tableName;
		this.trim = trim;
		this.outputFile = outputFile;
//		this.cpFrom = new CopyManager((BaseConnection) connection);
	}
	
	/**
	 * Copy data from SStoreSQL.
	 * 
	 * @return number of extracted rows
	 * @throws FileNotFoundException 
	 */
	public Long call() {
		log.info("start call: Copy from PostgreSQL (Executor)");
	    	System.out.println("copy from expert has been called! outputfile is: " + outputFile);
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
		log.info("issue command to PostgreSQL: Copy from PostgreSQL (Executor)");
//		System.out.println("issue command to PostgreSQL: Copy from PostgreSQL (Executor)");
		try {
//		    	countExtractedRows = cpFrom.copyOut(copyFromString, output);
			SStoreSQLHandler.executePreparedStatement(connection, copyFromString, tableName, trim);
//			connection.commit();
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
