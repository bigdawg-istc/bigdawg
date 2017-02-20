/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.datatypes.DataTypesFromSStoreSQLToPostgreSQL;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLColumnMetaData;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;

/**
 * Data migration between instances of PostgreSQL.
 * 
 * 
 * log table query: copy (select time,message from logs where message like
 * 'Migration result,%' order by time desc) to '/tmp/migration_log.csv' with
 * (format csv);
 * 
 * 
 * @author
 * 
 */
public class FromPostgresToSStore extends FromDatabaseToDatabase {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromPostgresToSStore.class);

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Always put extractor as the first task to be executed (while migrating
	 * data from S-Store to PostgreSQL).
	 */
	private static final int EXPORT_INDEX = 0;

	/**
	 * Always put loader as the second task to be executed (while migrating data
	 * from S-Store to PostgreSQL)
	 */
	private static final int LOAD_INDEX = 1;
	
	private AtomicLong globalCounter = new AtomicLong(0);
	private SStoreSQLHandler sstorehandler = null;
	private Boolean caching = false;
	private String serverAddress;
	private int serverPort;

	public FromPostgresToSStore(PostgreSQLConnectionInfo connectionFrom,
			String fromTable, SStoreSQLConnectionInfo connectionTo,
			String toTable, Boolean caching,
			String serverAddress, int serverPort) {
		this.migrationInfo = new MigrationInfo(connectionFrom, fromTable,
				connectionTo, toTable, null);
		this.sstorehandler = new SStoreSQLHandler(connectionFrom);
		this.caching = caching;
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
	}

	/**
	 * Create default instance of the class.
	 */
	public FromPostgresToSStore() {
		super();
	}	
	
	/**
	 * Migrate data from S-Store to PostgreSQL.
	 */
	public MigrationResult migrate(MigrationInfo migrationInfo)	throws MigrationException {
		logger.debug("General data migration: " + this.getClass().getName());
		if (migrationInfo.getConnectionFrom() instanceof PostgreSQLConnectionInfo
				&& migrationInfo.getConnectionTo() instanceof SStoreSQLConnectionInfo) {
			try {
				this.migrationInfo = migrationInfo;
				return this.dispatch();
			} catch (Exception e) {
				logger.error(StackTrace.getFullStackTrace(e));
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	@Override
	/**
	 * Migrate data from a local instance of the database to a remote one.
	 */
	public MigrationResult executeMigrationLocalRemote()
			throws MigrationException {
		return this.executeMigration();
	}

	@Override
	/**
	 * Migrate data between local instances of PostgreSQL.
	 */
	public MigrationResult executeMigrationLocally() throws MigrationException {
		return this.executeMigration();
	}

	public MigrationResult executeMigration() throws MigrationException {
		TimeStamp startTimeStamp = TimeStamp.getCurrentTime();
		logger.debug("start migration: " + startTimeStamp.toDateString());

		long startTimeMigration = System.currentTimeMillis();
		String copyFromCommand = PostgreSQLHandler.getExportCsvCommand(
				migrationInfo.getObjectFrom(), "|", "'", false);
		String copyToCommand = SStoreSQLHandler.getImportCommand();
		Connection conFrom = null;
		Connection conTo = null;
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			conFrom = PostgreSQLHandler.getConnection(getConnectionFrom());
			conTo = SStoreSQLHandler.getConnection(getConnectionTo());
			conFrom.setAutoCommit(false);

		    Long countExtractedElements = -1L;
		    Long countLoadedElements = -1L;
		    ServerSocket servSock = null;
		    Socket servSocket = null;
		    
		    String postgresPipe = "/tmp/psql_" + globalCounter.incrementAndGet() + ".out";
	    	OutputStream output = new BufferedOutputStream(new FileOutputStream(postgresPipe));
	    	ExportPostgres exportPostgres = new ExportPostgres(
	    			conFrom, copyFromCommand, output, new SStoreSQLHandler(getConnectionTo()));
	    	FutureTask exportTask = new FutureTask(exportPostgres);
	    	executor.submit(exportTask);
	    	countExtractedElements = (Long) exportTask.get();
		    
		    LoadSStore loadSStore = new LoadSStore();
			loadSStore.setMigrationInfo(migrationInfo);
			loadSStore.setAdditionalParams("psql", false, serverAddress, serverPort);
			loadSStore.setHandlerFrom(new PostgreSQLHandler());
			loadSStore.setFileFormat(FileFormat.CSV);
			loadSStore.setLoadFrom(postgresPipe);
			FutureTask loadTask = new FutureTask(loadSStore);
			executor.submit(loadTask);
//			countLoadedElements = (Long) loadTask.get();

	    	servSock = new ServerSocket(serverPort);
	    	servSocket = servSock.accept();
	    	BufferedReader in  = new BufferedReader(new InputStreamReader(servSocket.getInputStream()));

	    	String inputLine;
	    	while ((inputLine = in.readLine()) != null) {
	    		countLoadedElements = Long.parseLong(inputLine);
	    		break;
	    	}
	    	
			Boolean commit = countExtractedElements.equals(countLoadedElements) ? true : false;
	    	PrintWriter out = new PrintWriter(servSocket.getOutputStream(), true);
	    	out.println(commit);
	    	
	    	out.close();
		    in.close();
    		servSocket.close();
    		servSock.close();

    		long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			logger.debug("migration duration time msec: " + durationMsec);
			MigrationResult migrationResult = new MigrationResult(
					countExtractedElements, countLoadedElements, durationMsec,
					startTimeMigration, endTimeMigration);
			String message = "Migration was executed correctly.";
			return summary(migrationResult, migrationInfo, message);
		} catch (Exception e) {
			String message = e.getMessage()
					+ " Migration failed. Task did not finish correctly. ";
			logger.error(message + " Stack Trace: "
					+ StackTrace.getFullStackTrace(e), e);
			if (conTo != null) {
				ExecutorService executorTerminator = null;
				try {
					conTo.abort(executorTerminator);
				} catch (SQLException ex) {
					String messageRollbackConTo = " Could not roll back "
							+ "transactions in the destination database after "
							+ "failure in data migration: " + ex.getMessage();
					logger.error(messageRollbackConTo);
					message += messageRollbackConTo;
				} finally {
					if (executorTerminator != null) {
						executorTerminator.shutdownNow();
					}
				}
			}
			throw new MigrationException(message, e);
		} finally {
			if (conFrom != null) {
		    	// Drop table in Postgres
		    	try {
		    		PostgreSQLHandler postgresH = new PostgreSQLHandler(
		    				migrationInfo.getConnectionFrom());
		    		if (!caching) {
		    			postgresH.dropTableIfExists(migrationInfo.getObjectFrom());
		    			PostgreSQLHandler.getConnection(getConnectionTo()).commit();
		    		}
		    	} catch (SQLException sqle) {
		    		try {
						PostgreSQLHandler.getConnection(getConnectionTo()).rollback();
					} catch (SQLException e) {
						String msg = "Error in rollback of Postgres.";
						logger.error(msg + StackTrace.getFullStackTrace(e), e);
					}
		    	}
				/*
				 * calling closed on an already closed connection has no effect
				 */
				try {
					conFrom.close();
				} catch (SQLException e) {
					String msg = "Could not close the source database connection.";
					logger.error(msg + StackTrace.getFullStackTrace(e), e);
				}
				conFrom = null;
			}
			if (conTo != null) {
				try {
					conTo.close();
				} catch (SQLException e) {
					String msg = "Could not close the destination database connection.";
					logger.error(msg + StackTrace.getFullStackTrace(e), e);
				}
				conTo = null;
			}
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}
	

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		logger.debug("Migrating data from S-Store to PostgreSQL");
		ConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "pguser", "");
		ConnectionInfo conInfoTo = new SStoreSQLConnectionInfo("localhost",
				"21212", "", "user", "password");
		FromDatabaseToDatabase migrator = new FromPostgresToSStore(
				(PostgreSQLConnectionInfo)conInfoFrom, "orders", 
				(SStoreSQLConnectionInfo)conInfoTo, "orders", true, 
				"localhost", 18002);
		MigrationResult result;
		try {
			result = migrator.migrate(migrator.migrationInfo);
		} catch (MigrationException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw e;
		}
		logger.debug("Number of extracted rows: "
				+ result.getCountExtractedElements()
				+ " Number of loaded rows: " + result.getCountLoadedElements());
	}
	
}
