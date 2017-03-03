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
import istc.bigdawg.query.DBHandler;
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
public class FromSStoreToPostgres extends FromDatabaseToDatabase {

	/*
	 * log
	 */
	private static Logger logger = Logger
			.getLogger(FromSStoreToPostgres.class);

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
	
	private String serverAddress;
	private int serverPort;
	private FileFormat fileFormat;

	public FromSStoreToPostgres(SStoreSQLConnectionInfo connectionFrom,
			String fromTable, PostgreSQLConnectionInfo connectionTo,
			String toTable,
			FileFormat fileFormat,
			String serverAddress, int serverPort) {
		this.migrationInfo = new MigrationInfo(connectionFrom, fromTable,
				connectionTo, toTable, null);
		this.sstorehandler = new SStoreSQLHandler(connectionFrom);
		this.fileFormat = fileFormat;
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
	}

	/**
	 * Create default instance of the class.
	 */
	public FromSStoreToPostgres() {
		super();
	}	
	
	/**
	 * Migrate data from S-Store to PostgreSQL.
	 */
	public MigrationResult migrate(MigrationInfo migrationInfo)	throws MigrationException {
		logger.debug("General data migration: " + this.getClass().getName());
		if (migrationInfo.getConnectionFrom() instanceof SStoreSQLConnectionInfo
				&& migrationInfo.getConnectionTo() instanceof PostgreSQLConnectionInfo) {
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

	/**
	 * Create a new schema and table in the connectionTo if they do not exist.
	 * 
	 * Get the table definition from the connectionFrom.
	 * 
	 * @param connectionFrom
	 *            from which database we fetch the data
	 * @param fromTable
	 *            from which table we fetch the data
	 * @param connectionTo
	 *            to which database we connect to
	 * @param toTable
	 *            to which table we want to load the data
	 * @throws SQLException
	 */
	private PostgreSQLSchemaTableName createTargetTableSchema(Connection connectionTo)
					throws SQLException {
		/* separate schema name from the table name */
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(migrationInfo.getObjectTo());
		/* create the target schema if it is not already there */
		PostgreSQLHandler.executeStatement(connectionTo, "create schema if not exists " + schemaTable.getSchemaName());

		String createTableStatement = MigrationUtils.getUserCreateStatement(migrationInfo);
		/*
		 * get the create table statement for the source table from the source
		 * database
		 */
		if (createTableStatement == null) {
			logger.debug(
					"Get the create statement for target table from S-Store.");
			try {
				createTableStatement = getCreatePostgreSQLTableStatementFromSStoreTable();
			} catch (UnsupportedTypeException e) {
				throw (new SQLException("Cannot get create table statement from S-Store.")); 
			}
		}
		createTargetTableSchema(connectionTo, createTableStatement);
		return schemaTable;
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
		String copyFromCommand = SStoreSQLHandler.getExportCommand();
		String copyToCommand;
		
		if (fileFormat == FileFormat.BIN_POSTGRES) {
			copyToCommand = PostgreSQLHandler
					.getLoadBinCommand(getObjectTo());
		} else if (fileFormat == FileFormat.CSV){
			DBHandler toHandler = new PostgreSQLHandler(migrationInfo.getConnectionTo());
			copyToCommand = PostgreSQLHandler.getLoadCsvCommand(getObjectTo(), 
					toHandler.getCsvExportDelimiter(),
					FileFormat.getQuoteCharacter(),
					toHandler.isCsvExportHeader());
		} else {
			throw (new MigrationException("Unsupported file format"));
		}
		
		Connection conFrom = null;
		Connection conTo = null;
		ExecutorService executor = Executors.newFixedThreadPool(2);
		try {
			conFrom = SStoreSQLHandler.getConnection(getConnectionFrom());
			conTo = PostgreSQLHandler.getConnection(getConnectionTo());
			conTo.setAutoCommit(false);

			createTargetTableSchema(conTo);
			
		    Long countExtractedElements = -1L;
		    Long countLoadedElements = -1L;
		    ServerSocket servSock = null;
		    Socket servSocket = null;
		    
			ExportSStore exportSStore = new ExportSStore();
			exportSStore.setMigrationInfo(migrationInfo);
			String trim;
			if (fileFormat == FileFormat.BIN_POSTGRES) {
				trim = "psql";
			} else {
				trim = "csv";
			}
			exportSStore.setAdditionalParams(trim, false, serverAddress, serverPort);
			exportSStore.setHandlerTo(new PostgreSQLHandler());
			exportSStore.setFileFormat(fileFormat);
			// We cannot set an OutputStream for S-Store, 
			// because S-Store currently only takes a file, and write to the file in the db engine.
		    String sStorePipe = "/tmp/sstore_" + globalCounter.incrementAndGet() + ".out";
			exportSStore.setExportTo(sStorePipe);
			FutureTask exportTask = new FutureTask(exportSStore);
			executor.submit(exportTask);

	    	servSock = new ServerSocket(serverPort);
	    	servSocket = servSock.accept();
	    	BufferedReader in  = new BufferedReader(new InputStreamReader(servSocket.getInputStream()));
	    	PrintWriter out = new PrintWriter(servSocket.getOutputStream(), true);

	    	String inputLine;
	    	
	    	while ((inputLine = in.readLine()) != null) {
	    		countExtractedElements = Long.parseLong(inputLine);
	    		break;
	    	}
			
	    	InputStream input = new BufferedInputStream(new FileInputStream(sStorePipe));
			LoadPostgres loadPostgres = new LoadPostgres(conTo, migrationInfo, copyToCommand, input);
			FutureTask loadTask = new FutureTask(loadPostgres);
		    executor.submit(loadTask);
		    countLoadedElements = (Long) loadTask.get();

			Boolean commit = countExtractedElements.equals(countLoadedElements) ? true : false;
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
	 * Create a new schema and table in the connectionTo if they not exist. The
	 * table definition has to be inferred from the table in SStore.
	 * 
	 * @throws SQLException
	 */
	private void createTargetTableSchema(Connection postgresCon, String createTableStatement) throws SQLException {
		PostgreSQLHandler.executeStatement(postgresCon, createTableStatement);
	}

    private String getCreatePostgreSQLTableStatementFromSStoreTable()
	    throws SQLException, UnsupportedTypeException {
    	return getCreatePostgreSQLTableStatementFromSStoreTable(false);
    }
    
    // Sort the attributes by their positions
    class SStoreSQLColumnMetaDataComparator implements Comparator<SStoreSQLColumnMetaData> {
    	public int compare(SStoreSQLColumnMetaData s1, SStoreSQLColumnMetaData s2) {
    		return s1.getPosition() - s2.getPosition();
    	}
    }
    
    private String getCreatePostgreSQLTableStatementFromSStoreTable(Boolean sortByNames)
	    throws SQLException, UnsupportedTypeException {
    	List<SStoreSQLColumnMetaData> attributes = 
    			sstorehandler.getColumnsMetaData(migrationInfo.getObjectFrom()).getColumnsOrdered();
    	
    	List<SStoreSQLColumnMetaData> origOrderAttr = new ArrayList<SStoreSQLColumnMetaData>();
    	if (!sortByNames) { // Original order
    		Collections.sort(attributes, new SStoreSQLColumnMetaDataComparator());
    	}
    	
    	List<SStoreSQLColumnMetaData> columns = new ArrayList<>();
    	columns.addAll(attributes);
    	StringBuilder createTableStringBuf = new StringBuilder();
    	createTableStringBuf.append("create table if not exists " + migrationInfo.getObjectTo() + " (");
    	for (SStoreSQLColumnMetaData column : columns) {
    	    String colName = column.getName();
    	    String sStoreType = column.getdataType();
    	    String postgresType = DataTypesFromSStoreSQLToPostgreSQL.getPostgreSQLTypeFromSStoreType(sStoreType);
    	    if ("varchar".equals(postgresType)) {
    		postgresType += "(" + column.getCharacterMaximumLength() + ")";
    	    }
    	    String nullable = column.isNullable() ? "" : "NOT NULL";
    	    createTableStringBuf.append(colName + " " + postgresType +  " " + nullable + ",");
    	}
    	createTableStringBuf.deleteCharAt(createTableStringBuf.length() - 1);
    	createTableStringBuf.append(")");
    	logger.debug("create table command: " + createTableStringBuf.toString());
    	return createTableStringBuf.toString();
    	
    }
	
	

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		logger.debug("Migrating data from S-Store to PostgreSQL");
		ConnectionInfo conInfoFrom = new SStoreSQLConnectionInfo("localhost",
				"21212", "", "user", "password");
		ConnectionInfo conInfoTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "pguser", "");
		FromDatabaseToDatabase migrator = new FromSStoreToPostgres(
				(SStoreSQLConnectionInfo)conInfoFrom, "orders", 
				(PostgreSQLConnectionInfo)conInfoTo, "orders",
//				FileFormat.BIN_POSTGRES,
				FileFormat.CSV,
				"localhost", 18001);
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
