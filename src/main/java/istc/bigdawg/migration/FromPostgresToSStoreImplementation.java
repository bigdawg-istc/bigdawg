package istc.bigdawg.migration;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.log4j.Logger;
import org.voltdb.jdbc.JDBC4Connection;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.datatypes.DataTypesFromSStoreSQLToPostgreSQL;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.sstore.SStoreSQLColumnMetaData;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.sstore.SStoreSQLTableMetaData;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;

public class FromPostgresToSStoreImplementation implements MigrationImplementation {

    private static Logger log = Logger.getLogger(FromPostgresToSStoreImplementation.class.getName());

    /* General message about the action in the class. */
    private String generalMessage = "Data migration from PostgreSQL to S-Store";

    /* General error message when the migration fails in the class. */
    private String errMessage = generalMessage + " failed! ";

    private PostgreSQLConnectionInfo connectionFrom;
    private String fromTable;
    private SStoreSQLConnectionInfo connectionTo;
    private String toTable;

    /* Resources that have to be cleaned at the end of the migration process. */
    private String postgresPipe = null;
    private String sStorePipe = null;
    private ExecutorService executor = null;
    private Connection connectionSStore = null;
    private Connection connectionPostgres = null;

    public FromPostgresToSStoreImplementation(PostgreSQLConnectionInfo connectionFrom, String fromTable,
	    SStoreSQLConnectionInfo connectionTo, String toTable) throws MigrationException {
    	this.connectionFrom = connectionFrom;
    	this.fromTable = fromTable;
    	this.connectionTo = connectionTo;
    	this.toTable = toTable;
//    	PostgreSQLHandler handler = new PostgreSQLHandler(connectionFrom);
    }

    @Override
    public MigrationResult migrate() throws MigrationException {
    	return migrateSingleThreadCSV();
    }
    
    public MigrationResult migrate(boolean caching) throws MigrationException {
    	return migrateSingleThreadCSV(caching);
    }
    
    private MigrationResult migrateSingleThreadCSV() throws MigrationException {
    	return migrateSingleThreadCSV(false);
    }

    private MigrationResult migrateSingleThreadCSV(boolean caching) throws MigrationException {
	log.info(generalMessage + " Mode: migrateSingleThreadCSV");
	long startTimeMigration = System.currentTimeMillis();
	String copyFromCommand = PostgreSQLHandler
			.getExportCsvCommand(fromTable, "|", "'", false);
	String copyToCommand = SStoreSQLHandler.getImportCommand();
	
	try {
//		postgresPipe = Pipe.INSTANCE.createAndGetFullName("postgres.out");
	    postgresPipe = "/tmp/postgres.out_1";
	    executor = Executors.newFixedThreadPool(2);
	    
	    SStoreSQLHandler sStoreSQLHandler = new SStoreSQLHandler(connectionTo);
		connectionSStore = sStoreSQLHandler.getConnection(connectionTo);
	    connectionPostgres = PostgreSQLHandler.getConnection(connectionFrom);
	    connectionPostgres.setAutoCommit(false);
		connectionPostgres.setReadOnly(true);

		CopyFromPostgresExecutor copyFromExecutor = new CopyFromPostgresExecutor(
				connectionFrom, copyFromCommand, postgresPipe);
	    FutureTask<Long> exportTask = new FutureTask<Long>(copyFromExecutor);
	    executor.submit(exportTask);

	    CopyToSStoreExecutor loadExecutor = new CopyToSStoreExecutor(connectionTo,
			copyToCommand, postgresPipe, toTable, "csv");
	    FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
	    executor.submit(loadTask);
	    
	    Long countexportElements = exportTask.get();
	    Long countLoadedElements = loadTask.get();
	    
	    finishTransaction(countexportElements, countLoadedElements, caching);

	    long endTimeMigration = System.currentTimeMillis();
	    long durationMsec = endTimeMigration - startTimeMigration;
	    MigrationStatistics stats = new MigrationStatistics(connectionFrom, connectionTo, fromTable, toTable,
		    startTimeMigration, endTimeMigration, countexportElements, countLoadedElements, this.getClass().getName());
//	    Monitor.addMigrationStats(stats);
	    log.debug("Migration result,connectionFrom," + connectionFrom.toSimpleString() + ",connectionTo,"
		    + connectionTo.toString() + ",fromTable," + fromTable + ",toArray," + toTable
		    + ",startTimeMigration," + startTimeMigration + ",endTimeMigration," + endTimeMigration
		    + ",countExtractedElements," + countLoadedElements + ",countLoadedElements," + "N/A"
		    + ",durationMsec," + durationMsec + ","
		    + Thread.currentThread().getStackTrace()[1].getMethodName());
	    return new MigrationResult(countLoadedElements, countexportElements, 
	    		startTimeMigration, endTimeMigration, durationMsec, 
	    		"Migration was executed correctly.", false, 0L);
//	    return null;
	} catch (SQLException | InterruptedException
//		| ExecutionException | IOException | RunShellException exception) {
			| ExecutionException exception) {
//	     MigrationException migrationException =
//	     handleException(exception, "Migration in CSV format failed. ");
		try {
			connectionSStore.rollback();
			connectionPostgres.rollback();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	     throw new MigrationException(errMessage + " " + exception.getMessage());
	} finally {
		if (connectionPostgres != null) {
			// calling closed on an already closed connection has no effect
			try {
				connectionPostgres.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			connectionPostgres = null;
		}
		if (connectionSStore != null) {
			try {
				connectionSStore.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			connectionSStore = null;
		}
	     cleanResources();
	}

    }

	private void finishTransaction(Long countexportElements,
			Long countLoadedElements, boolean caching) throws SQLException, MigrationException {
		if (!countexportElements.equals(countLoadedElements)) { // failed
	    	connectionPostgres.rollback();
	    	throw new MigrationException(errMessage + " " + "number of rows do not match" + 
	    			"\ntable: " + fromTable +
	    			"\nexported rows: " + countexportElements + 
	    			"\nloaded rows: " + countLoadedElements);
	    } else {
	    	// Drop table in Postgres
	    	try {
	    		PostgreSQLHandler postgresH = new PostgreSQLHandler(connectionFrom);
	    		if (!caching) {
	    			postgresH.dropTableIfExists(fromTable);
	    			connectionPostgres.commit();
	    		}
	    	} catch (SQLException sqle) {
	    		connectionPostgres.rollback();
	    	}
	    }
	}


	public MigrationResult migrateBin() throws MigrationException, Exception {
		log.info(generalMessage + " Mode: migrate postgreSQL binary format");
		long startTimeMigration = System.currentTimeMillis();
		TimeStamp startTimeStamp = TimeStamp.getCurrentTime();
		log.debug("start migration: " + startTimeStamp.toDateString());
		String copyFromCommand = PostgreSQLHandler
				.getExportBinCommand(fromTable);
		String copyToCommand = SStoreSQLHandler.getImportCommand();

		Connection conFrom = null;
		Connection conTo = null;
		try {
		    postgresPipe = Pipe.INSTANCE.createAndGetFullName("postgres.out");
		    executor = Executors.newFixedThreadPool(2);

		    conFrom = PostgreSQLHandler.getConnection(connectionFrom);
		    SStoreSQLHandler sstoreSQLHandler = new SStoreSQLHandler(connectionTo);
			conTo = sstoreSQLHandler.getConnection(connectionTo);

			conFrom.setReadOnly(true);
			conFrom.setAutoCommit(false);

			CopyFromPostgresExecutor copyFromExecutor = new CopyFromPostgresExecutor(
					connectionFrom, copyFromCommand, postgresPipe);
			FutureTask<Long> taskCopyFromExecutor = new FutureTask<Long>(copyFromExecutor);
			executor.submit(taskCopyFromExecutor);
			
			CopyToSStoreExecutor copyToExecutor = new CopyToSStoreExecutor(connectionTo, copyToCommand, postgresPipe, toTable, "psql");
			FutureTask<Long> taskCopyToExecutor = new FutureTask<Long>(copyToExecutor);
			executor.submit(taskCopyToExecutor);
			
			long countExtractedElements = taskCopyFromExecutor.get();
			long countLoadedElements = taskCopyToExecutor.get();

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			log.debug("migration duration time msec: " + durationMsec);
			MigrationStatistics stats = new MigrationStatistics(connectionFrom,
					connectionTo, fromTable, toTable, startTimeMigration,
					endTimeMigration, countExtractedElements,
					countLoadedElements, this.getClass().getName());
			Monitor.addMigrationStats(stats);
			log.debug("Migration result,connectionFrom,"
					+ connectionFrom.toSimpleString() + ",connectionTo,"
					+ connectionTo.toSimpleString() + ",fromTable," + fromTable
					+ ",toTable," + toTable + ",startTimeMigration,"
					+ startTimeMigration + ",endTimeMigration,"
					+ endTimeMigration + ",countExtractedElements,"
					+ countExtractedElements + ",countLoadedElements,"
					+ countLoadedElements + ",durationMsec," + durationMsec);
			/**
			 * log table query: copy (select time,message from logs where message like 'Migration result,%' order by time desc) to '/tmp/migration_log.csv' with (format csv);
			 */
			return new MigrationResult(countExtractedElements,
					countLoadedElements);
		} catch (Exception e) {
			e.printStackTrace();
			String msg = e.getMessage()
					+ " Migration failed. Task did not finish correctly.";
			log.error(msg + StackTrace.getFullStackTrace(e), e);
			conTo.rollback();
			conFrom.rollback();
			throw e;
		} finally {
			if (conFrom != null) {
				// calling closed on an already closed connection has no effect
				conFrom.close();
				conFrom = null;
			}
			if (conTo != null) {
				conTo.close();
				conTo = null;
			}
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}
    
    
//    public MigrationResult migrateBin() throws MigrationException {
//	log.info(generalMessage + " Mode: migrate postgreSQL binary format");
//	long startTimeMigration = System.currentTimeMillis();
//	
//	try {
//	    sStorePipe = Pipe.INSTANCE.createAndGetFullName("sstore.out");
//	    executor = Executors.newFixedThreadPool(2);
//	    
//	    String copyFromString = SStoreSQLHandler.getExportCommand();
//	    System.out.println("pipe path is " + sStorePipe);
//	    CopyFromSStoreExecutor exportExecutor = new CopyFromSStoreExecutor(connectionFrom, copyFromString, fromTable, "psql",  sStorePipe);
//	    FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
//	    executor.submit(exportTask);
//
//	    connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
//	    connectionPostgres.setAutoCommit(false);
//	    String createTableStatement = null;
//	    createTableStatement = getCreatePostgreSQLTableStatementFromSStoreTable();
//	    createTargetTableSchema(connectionPostgres, createTableStatement);
//	    
//	    CopyToPostgresExecutor loadExecutor = new CopyToPostgresExecutor(connectionPostgres,
//			PostgreSQLHandler.getLoadBinCommand(toTable), sStorePipe);
//	    FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
//	    executor.submit(loadTask);
//	    
//	    Long countexportElements = exportTask.get();
//	    Long countLoadedElements = loadTask.get();
//	    
//	    // Delete all tuples from S-Store
//	    String rmTupleStatement = "DELETE FROM " + fromTable;
//	    SStoreSQLHandler sstoreH = new SStoreSQLHandler(connectionFrom);
//	    sstoreH.executeUpdateQuery(rmTupleStatement);
//	    
//
//	    long endTimeMigration = System.currentTimeMillis();
//	    long durationMsec = endTimeMigration - startTimeMigration;
//	    MigrationStatistics stats = new MigrationStatistics(connectionFrom, connectionTo, fromTable, toTable,
//		    startTimeMigration, endTimeMigration, countexportElements, countLoadedElements, this.getClass().getName());
////	    Monitor.addMigrationStats(stats);
//	    log.debug("Migration result,connectionFrom," + connectionFrom.toSimpleString() + ",connectionTo,"
//		    + connectionTo.toString() + ",fromTable," + fromTable + ",toArray," + toTable
//		    + ",startTimeMigration," + startTimeMigration + ",endTimeMigration," + endTimeMigration
//		    + ",countExtractedElements," + countLoadedElements + ",countLoadedElements," + "N/A"
//		    + ",durationMsec," + durationMsec + ","
//		    + Thread.currentThread().getStackTrace()[1].getMethodName());
//	    return new MigrationResult(countLoadedElements, countexportElements, " No information about number of loaded rows.", false);
////	    return null;
//	} catch (SQLException | InterruptedException | UnsupportedTypeException
//		| ExecutionException | IOException | RunShellException exception) {
////	     MigrationException migrationException =
////	     handleException(exception, "Migration in CSV format failed. ");
//	     throw new MigrationException(errMessage + " " + exception.getMessage());
//	} finally {
//	     cleanResources();
//	}
//
//    }
    
    /**
	 * Clean resources of this instance of the migrator at the end of migration.
	 * 
	 * @throws MigrationException
	 */
	private void cleanResources() throws MigrationException {
		if (postgresPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(postgresPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: " + postgresPipe + " " + e.getMessage());
			}
		}
		if (sStorePipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(sStorePipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: " + sStorePipe + " " + e.getMessage());
			}
		}
		if (executor != null && !executor.isShutdown()) {
			executor.shutdownNow();
		}
		if (connectionPostgres != null) {
			try {
				connectionPostgres.close();
			} catch (SQLException e) {
				e.printStackTrace();
				String msg = "Could not close connection to PostgreSQL!" + e.getMessage() + " " + generalMessage;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
		
		if (connectionSStore != null) {
			try {
			    connectionSStore.close();
			} catch (SQLException e) {
				e.printStackTrace();
				String msg = "Could not close connection to SStoreSQL!" + e.getMessage() + " " + generalMessage;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
	}
    

}
