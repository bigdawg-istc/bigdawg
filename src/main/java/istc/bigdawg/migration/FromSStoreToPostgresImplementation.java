package istc.bigdawg.migration;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

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

public class FromSStoreToPostgresImplementation implements MigrationImplementation {

    private static Logger log = Logger.getLogger(FromSStoreToPostgresImplementation.class.getName());

    /* General message about the action in the class. */
    private String generalMessage = "Data migration from SStore to PostgreSQL";

    /* General error message when the migration fails in the class. */
    private String errMessage = generalMessage + " failed! ";

    private SStoreSQLConnectionInfo connectionFrom;
    private String fromTable;
    private PostgreSQLConnectionInfo connectionTo;
    private String toTable;
    private SStoreSQLTableMetaData sStoreSQLTableMetaData;

    /* Resources that have to be cleaned at the end of the migration process. */
    private String postgresPipe = null;
    private String sStorePipe = null;
    private ExecutorService executor = null;
    private Connection connectionSStore = null;
    private Connection connectionPostgres = null;

    public FromSStoreToPostgresImplementation(SStoreSQLConnectionInfo connectionFrom, String fromTable,
	    PostgreSQLConnectionInfo connectionTo, String toTable) throws MigrationException {
	this.connectionFrom = connectionFrom;
	this.fromTable = fromTable;
	this.connectionTo = connectionTo;
	this.toTable = toTable;
	try {
	    SStoreSQLHandler handler = new SStoreSQLHandler(connectionFrom);
	    connectionSStore = SStoreSQLHandler.getConnection(connectionFrom);
	    this.sStoreSQLTableMetaData = handler.getColumnsMetaData(fromTable);
	    connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
	    connectionPostgres.setAutoCommit(false);
	} catch (SQLException sStoreException) {
//	     MigrationException migrateException = handleException(sStoreException, "Extraction of meta data on the array: " 
//		     + fromTable + " in SciDB failed. ");
//	     throw migrateException;
	}
    }

    @Override
    public MigrationResult migrate() throws MigrationException {
	return migrateSingleThreadCSV();
    }

    private MigrationResult migrateSingleThreadCSV() throws MigrationException {
	log.info(generalMessage + " Mode: migrateSingleThreadCSV");
	long startTimeMigration = System.currentTimeMillis();
	
	try {
	    sStorePipe = Pipe.INSTANCE.createAndGetFullName("sstore.out");
//	    postgresPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_toPostgres_" + toTable);
	    executor = Executors.newFixedThreadPool(2);
	    
	    String copyFromString = SStoreSQLHandler.getExportCommand();
	    CopyFromSStoreExecutor exportExecutor = new CopyFromSStoreExecutor(connectionSStore, copyFromString, fromTable, "csv",  sStorePipe);
	    FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
	    executor.submit(exportTask);

//	    String createTableStatement = null;
//	    createTableStatement = getCreatePostgreSQLTableStatementFromSStoreTable();
//	    System.out.print(createTableStatement);
	    connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
	    connectionPostgres.setAutoCommit(false);
//	    createTargetTableSchema(connectionPostgres, createTableStatement);
	    
	    CopyToPostgresExecutor loadExecutor = new CopyToPostgresExecutor(connectionPostgres,
			getCopyToPostgreSQLCsvCommand(toTable), sStorePipe);
	    FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
	    executor.submit(loadTask);
	    
	    Long countexportElements = exportTask.get();
	    Long countLoadedElements = loadTask.get();
	    
	    finishTransaction(countexportElements, countLoadedElements);

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
	    return new MigrationResult(countLoadedElements, countexportElements, " No information about number of loaded rows.", false);
//	    return null;
//	} catch (SQLException | UnsupportedTypeException | InterruptedException
//		| ExecutionException | IOException | RunShellException exception) {
	} catch (SQLException | InterruptedException
			| ExecutionException | IOException | RunShellException exception) {
//	     MigrationException migrationException =
//	     handleException(exception, "Migration in CSV format failed. ");
	     throw new MigrationException(errMessage + " " + exception.getMessage());
	} finally {
	     cleanResources();
	}

    }

	private void finishTransaction(Long countexportElements,
			Long countLoadedElements) throws SQLException, MigrationException {
		if (!countexportElements.equals(countLoadedElements)) { // failed
	    	connectionPostgres.rollback();
	    	throw new MigrationException(errMessage + " " + "number of rows do not match");
	    } else {
	    	// Delete all tuples from S-Store
	    	String rmTupleStatement = "DELETE FROM " + fromTable;
	    	SStoreSQLHandler sstoreH = new SStoreSQLHandler(connectionFrom);
	    	sstoreH.executeUpdateQuery(rmTupleStatement);
	    	connectionPostgres.commit();
	    }
	}
    
    public MigrationResult migrateBin() throws MigrationException {
	log.info(generalMessage + " Mode: migrate postgreSQL binary format");
	long startTimeMigration = System.currentTimeMillis();
	
	try {
	    sStorePipe = Pipe.INSTANCE.createAndGetFullName("sstore.out");
	    executor = Executors.newFixedThreadPool(2);
	    
	    String copyFromString = SStoreSQLHandler.getExportCommand();
//	    System.out.println("pipe path is " + sStorePipe);
	    CopyFromSStoreExecutor exportExecutor = new CopyFromSStoreExecutor(
	    		connectionSStore, copyFromString, fromTable, "psql",  sStorePipe);
	    FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
	    executor.submit(exportTask);

	    connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
	    connectionPostgres.setAutoCommit(false);

	    String createTableStatement = null;
	    createTableStatement = getCreatePostgreSQLTableStatementFromSStoreTable();
	    createTargetTableSchema(connectionPostgres, createTableStatement);
	    
	    CopyToPostgresExecutor loadExecutor = new CopyToPostgresExecutor(connectionPostgres,
			PostgreSQLHandler.getLoadBinCommand(toTable), sStorePipe);
	    FutureTask<Long> loadTask = new FutureTask<Long>(loadExecutor);
	    executor.submit(loadTask);
	    
	    Long countexportElements = exportTask.get();
	    Long countLoadedElements = loadTask.get();
	    
	    finishTransaction(countexportElements, countLoadedElements);

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
	    return new MigrationResult(countLoadedElements, countexportElements, " No information about number of loaded rows.", false);
//	    return null;
	} catch (SQLException | InterruptedException | UnsupportedTypeException
		| ExecutionException | IOException | RunShellException exception) {
//	     MigrationException migrationException =
//	     handleException(exception, "Migration in CSV format failed. ");
	     throw new MigrationException(errMessage + " " + exception.getMessage());
	} finally {
	     cleanResources();
	}

    }
    
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
				String msg = "Could not close connection to PostgreSQL!" + e.getMessage() + " " + generalMessage;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
	}
    
    /**
	 * Get the copy to command to PostgreSQL.
	 *
	 *
	 * @return the copy command
	 */
	private String getCopyToPostgreSQLCsvCommand(String table) {
		StringBuilder copyTo = new StringBuilder("copy ");
		copyTo.append(table);
		copyTo.append(" from STDIN with ");
		copyTo.append("(format csv, delimiter '|', header false, quote \"'\")");
		String copyCommand = copyTo.toString();
		log.debug(LogUtils.replace(copyCommand));
		return copyCommand;
	}
    
    /**
	 * Create a new schema and table in the connectionTo if they not exist. The
	 * table definition has to be inferred from the table in SStore.
	 * 
	 * @throws SQLException
	 */
	private void createTargetTableSchema(Connection postgresCon, String createTableStatement) throws SQLException {
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(toTable);
		PostgreSQLHandler.executeStatement(postgresCon, "create schema if not exists " + schemaTable.getSchemaName());
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
    	List<SStoreSQLColumnMetaData> attributes = sStoreSQLTableMetaData.getColumnsOrdered();
    	
    	List<SStoreSQLColumnMetaData> origOrderAttr = new ArrayList<SStoreSQLColumnMetaData>();
    	if (!sortByNames) { // Original order
    		Collections.sort(attributes, new SStoreSQLColumnMetaDataComparator());
    	}
    	
    	List<SStoreSQLColumnMetaData> columns = new ArrayList<>();
    	columns.addAll(attributes);
    	StringBuilder createTableStringBuf = new StringBuilder();
    	createTableStringBuf.append("create table if not exists " + toTable + " (");
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
    	log.debug("create table command: " + createTableStringBuf.toString());
    	return createTableStringBuf.toString();
    	
    }
}
