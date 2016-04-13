package istc.bigdawg.migration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import istc.bigdawg.sstore.CopyFromSStoreExecutor;
import istc.bigdawg.sstore.SStoreSQLColumnMetaData;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.sstore.SStoreSQLTableMetaData;
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
	    this.sStoreSQLTableMetaData = handler.getColumnsMetaData(fromTable);
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
	String delimiter = "|";
	try {
	    sStorePipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_fromSStore_" + fromTable);
	    postgresPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_toPostgres_" + toTable);
	    executor = Executors.newFixedThreadPool(3);

//	    CopyFromPostgresExecutor exportExecutor = new CopyFromSStoreExecutor(connectionFrom,
//		    SStoreSQLHandler.getExportCsvCommand(fromTable, delimiter), sStorePipe);
//	    FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
//	    executor.submit(exportTask);

	    String createTableStatement = null;
	    createTableStatement = getCreatePostgreSQLTableStatementFromSStoreTable();

	    connectionPostgres = PostgreSQLHandler.getConnection(connectionTo);
	    connectionPostgres.setAutoCommit(false);
	    createTargetTableSchema(connectionPostgres, createTableStatement);
	    

	    long endTimeMigration = System.currentTimeMillis();
	    long durationMsec = endTimeMigration - startTimeMigration;
//	    MigrationStatistics stats = new MigrationStatistics(connectionFrom, connectionTo, fromTable, toTable,
//		    startTimeMigration, endTimeMigration, null, countExtractedElements, this.getClass().getName());
//	    Monitor.addMigrationStats(stats);
//	    log.debug("Migration result,connectionFrom," + connectionFrom.toSimpleString() + ",connectionTo,"
//		    + connectionTo.toString() + ",fromTable," + fromTable + ",toArray," + toArray
//		    + ",startTimeMigration," + startTimeMigration + ",endTimeMigration," + endTimeMigration
//		    + ",countExtractedElements," + countExtractedElements + ",countLoadedElements," + "N/A"
//		    + ",durationMsec," + durationMsec + ","
//		    + Thread.currentThread().getStackTrace()[1].getMethodName());
//	    return new MigrationResult(countExtractedElements, null,
//		    loadMessage + " No information about number of loaded rows.", false);
	    return null;
	} catch (SQLException | UnsupportedTypeException | InterruptedException
		| IOException | RunShellException exception) {
//	     MigrationException migrationException =
//	     handleException(exception, "Migration in CSV format failed. ");
	     throw new MigrationException(errMessage + " " + exception.getMessage());
	} finally {
	    // cleanResources();
	}

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
	List<SStoreSQLColumnMetaData> attributes = sStoreSQLTableMetaData.getColumnsOrdered();
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
