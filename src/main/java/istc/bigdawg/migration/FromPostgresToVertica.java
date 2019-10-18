package istc.bigdawg.migration;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.relational.RelationalHandler;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;
import istc.bigdawg.vertica.VerticaConnectionInfo;
import istc.bigdawg.vertica.VerticaHandler;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.log4j.Logger;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author kateyu
 */
public class FromPostgresToVertica extends FromDatabaseToDatabase {

    /*
     * log
     */
    private static Logger logger = Logger
            .getLogger(FromPostgresToVertica.class);

    private String verticaPipe;

    /**
     * Always put extractor as the first task to be executed (while migrating
     * data from PostgreSQL to Vertica).
     */
    private static final int EXPORT_INDEX = 0;

    /**
     * Always put loader as the second task to be executed (while migrating data
     * from PostgreSQL to Vertica)
     */
    private static final int LOAD_INDEX = 1;

    public FromPostgresToVertica(ConnectionInfo connectionFrom,
                               String fromTable, ConnectionInfo connectionTo,
                               String toTable) {
        this.migrationInfo = new MigrationInfo(connectionFrom, fromTable,
                connectionTo, toTable, null);
    }

    /**
     * Create default instance of the class.
     */
    public FromPostgresToVertica() {
        super();
    }

    /**
     * Migrate data from Postgres instance to Vertica.
     */
    public MigrationResult migrate(MigrationInfo migrationInfo)	throws MigrationException {
        logger.debug("General data migration: " + this.getClass().getName());
        if (migrationInfo.getConnectionFrom() instanceof PostgreSQLConnectionInfo
                && migrationInfo.getConnectionTo() instanceof VerticaConnectionInfo) {
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
    private PostgreSQLSchemaTableName createTargetTableSchema(
            Connection connectionFrom, Connection connectionTo)
            throws SQLException {
		/* separate schema name from the table name */
        PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
                migrationInfo.getObjectTo());
		/* create the target schema if it is not already there */
		String schemaName = schemaTable.getSchemaName();
		if (!schemaName.equals("public")) {
		    // "public" is reserved in Vertica, will throw error if you try and recreate it
            VerticaHandler.executeStatement(connectionTo,
                    "create schema if not exists " + schemaTable.getSchemaName());
        }

        String createTableStatement = MigrationUtils
                .getUserCreateStatement(migrationInfo);
        Optional<String> name = migrationInfo.getMigrationParams().flatMap(MigrationParams::getName);
        /*
         * get the create table statement for the source table from the source
         * database
         */
        if (createTableStatement == null) {
            logger.debug(
                    "Get the create statement for target table from the source database.");
            String postgresCreateTable = PostgreSQLHandler.getCreateTable(
                    connectionFrom, migrationInfo.getObjectFrom(),
                    migrationInfo.getObjectTo());
            logger.debug("postgresCreateTable statement: " + postgresCreateTable);
            createTableStatement = postgresCreateTable;
            name = Optional.of(migrationInfo.getObjectTo());
        }
        if (name.isPresent() && BigDawgConfigProperties.INSTANCE.isVerticaDropDataSet()) {
            VerticaHandler.executeStatement(connectionTo, RelationalHandler.getDropTableStatement(name.get()));
        }
        VerticaHandler.executeStatement(connectionTo, createTableStatement);
        return schemaTable;
    }

    @Override
    /**
     * Migrate data between local instances of PostgreSQL.
     */
    public MigrationResult executeMigrationLocally() throws MigrationException {
        return this.migrateSingleThreadCSV();
    }

    public MigrationResult migrateSingleThreadCSV() throws MigrationException {
        TimeStamp startTimeStamp = TimeStamp.getCurrentTime();
        logger.debug("start migration: " + startTimeStamp.toDateString());

        long startTimeMigration = System.currentTimeMillis();
        String copyFromCommand = PostgreSQLHandler
                .getExportCsvCommand(getObjectFrom(), FileFormat.getCsvDelimiter(),
                        FileFormat.getQuoteCharacter(), false);
        Connection conFrom = null;
        Connection conTo = null;
        ExecutorService executor = null;
        try {
            conFrom = PostgreSQLHandler.getConnection(getConnectionFrom());
            conTo = VerticaHandler.getConnection(getConnectionTo());

            conFrom.setReadOnly(true);
            conFrom.setAutoCommit(false);
            conTo.setAutoCommit(false);

            createTargetTableSchema(conFrom, conTo);

            final PipedOutputStream output = new PipedOutputStream();
            final PipedInputStream input = new PipedInputStream(output);
            verticaPipe = Pipe.INSTANCE.createAndGetFullName(
                    this.getClass().getName() + "_toVertica_" + getObjectFrom());

            List<Callable<Object>> tasks = new ArrayList<>();
            tasks.add(new ExportPostgres(migrationInfo.getConnectionFrom(), copyFromCommand,
                    verticaPipe, new VerticaHandler(getConnectionTo())));
            tasks.add(new LoadVertica(conTo, migrationInfo, verticaPipe));

            executor = Executors.newFixedThreadPool(tasks.size());
            logger.debug("Executing tasks");
            List<Future<Object>> results = TaskExecutor.execute(executor,
                    tasks);
            Long countExtractedElements = (Long) results.get(EXPORT_INDEX)
                    .get();
            Long countLoadedElements = (Long) results.get(LOAD_INDEX).get();
            long endTimeMigration = System.currentTimeMillis();
            long durationMsec = endTimeMigration - startTimeMigration;
            logger.debug("migration duration time msec: " + durationMsec);
            MigrationResult migrationResult = new MigrationResult(
                    countExtractedElements, countLoadedElements, durationMsec,
                    startTimeMigration, endTimeMigration);
            String message = "Migration was executed correctly.";
            return migrationResult;
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
            if (conFrom != null) {
                ExecutorService executorTerminator = null;
                try {
                    executorTerminator = Executors.newCachedThreadPool();
                    conFrom.abort(executorTerminator);
                } catch (SQLException ex) {
                    String messageRollbackConFrom = " Could not roll back "
                            + "transactions in the source database "
                            + "after failure in data migration: "
                            + ex.getMessage();
                    logger.error(messageRollbackConFrom);
                    message += messageRollbackConFrom;
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

    public static void main (String[] args) {
        LoggerSetup.setLogging();
        ConnectionInfo conInfoTo = new VerticaConnectionInfo("192.168.99.100",
                "5433", "docker", "dbadmin", "");
        ConnectionInfo conInfoFrom = new PostgreSQLConnectionInfo("localhost",
                "5432", "test", "pguser", "test");
        MigrationResult result;
        try {
            FromPostgresToVertica migrator = new FromPostgresToVertica(conInfoFrom,
                    "patients2", conInfoTo, "test.patients5");
            result = migrator.executeMigrationLocally();
            logger.debug("Number of extracted rows: "
                    + result.getCountExtractedElements()
                    + " Number of loaded rows: " + result.getCountLoadedElements());
        } catch (MigrationException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            //throw e;
        }
    }
}
