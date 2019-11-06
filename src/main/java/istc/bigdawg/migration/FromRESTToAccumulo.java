/**
 *
 */
package istc.bigdawg.migration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import istc.bigdawg.executor.RESTQueryResult;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.utils.Tuple;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;

/**
 *
 */

import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic, Matthew J. Mucklo
 */
public class FromRESTToAccumulo extends FromDatabaseToDatabase {

    /**
     * For transfer via network.
     */
    private static final long serialVersionUID = 1L;

    private static Logger logger = Logger
            .getLogger(FromRESTToAccumulo.class);

    private AccumuloInstance accInst = null;

    // parameters
    private long rowIdCounterForAccumuloFromREST = 0L;
    private static long ACCUMULO_BATCH_WRITER_MAX_MEMORY = 50 * 1024 * 1024L;
    private static int ACCUMULO_BATCH_WRITER_MAX_WRITE_THREADS = 4;
    private int accumuloBatchWriteSize = 1000;

    private AccumuloConnectionInfo conTo;

    public AccumuloInstance getAccumuloInstance() {
        return accInst;
    }

    public FromRESTToAccumulo() {
        logger.debug("Created migrator from REST to Accumulo");
    }

    public FromRESTToAccumulo(AccumuloInstance accInst) {
        this();
        this.accInst = accInst;
    }

    private BatchWriter getAccumuloBatchWriter(final String table)
            throws AccumuloException, AccumuloSecurityException,
            AccumuloBigDawgException, TableNotFoundException {
        BatchWriterConfig config = new BatchWriterConfig();
        // bytes available to batch-writer for buffering mutations
        config.setMaxMemory(ACCUMULO_BATCH_WRITER_MAX_MEMORY);
        config.setMaxWriteThreads(ACCUMULO_BATCH_WRITER_MAX_WRITE_THREADS);
        try {
            return accInst.getConnector().createBatchWriter(table,
                    config);
        } catch (TableNotFoundException e1) {
            e1.printStackTrace();
            throw e1;
        }
    }

    @Override
    public MigrationResult migrate(MigrationInfo migrationInfo)
            throws MigrationException {
        if (!(migrationInfo.getConnectionTo() instanceof AccumuloConnectionInfo
                && migrationInfo
                .getConnectionFrom() instanceof RESTConnectionInfo)) {
            return null;
        }
        conTo = (AccumuloConnectionInfo) migrationInfo.getConnectionTo();
        try {
            this.accInst = AccumuloInstance.getFullInstance(conTo);
            if (BigDawgConfigProperties.INSTANCE.isAccumuloDropDataSet()) {
                accInst.dropDataSetIfExists(migrationInfo.getObjectTo());
            }
            accInst.createTableIfNotExists(migrationInfo.getObjectTo());
        } catch (Exception e) {
            throw new MigrationException("Problem with Accumulo", e);
        }
        try {
            return fromRESTToAccumulo(migrationInfo);
        } catch (MigrationException e) {
            String msg = "Could not close the destination database connection.";
            logger.error(msg + StackTrace.getFullStackTrace(e), e);
            throw new MigrationException(msg, e);
        }
    }

    public MigrationResult fromRESTToAccumulo(MigrationInfo migrationInfo) throws MigrationException {
        logger.debug("Migrate data from REST to Accumulo.");
        final String accumuloTable = migrationInfo.getObjectTo();
        long startTimeMigration = System.currentTimeMillis();
        BatchWriter writer = null;
        long fullRowCounter = 0; /* Full counter of rows extracted/loaded. */
        try {
            try {
                writer = getAccumuloBatchWriter(accumuloTable);
            } catch (AccumuloException | AccumuloSecurityException
                    | AccumuloBigDawgException | TableNotFoundException exp) {
                String msg = "Could not open Accumulo BatchWriter.";
                logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
                throw new MigrationException(msg, exp);
            }
            if (migrationInfo.getMigrationParams().isPresent()) {
                MigrationParams migrationParams = migrationInfo.getMigrationParams().get();
                IntraIslandQuery source = migrationParams.getSource();
                if (source != null) {
                    RESTQueryResult restQueryResult = (RESTQueryResult) source.getQueryResult();
                    List<Map<String, Object>> rows = restQueryResult.getRowsWithHeadings();
                    List<Tuple.Tuple3<String, String, Boolean>> columns = restQueryResult.getColumns();
                    long counterLocal = 0;
                    if (rows != null) {
                        for (Map<String, Object> row : rows) {
                            ++rowIdCounterForAccumuloFromREST;
                            ++counterLocal;
                            Text rowId = new Text(Long.toString(rowIdCounterForAccumuloFromREST));
                            int colNum = 0;
                            for (Tuple.Tuple3<String, String, Boolean> column : columns) {
                                colNum++;
                                final String colName = column.getT1();
                                Mutation mutation = new Mutation(rowId);
                                /* colFamily, colQualifier, value */
                                Text colFamily = new Text(String.valueOf(colNum));
                                Text colQual = new Text(colName);
                                Object colVal = row.getOrDefault(colName, "");
                                if (colVal == null) {
                                    colVal = "";
                                }
                                Value value = new Value(
                                        colVal.toString().getBytes());
                                mutation.put(colFamily, colQual, value);
                                try {
                                    writer.addMutation(mutation);
                                } catch (MutationsRejectedException e) {
                                    String msg = "Mutation (new data) to Accumulo"
                                            + " was rejected with: " + "colFamily: "
                                            + colFamily + " colQualifier: " + colQual
                                            + " value:" + value;
                                    logger.error(msg + StackTrace.getFullStackTrace(e),
                                            e);
                                    throw new MigrationException(msg, e);
                                }
                            }
                            if (counterLocal % accumuloBatchWriteSize == 0) {
                                counterLocal = 0;
                                try {
                                    writer.flush();
                                    writer.close();
                                } catch (MutationsRejectedException exp) {
                                    String msg = "Could not close BatchWriter to Accumulo.";
                                    logger.error(
                                            msg + StackTrace.getFullStackTrace(exp),
                                            exp);
                                    throw new MigrationException(msg, exp);
                                }
                                try {
                                    writer = getAccumuloBatchWriter(accumuloTable);
                                } catch (AccumuloException | AccumuloSecurityException
                                        | AccumuloBigDawgException
                                        | TableNotFoundException exp) {
                                    String msg = "Could not open next Accumulo BatchWriter.";
                                    logger.error(
                                            msg + StackTrace.getFullStackTrace(exp),
                                            exp);
                                    throw new MigrationException(msg, exp);
                                }
                            }
                        }
                    }
                }
            }

            long endTimeMigration = System.currentTimeMillis();
            long durationMsec = endTimeMigration - startTimeMigration;
            return new MigrationResult(fullRowCounter, fullRowCounter,
                    startTimeMigration, endTimeMigration, durationMsec);
        } catch (Exception exp) {
            String msg = "Problem with access to REST result: "
                    + exp.getMessage();
            logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
            throw new MigrationException(msg, exp);
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (MutationsRejectedException exp) {
                    String msg = "Could not close BatchWriter to Accumulo.";
                    logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
                    throw new MigrationException(msg, exp);
                }
            }
        }
    }
}
