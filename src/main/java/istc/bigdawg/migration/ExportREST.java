package istc.bigdawg.migration;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.executor.RESTQueryResult;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.rest.RESTHandler;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.Tuple;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class ExportREST implements Export {

    private static Logger log = Logger.getLogger(ExportREST.class);

    private String outputFile;
    private MigrationInfo migrationInfo;
    private DBHandler handlerTo;
    private transient OutputStream output;

    public ExportREST() {

    }

    /**
     * @param handlerTo handler for the target engine for import
     */
    public ExportREST(OutputStream output, DBHandler handlerTo, MigrationInfo mi) {
        this.handlerTo = handlerTo;
        this.output = output;
        this.migrationInfo = mi;
    }

    @Override
    public void setMigrationInfo(MigrationInfo migrationInfo) { this.migrationInfo = migrationInfo; }

    @Override
    public Object call() throws MigrationException {
        log.debug("Starting export.");
        try {
            OutputStream outputStream = output;
            if (output == null) {
                outputStream = new FileOutputStream(outputFile);
            }

            log.debug("Exporting data.");

            StringBuilder resultBuffer = new StringBuilder(""); // @TODO - explore what to do if no migration params passed in
            if (migrationInfo.getMigrationParams().isPresent()) {
                MigrationParams migrationParams = migrationInfo.getMigrationParams().get();
                IntraIslandQuery source = migrationParams.getSource();
                if (source != null) {
                    RESTQueryResult restQueryResult = (RESTQueryResult) source.getQueryResult();
                    List<Map<String, Object>> rows = restQueryResult.getRowsWithHeadings();
                    List<Tuple.Tuple3<String, String, Boolean>> columns = restQueryResult.getColumns();
                    if (rows != null) {
                        for(Map<String, Object> row: rows) {
                            StringJoiner stringJoiner = new StringJoiner(",");
                            for (Tuple.Tuple3<String, String, Boolean> column : columns) {
                                final String name = column.getT1();
                                final String type = column.getT2();
                                final Object value = row.getOrDefault(name, null);
                                if (value == null) {
                                    stringJoiner.add("null");
                                } else {
                                    Class objectClass = value.getClass();
                                    if (objectClass == JSONObject.class ||
                                            objectClass == JSONArray.class) {
                                        stringJoiner.add("'" + value.toString().replaceAll("'", "''") + "'");
                                    } else if (objectClass == String.class) {
                                        if (type.equals("json")) {
                                            stringJoiner.add("'\"" + value.toString().replaceAll("'", "''") + "\"'");
                                        }
                                        else {
                                            stringJoiner.add("'" + value.toString().replaceAll("'", "''") + "'");
                                        }
                                    } else if (type.equals("json")) {
                                        stringJoiner.add("'" + value.toString().replaceAll("'", "''") + "'");
                                    }
                                    else {
                                        stringJoiner.add(value.toString());
                                    }
                                }
                            }
                            resultBuffer.append(stringJoiner.toString());
                            resultBuffer.append("\n");
                        }
                    }
                }
            }

//            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(output);
            Writer out = new OutputStreamWriter(outputStream, "UTF8");

            out.write(resultBuffer.toString());
            out.flush();
            if (this.output == null) { // this stream was created just for this time
                out.close();
            }
//            DataOutputStream dataOutputStream = new DataOutputStream(bufferedOutputStream);
//            dataOutputStream.writeBytes(resultStr);
//            dataOutputStream.flush();
//            dataOutputStream.close();
//            log.debug(numRows + " rows exported.");
        } catch (IOException e) {
            String message = e.getMessage()
                    + "Something went wrong when writing REST results.";
            log.error(message + StackTrace.getFullStackTrace(e), e);
            throw new MigrationException(message, e);
        }
        return null;
    }

    @Override
    public void setExportTo(String filePath) { this.outputFile = filePath; }

    @Override
    public DBHandler getHandler() throws MigrationException {
        if (!(migrationInfo.getConnectionFrom() instanceof RESTConnectionInfo)) {
            throw new MigrationException("Wrong ConnectionInfo, should be RESTConnectionInfo, instead got: " + migrationInfo.getConnectionFrom().getClass());
        }
        return new RESTHandler((RESTConnectionInfo) migrationInfo.getConnectionFrom());
    }

    @Override
    public void setHandlerTo(DBHandler handlerto) throws MigrationException { this.handlerTo = handlerTo; }

    @Override
    public boolean isSupportedConnector(ConnectionInfo connection) {
        return (connection instanceof RESTConnectionInfo);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ExportREST [outputFile=" + outputFile + ", connection="
                + ", migrationInfo=" + migrationInfo
                + ", handlerTo=" + handlerTo + "]";
    }

}
