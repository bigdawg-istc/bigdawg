package istc.bigdawg.migration;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.rest.RESTHandler;
import istc.bigdawg.utils.StackTrace;
import org.apache.log4j.Logger;
import java.io.*;

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

            StringBuffer resultBuffer = new StringBuffer(""); // @TODO - explore what to do if no migration params passed in
            if (migrationInfo.getMigrationParams().isPresent()) {
                MigrationParams migrationParams = migrationInfo.getMigrationParams().get();
                IntraIslandQuery source = migrationParams.getSource();
                if (source != null) {
                    QueryResult result = source.getQueryResult();
                    // there should be a better way to do this
                    String[] resultArr = result.toPrettyString().split("\n"); // @TODO - explore making this more efficient by not having to use result.toPrettyString()
                    for (String line: resultArr) {
                        resultBuffer.append("'");
                        resultBuffer.append(line.replaceAll("'", "''"));
                        resultBuffer.append("'\n");
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
