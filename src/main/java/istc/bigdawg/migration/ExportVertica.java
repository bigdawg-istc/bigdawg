package istc.bigdawg.migration;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.vertica.VerticaConnectionInfo;
import istc.bigdawg.vertica.VerticaHandler;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;

/**
 * @author kateyu
 * Used to copy data from Vertica. Currently only supports CSV export.
 */
public class ExportVertica implements Export {
    /** log */
    private static Logger log = Logger.getLogger(ExportVertica.class);

    private String outputFile;
    private Connection connection;
    private MigrationInfo migrationInfo;
    private DBHandler handlerTo;
    private transient OutputStream output;

    /**
     * @param connectionVertica the connection to Vertica to export from
     * @param handlerTo handler for the target engine for import
     */
    public ExportVertica(Connection connectionVertica, String outputFile, DBHandler handlerTo, MigrationInfo mi) {
        this.connection = connectionVertica;
        this.handlerTo = handlerTo;
        this.outputFile = outputFile;
        this.migrationInfo = mi;
    }

    @Override
    public void setMigrationInfo(MigrationInfo migrationInfo) { this.migrationInfo = migrationInfo; }

    @Override
    public Object call() throws MigrationException {
        log.debug("Starting export.");
        try {
            connection.setAutoCommit(false);
            connection.setReadOnly(true);

            log.debug("Creating fileWriter");
            FileWriter fw = new FileWriter(outputFile);

            log.debug("Exporting data.");

            log.debug("Generating copy command.");
            String query = "SELECT * FROM " + migrationInfo.getObjectFrom() + ";";
            log.debug("Query: " + query);
            Statement st = connection.createStatement();

            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int numCols = rsmd.getColumnCount();
            int numRows = 0;
            while (rs.next()) {
                for (int i = 1; i < numCols; i++) {
                    fw.append(rs.getString(i));
                    fw.append(FileFormat.getCsvDelimiter());
                }
                if (numCols > 0) {
                    numRows =+ 1;
                    fw.append(rs.getString(numCols));
                    fw.append("\n");
                }
            }
            fw.flush();
            fw.close();
            log.debug(numRows + " rows exported.");
        } catch (SQLException | IOException e) {
            String message = e.getMessage()
                    + "Something went wrong when writing table results to file.";
            log.error(message + StackTrace.getFullStackTrace(e), e);
            throw new MigrationException(message, e);
        }
        return null;
    }

    @Override
    public void setExportTo(String filePath) { this.outputFile = filePath; }

    @Override
    public DBHandler getHandler() throws MigrationException {
        return new VerticaHandler(migrationInfo.getConnectionFrom());
    }

    @Override
    public void setHandlerTo(DBHandler handlerto) throws MigrationException { this.handlerTo = handlerTo; }

    @Override
    public boolean isSupportedConnector(ConnectionInfo connection) {
        return (connection instanceof VerticaConnectionInfo);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ExportVertica [outputFile=" + outputFile + ", connection="
                + connection + ", migrationInfo=" + migrationInfo
                + ", handlerTo=" + handlerTo + "]";
    }

    public static void main(String[] args) {
        LoggerSetup.setLogging();
        Connection c;
        String host = "192.168.99.100";
        String user = "dbadmin";
        String password = "";
        String file = "/Users/kateyu/Research/bigdawgmiddle/tmp.txt";
        ConnectionInfo ci = new VerticaConnectionInfo(host, "5433", "docker", "dbadmin", "");
        System.out.println(ci.toString());
        DBHandler dbh = new PostgreSQLHandler();
        try {
            c = new VerticaHandler(ci).getConnection();
            MigrationInfo mi = new MigrationInfo(ci, "cust", null, "cust");
            ExportVertica export = new ExportVertica(c, file, dbh, mi);
            try {
                export.call();
            } catch (MigrationException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
