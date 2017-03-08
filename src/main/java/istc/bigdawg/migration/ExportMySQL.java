package istc.bigdawg.migration;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.mysql.MySQLConnectionInfo;
import istc.bigdawg.mysql.MySQLHandler;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.StackTrace;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;

/**
 * @author kateyu
 * Used to copy data from MySQL. Currently only supports CSV export.
 */
public class ExportMySQL implements Export {

    /** log */
    private static Logger log = Logger.getLogger(ExportMySQL.class);

    private String outputFile;
    private Connection connection;
    private String copyFromString;
    private MigrationInfo migrationInfo;
    private DBHandler handlerTo;
    private transient OutputStream output;

    /**
     * @param connectionMySQL the connection to MySQL to export from
     * @param handlerTo handler for the target engine for import
     */
    //public ExportMySQL(Connection connectionMySQL, String outputFile, DBHandler handlerTo) {
    public ExportMySQL(Connection connectionMySQL, String outputFile, DBHandler handlerTo, MigrationInfo mi) {
        this.connection = connectionMySQL;
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
            Statement st = connection.createStatement();

            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int numCols = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i < numCols; i++) {
                    fw.append(rs.getString(i));
                    fw.append(FileFormat.getCsvDelimiter());
                }
                if (numCols > 0) {
                    fw.append(rs.getString(numCols));
                    fw.append("\n");
                }
            }
            fw.flush();
            fw.close();
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
        return new MySQLHandler(migrationInfo.getConnectionFrom());
    }

    @Override
    public void setHandlerTo(DBHandler handlerto) throws MigrationException { this.handlerTo = handlerTo; }

    @Override
    public boolean isSupportedConnector(ConnectionInfo connection) {
        return (connection instanceof MySQLConnectionInfo);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ExportMySQL [copyFromString="
                + copyFromString + ", outputFile=" + outputFile + ", connection="
                + connection + ", migrationInfo=" + migrationInfo
                + ", handlerTo=" + handlerTo + "]";
    }

    public static void main(String[] args) {
        LoggerSetup.setLogging();
        Connection c;
        String url = "localhost:3306";
        String user = "mysqluser";
        String password = "test";
        String file = "/Users/kateyu/Research/bigdawgmiddle/tmp.txt";
        ConnectionInfo ci = new MySQLConnectionInfo("localhost", "3306", "test", "mysqluser", "test");
        DBHandler dbh = new PostgreSQLHandler();
        try {
            c = new PostgreSQLHandler(ci).getConnection();
            MigrationInfo mi = new MigrationInfo(ci, "test", null, "test");
            ExportMySQL export = new ExportMySQL(c, file, dbh, mi);
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
