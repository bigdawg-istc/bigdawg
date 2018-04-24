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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by kateyu on 2/15/17.
 */
public class LoadMySQL implements Load {

    /** For internal logging in the class. */
    private static Logger log = Logger.getLogger(LoadMySQL.class);

    /** The name of the input file from where the data should be loaded. */
    private String inputFile;


    private boolean fromInputStream = false;

    /** Connection (physical not info) to an instance of PostgreSQL. */
    private transient Connection connection;

    /**
     * Information about migration: connection information from/to database,
     * object/table/array to export/load data from/to.
     */
    private MigrationInfo migrationInfo;

    /** Handler to the database from which the data is exported. */
    private transient DBHandler fromHandler;

    /** File format in which data should be loaded to PostgreSQL. */
    private FileFormat fileFormat;

    /**
     * Create task for loading data to MySQL directly from a file (or named
     * pipe).
     *
     * @param connection
     *            Connection to a MySQL instance.
     * @param inputFile
     *            The name of the input file (or named pipe) from which the data
     *            should be loaded.
     * @throws SQLException
     *             If something was wrong when the copy manager for PostgreSQL
     *             was trying to connect to the database (for example, wrong
     *             encoding).
     */
    public LoadMySQL(Connection connection, MigrationInfo migrationInfo, final String inputFile)
            throws SQLException {
        this.connection = connection;
        this.migrationInfo = migrationInfo;
        this.inputFile = inputFile;
    }

    /**
     * Copy data to MySQL.
     *
     * @return number of loaded rows
     *
     * @throws Exception
     */
    public Long call() throws Exception {
        log.debug("Start loading data to MySQL "
                + this.getClass().getCanonicalName() + ". ");
        try {
            String copyToString = "LOAD DATA LOCAL INFILE '" + inputFile
                    + "' INTO TABLE " + migrationInfo.getObjectTo()
                    + " FIELDS TERMINATED BY '" + FileFormat.getCsvDelimiter()
                    + "' OPTIONALLY ENCLOSED BY \"" + FileFormat.getQuoteCharacter()
                    + "\" LINES TERMINATED BY '\n'";
            log.debug("copy to string: " + copyToString);
            Statement st = connection.createStatement();
            st.executeQuery(copyToString);
            Statement countSt = connection.createStatement();

            // check how many rows were affected
            ResultSet rs = countSt.executeQuery("SELECT ROW_COUNT();");
            connection.commit();
            Long countLoadedRows = 0L;
            if (rs.next()) {
                countLoadedRows = rs.getLong(1);
            }
            log.debug(countLoadedRows + " rows loaded.");
            return countLoadedRows;
        } catch (Exception e) {
            String msg = e.getMessage()
                    + " Problem when loading data to MySQL ";
            log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * istc.bigdawg.migration.ConnectorChecker#isSupportedConnector(istc.bigdawg
     * .query.ConnectionInfo)
     */
    @Override
    public boolean isSupportedConnector(ConnectionInfo connection) {
       return (connection instanceof MySQLConnectionInfo);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * istc.bigdawg.migration.SetMigrationInfo#setMigrationInfo(istc.bigdawg.
     * migration.MigrationInfo)
     */
    @Override
    public void setMigrationInfo(MigrationInfo migrationInfo) {
        this.migrationInfo = migrationInfo;
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.migration.Load#setLoadFrom(java.lang.String)
     */
    @Override
    public void setLoadFrom(String filePath) {
        this.inputFile = filePath;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * istc.bigdawg.migration.Load#setHandlerFrom(istc.bigdawg.query.DBHandler)
     */
    @Override
    public void setHandlerFrom(DBHandler fromHandler) {
        this.fromHandler = fromHandler;
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.migration.Load#getHandler()
     */
    @Override
    public DBHandler getHandler() {
        return new PostgreSQLHandler(migrationInfo.getConnectionTo());
    }

    @Override
    /**
     * Close the connection to MySQL;
     */
    public void close() throws Exception {
        try {
            if (connection != null && !connection.isClosed()) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    log.info("Could not commit any sql statement for "
                            + "the connection in LoadMySQL. "
                            + e.getMessage());
                }
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            String message = "Could not close the connection to SciDB. "
                    + e.getMessage();
            throw new SQLException(message, e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see istc.bigdawg.migration.Load#getMigrationInfo()
     */
    @Override
    public MigrationInfo getMigrationInfo() {
        return migrationInfo;
    }

    public static void main(String args[]) {
        LoggerSetup.setLogging();
        Connection c;
        String url = "localhost:3306";
        String user = "mysqluser";
        String password = "test";
        String file = "/Users/kateyu/Research/bigdawgmiddle/tmp.txt";
        try {
            ConnectionInfo ci = new MySQLConnectionInfo("localhost", "3306", "test", "mysqluser", "test");
            DBHandler dbh = new MySQLHandler();
            MigrationInfo mi = new MigrationInfo(ci, "test", null, "patients2");
            c = new PostgreSQLHandler(ci).getConnection();
            LoadMySQL load = new LoadMySQL(c, mi, file);
            try {
                load.call();
            } catch (MigrationException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
