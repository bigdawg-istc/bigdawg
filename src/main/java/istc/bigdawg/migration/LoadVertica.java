package istc.bigdawg.migration;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.vertica.VerticaConnectionInfo;
import istc.bigdawg.vertica.VerticaHandler;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by kateyu on 3/7/17.
 */
public class LoadVertica implements Load {

    /** For internal logging in the class. */
    private static Logger log = Logger.getLogger(LoadVertica.class);

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
     * Create task for loading data to Vertica directly from a file (or named
     * pipe).
     *
     * @param connection
     *            Connection to a Vertica instance.
     * @param inputFile
     *            The name of the input file (or named pipe) from which the data
     *            should be loaded.
     * @throws SQLException
     *             If something was wrong when the copy manager for PostgreSQL
     *             was trying to connect to the database (for example, wrong
     *             encoding).
     */
    public LoadVertica(Connection connection, MigrationInfo migrationInfo, final String inputFile)
            throws SQLException {
        this.connection = connection;
        this.migrationInfo = migrationInfo;
        this.inputFile = inputFile;
    }

    /**
     * Copy data to Vertica.
     *
     * @return number of loaded rows
     *
     * @throws Exception
     */
    public Long call() throws Exception {
        log.debug("Start loading data to Vertica "
                + this.getClass().getCanonicalName() + ". ");
        try {
            String copyToString = "COPY " + migrationInfo.getObjectTo() + " FROM LOCAL '"
                    + inputFile + "' DELIMITER '" + FileFormat.getCsvDelimiter() + "'";
            log.debug("copy to string: " + copyToString);
            Statement st = connection.createStatement();
            st.execute(copyToString);
            connection.commit();
            // Vertica currently has no way of counting the number of loaded rows.
            return 0L;
        } catch (Exception e) {
            String msg = e.getMessage()
                    + " Problem when loading data to Vertica ";
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
        return (connection instanceof VerticaConnectionInfo);
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
        return new VerticaHandler(migrationInfo.getConnectionTo());
    }

    @Override
    /**
     * Close the connection to Vertica;
     */
    public void close() throws Exception {
        try {
            if (connection != null && !connection.isClosed()) {
                try {
                    connection.commit();
                } catch (SQLException e) {
                    log.info("Could not commit any sql statement for "
                            + "the connection in LoadVertica. "
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
        String host = "192.168.99.100";
        String file = "/Users/kateyu/Research/bigdawgmiddle/tmp.txt";
        try {
            ConnectionInfo ci = new VerticaConnectionInfo(host, "5433", "docker", "dbadmin", "");
            DBHandler dbh = new VerticaHandler();
            MigrationInfo mi = new MigrationInfo(ci, "cust", null, "cust2");
            c = new VerticaHandler(ci).getConnection();
            LoadVertica load = new LoadVertica(c, mi, file);
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
