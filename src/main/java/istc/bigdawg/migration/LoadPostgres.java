/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.StackTrace;

/**
 * This is run in a separate thread to copy data to PostgreSQL.
 * 
 * The input data to be loaded to PostgreSQL can be fetched either directly from
 * a file (create an instance of the LoadPostgres class providing the name of
 * the input file) or from an InputStream.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:07:05 PM
 */
public class LoadPostgres implements Load {

	/** For internal logging in the class. */
	private static Logger log = Logger.getLogger(LoadPostgres.class);

	/** Internally we keep the handler for the copy manager for PostgreSQL. */
	private CopyManager cpTo;

	/** SQL statement which represents the copy command. */
	private String copyToString;

	/** Input stream from which the data for loading can be read. */
	private InputStream input;

	/** The name of the input file from where the data should be loaded. */
	private String inputFile;

	/** Connection (physical not info) to an instance of PostgreSQL. */
	private Connection connection;

	/**
	 * Information about migration: connection information from/to database,
	 * object/table/array to export/load data from/to.
	 */
	private MigrationInfo migrationInfo;

	/** Handler to the database from which the data is exported. */
	private DBHandler fromHandler;

	/**
	 * Check: {@link #LoadPostgres(Connection, String, InputStream)}
	 * 
	 * Create task for loading data to PostgreSQL directly from a file (or named
	 * pipe).
	 * 
	 * @param connection
	 *            Connection to a PostgreSQL instance.
	 * @param copyToString
	 *            SQL statement which represent the copy command for PostgreSQL.
	 * @param inputFile
	 *            The name of the input file (or named pipe) from which the data
	 *            should be loaded.
	 * @throws SQLException
	 *             If something was wrong when the copy manager for PostgreSQL
	 *             was trying to connect to the database (for example, wrong
	 *             encoding).
	 */
	public LoadPostgres(Connection connection, final String copyToString,
			final String inputFile) throws SQLException {
		this.connection = connection;
		this.copyToString = copyToString;
		this.inputFile = inputFile;
		this.input = null;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Check: {@link #LoadPostgres(Connection, String, String)}
	 * 
	 * Create task for loading data to PostgreSQL directly from a file (or named
	 * pipe).
	 * 
	 * @param connection
	 *            Connection to a PostgreSQL instance.
	 * @param copyToString
	 *            SQL statement which represent the copy command for PostgreSQL.
	 * @param inputFile
	 *            The name of the input file (or named pipe) from which the data
	 *            should be loaded.
	 * @throws SQLException
	 *             If something was wrong when the copy manager for PostgreSQL
	 *             was trying to connect to the database (for example, wrong
	 *             encoding).
	 */
	public LoadPostgres(Connection connection, final String copyToString,
			InputStream input) throws SQLException {
		this.connection = connection;
		this.copyToString = copyToString;
		this.input = input;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Copy data to PostgreSQL.
	 * 
	 * @return number of loaded rows or -2 if there was any error during
	 *         execution
	 * @throws Exception
	 */
	public Long call() throws Exception {
		log.debug("Start loading data to PostgreSQL "
				+ this.getClass().getCanonicalName() + ". ");
		if (input == null) {
			try {
				input = new BufferedInputStream(new FileInputStream(inputFile));
			} catch (FileNotFoundException e) {
				String msg = e.getMessage()
						+ " Problem with thread for PostgreSQL copy manager "
						+ "while loading data from PostgreSQL.";
				log.error(msg + StackTrace.getFullStackTrace(e), e);
				throw e;
			}
		}
		Long countLoadedRows = 0L;
		try {
			countLoadedRows = cpTo.copyIn(copyToString, input);
			input.close();
			connection.commit();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for PostgreSQL copy manager "
					+ "while copying data to PostgreSQL.";
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			throw e;
		}
		return countLoadedRows;
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
		if (connection instanceof PostgreSQLConnectionInfo) {
			return true;
		}
		return false;
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
}
