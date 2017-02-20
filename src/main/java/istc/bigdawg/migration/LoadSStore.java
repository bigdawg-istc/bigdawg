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
import java.util.List;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * This is run in a separate thread to copy data to PostgreSQL.
 * 
 * The input data to be loaded to PostgreSQL can be fetched either directly from
 * a file (create an instance of the LoadPostgres class providing the name of
 * the input file) or from an InputStream.
 * 
 * @author 
 * 
 *         
 */
public class LoadSStore implements Load {

	/**
	 * Determines if a de-serialized file is compatible with this class.
	 */
	private static final long serialVersionUID = -8743091780174837738L;
												 

	/** For internal logging in the class. */
	private static Logger log = Logger.getLogger(LoadSStore.class);

	/** Internally we keep the handler for the copy manager for S-Store. */
	private transient CopyManager cpTo;

	/** SQL statement which represents the copy command. */
	private String copyToString;

	/** Input stream from which the data for loading can be read. */
	private transient InputStream input;

	/** The name of the input file from where the data should be loaded. */
	private String inputFile;

	/** Connection (physical not info) to an instance of S-Store. */
	private transient Connection connection;

	/**
	 * Information about migration: connection information from/to database,
	 * object/table/array to export/load data from/to.
	 */
	private MigrationInfo migrationInfo;

	/** Handler to the database from which the data is exported. */
	private transient DBHandler fromHandler;

	/** File format in which data should be loaded to S-Store. */
	private FileFormat fileFormat;
	
	private String trim;
	private boolean caching;	// Whether caching in S-Store is turned on
	private String serverAddress; // Communicate between S-Store and the other DB for rollback/commit of S-Store
	private int port; // Communicate between S-Store and the other DB for rollback/commit of S-Store
	
	public LoadSStore() {
		
	}

	/**
	 * Declare only the file format in which the data should be loaded. The
	 * remaining parameters should be added when the migration is prepared.
	 * 
	 * @param fileFormat
	 *            File format in which the data should be loaded.
	 */
	private LoadSStore(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	/**
	 * see: {@link #ExportSStore(FileFormat)}
	 * 
	 * @param fileFormat
	 * @return Instance of LoadSStore which will load data in the specified
	 *         fileFormat.
	 */
	public static LoadSStore ofFormat(FileFormat fileFormat) {
		return new LoadSStore(fileFormat);
	}

	public void setAdditionalParams(String trim, Boolean caching, String serverAddress, Integer port) {
		this.trim = trim;
		this.caching = caching;
		this.serverAddress = serverAddress;
		this.port = port;
	}
	
	/**
	 * Check: {@link #LoadPostgres(Connection, String, InputStream)}
	 * 
	 * Create task for loading data to S-Store directly from a file (or named
	 * pipe).
	 * 
	 * @param connection
	 *            Connection to a S-Store instance.
	 * @param copyToString
	 *            SQL statement which represent the copy command for S-Store.
	 * @param inputFile
	 *            The name of the input file (or named pipe) from which the data
	 *            should be loaded.
	 * @throws SQLException
	 *             If something was wrong when the copy manager for S-Store
	 *             was trying to connect to the database (for example, wrong
	 *             encoding).
	 */
	public LoadSStore(Connection connection, MigrationInfo migrationInfo,
			final String copyToString, final String inputFile)
					throws SQLException {
		this.connection = connection;
		this.migrationInfo = migrationInfo;
		this.copyToString = copyToString;
		this.inputFile = inputFile;
		this.input = null;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Check: {@link #LoadPostgres(Connection, String, String)}
	 * 
	 * Create task for loading data to S-Store directly from a file (or named
	 * pipe).
	 * 
	 * @param connection
	 *            Connection to a S-Store instance.
	 * @param copyToString
	 *            SQL statement which represent the copy command for S-Store.
	 * @param inputFile
	 *            The name of the input file (or named pipe) from which the data
	 *            should be loaded.
	 * @throws SQLException
	 *             If something was wrong when the copy manager for S-Store
	 *             was trying to connect to the database (for example, wrong
	 *             encoding).
	 */
	public LoadSStore(Connection connection, MigrationInfo migrationInfo,
			final String copyToString, InputStream input) throws SQLException {
		this.connection = connection;
		this.migrationInfo = migrationInfo;
		this.copyToString = copyToString;
		this.input = input;
		this.cpTo = new CopyManager((BaseConnection) connection);
	}

	/**
	 * Initialize the required objects for the migration.
	 * 
	 * @throws MigrationException
	 */
	private void lazyInitialization() throws MigrationException {
		if (input == null) {
			try {
				input = new BufferedInputStream(new FileInputStream(inputFile));
			} catch (FileNotFoundException e) {
				String msg = e.getMessage()
						+ " Problem with thread for PostgreSQL copy manager "
						+ "while loading data from PostgreSQL.";
				log.error(msg + StackTrace.getFullStackTrace(e), e);
				throw new MigrationException(msg, e);
			}
		}
		if (copyToString == null) {
			if (fileFormat == FileFormat.CSV) {
				copyToString = SStoreSQLHandler.getImportCommand();
//			} else if (fileFormat == FileFormat.BIN_POSTGRES) {
//				copyToString = SStoreSQLHandler
//						.getLoadBinCommand(migrationInfo.getObjectTo());
			} else {
				String msg = "Usupported type: " + fileFormat;
				log.error(msg);
				throw new IllegalArgumentException(msg);
			}
		}
		if (connection == null) {
			try {
				connection = SStoreSQLHandler
						.getConnection(migrationInfo.getConnectionTo());
			} catch (SQLException e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
//		if (cpTo == null) {
//			try {
//				this.cpTo = new CopyManager((BaseConnection) connection);
//			} catch (SQLException e) {
//				throw new MigrationException(e.getMessage(), e);
//			}
//		}
	}

	/**
	 * Copy data to S-Store.
	 * 
	 * @return number of loaded rows
	 * 
	 * @throws Exception
	 */
	public Long call() throws Exception {
		log.debug("Start loading data to S-Store "
				+ this.getClass().getCanonicalName() + ". ");
		lazyInitialization();
		Long countLoadedRows = -1L;
		try {
			countLoadedRows = 
					SStoreSQLHandler.executePreparedImportStatement(
							connection, copyToString, migrationInfo.getObjectTo(), 
							input, "csv", inputFile,
							serverAddress, port);
			input.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for S-Store copy manager "
					+ "while copying data to S-Store. ";
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

	public void setFileFormat(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
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
	 * Close the connection to S-Store.
	 */
	public void close() throws Exception {
		try {
			if (connection != null && !connection.isClosed()) {
//				try {
//					connection.commit();
//				} catch (SQLException e) {
//					log.info("Could not commit any sql statement for "
//							+ "the connection in LoadPostgres. "
//							+ e.getMessage());
//				}
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
}
