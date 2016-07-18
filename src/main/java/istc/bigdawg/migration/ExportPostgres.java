/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.StackTrace;

/**
 * This is run as a separate thread to copy data from PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 14, 2016 6:06:36 PM
 */
public class ExportPostgres implements Export {

	/** log */
	private static Logger log = Logger.getLogger(ExportPostgres.class);

	/** Copy manager for PostgreSQL - it implements the copy command. */
	private CopyManager cpFrom = null;

	/**
	 * The SQL statement that should be used to issue the copy command from
	 * PostgreSQL.
	 */
	private String copyFromString = null;

	/**
	 * The full path to the output file - where the data should be extracted to.
	 */
	private String outputFile = null;

	/**
	 * The handler to the output stream where the data should be extracted to.
	 */
	private OutputStream output = null;

	/**
	 * The format in which data should be written to the file/pipe/output
	 * stream.
	 */
	private FileFormat fileFormat = null;

	/**
	 * The connection to the instance of PostgreSQL from the data should be
	 * extracted.
	 */
	private Connection connectionFrom;

	/**
	 * Information about the migration process.
	 * 
	 * {@link #setMigrationInfo(MigrationInfo)}
	 */
	private MigrationInfo migrationInfo = null;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ExportPostgres [cpFrom=" + cpFrom + ", copyFromString="
				+ copyFromString + ", outputFile=" + outputFile + ", output="
				+ output + ", fileFormat=" + fileFormat + ", connectionFrom="
				+ connectionFrom + ", migrationInfo=" + migrationInfo
				+ ", handlerTo=" + handlerTo + "]";
	}

	/**
	 * Handler to the database to which we load the data.
	 */
	private DBHandler handlerTo;

	/**
	 * Declare only the file format in which the data should be exported. The
	 * remaining parameters should be added when the migration is prepared.
	 * 
	 * @param fileFormat
	 *            File format in which the data should be exported.
	 */
	private ExportPostgres(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	/**
	 * see: {@link #ExportPostgres(FileFormat)}
	 * 
	 * @param fileFormat
	 * @return Instance of ExportPostgres which will export data in the
	 *         fileFormat.
	 */
	public static ExportPostgres ofFormat(FileFormat fileFormat) {
		return new ExportPostgres(fileFormat);
	}

	public ExportPostgres(Connection connectionPostgreSQL,
			final String copyFromString, OutputStream output,
			DBHandler handlerTo) throws SQLException {
		this.connectionFrom = connectionPostgreSQL;
		this.copyFromString = copyFromString;
		this.output = output;
		this.cpFrom = new CopyManager((BaseConnection) connectionFrom);
		this.handlerTo = handlerTo;
	}

	public ExportPostgres(ConnectionInfo connectionPostgreSQL,
			final String copyFromString, final String outputFile,
			DBHandler handlerTo) throws SQLException {
		connectionFrom = PostgreSQLHandler.getConnection(connectionPostgreSQL);
		connectionFrom.setAutoCommit(false);
		connectionFrom.setReadOnly(true);
		this.copyFromString = copyFromString;
		this.outputFile = outputFile;
		this.cpFrom = new CopyManager((BaseConnection) connectionFrom);
		this.handlerTo = handlerTo;
	}

	/**
	 * Initialize the required objects for the migration.
	 * 
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws MigrationException
	 */
	private void lazyInitialization() throws MigrationException {
		log.debug("Lazy initialization.");

		log.debug("Establish connection.");
		if (connectionFrom == null) {
			try {
				connectionFrom = PostgreSQLHandler
						.getConnection((PostgreSQLConnectionInfo) migrationInfo
								.getConnectionFrom());
				connectionFrom.setAutoCommit(false);
				connectionFrom.setReadOnly(true);
				this.cpFrom = new CopyManager((BaseConnection) connectionFrom);
			} catch (SQLException e) {
				String msg = "Problem with connection to PostgreSQL. "
						+ e.getMessage();
				log.error(msg + " " + StackTrace.getFullStackTrace(e));
				throw new MigrationException(msg, e);
			}
		}
		log.debug("Create the output stream.");
		if (output == null) {
			try {
				if (outputFile == null) {
					String msg = "The output file is unknown for ExportPostgres.!";
					log.error(msg);
					throw new IllegalStateException(msg);
				}
				output = new BufferedOutputStream(new FileOutputStream(
						outputFile));
			} catch (FileNotFoundException e) {
				String msg = "File not found: " + outputFile + " "
						+ e.getMessage()
						+ " Problem with thread for PostgreSQL copy manager "
						+ "while copying (extracting) data from PostgreSQL.";
				log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
				throw new MigrationException(msg, e);
			}
		}
		log.debug("Specify the copy from command for PostgreSQL.");
		if (copyFromString == null) {
			if (fileFormat == FileFormat.CSV) {
				copyFromString = PostgreSQLHandler.getExportCsvCommand(
						migrationInfo.getObjectFrom(),
						FileFormat.getCsvDelimiter(),
						FileFormat.getQuoteCharacter(),
						handlerTo.isCsvLoadHeader());
			} else if (fileFormat == FileFormat.BIN_POSTGRES) {
				copyFromString = PostgreSQLHandler
						.getExportBinCommand(migrationInfo.getObjectFrom());
			} else {
				String msg = "Usupported type: " + fileFormat;
				log.error(msg);
				throw new IllegalArgumentException(msg);
			}
		}
		if (handlerTo == null) {
			throw new IllegalStateException(
					"The handler (for database to which we load the data) "
							+ "was not initialized.");
		}
	}

	/**
	 * Copy data from PostgreSQL.
	 * 
	 * @return number of extracted rows
	 * @throws Exception
	 */
	public Long call() throws MigrationException {
		log.debug("start call: Copy from PostgreSQL (Executor)");
		lazyInitialization();
		Long countExtractedRows = 0L;
		log.debug(
				"issue command to PostgreSQL: Copy from PostgreSQL (Executor)");
		try {
			log.debug("psql copy statement: " + copyFromString);
			countExtractedRows = cpFrom.copyOut(copyFromString, output);
			// PostgreSQLHandler.executeStatement(connection, copyFromString);
			connectionFrom.commit();
			output.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for PostgreSQL copy manager "
					+ "while copying (extracting) data from PostgreSQL.";
			log.error(msg + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		}
		log.debug("Extracted rows: " + countExtractedRows);
		return countExtractedRows;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.migration.Export#isSupportedConnector(istc.bigdawg.query.
	 * ConnectionInfo)
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

	/**
	 * @return the migrationInfo
	 */
	public MigrationInfo getMigrationInfo() {
		return migrationInfo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.Export#setExportTo(java.lang.String)
	 */
	@Override
	public void setExportTo(String filePath) {
		this.outputFile = filePath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.Export#getHandler()
	 */
	@Override
	public DBHandler getHandler() {
		return new PostgreSQLHandler(migrationInfo.getConnectionFrom());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.migration.Export#setHandlerTo(istc.bigdawg.query.DBHandler)
	 */
	@Override
	public void setHandlerTo(DBHandler handlerTo) throws MigrationException {
		this.handlerTo = handlerTo;
	}
}