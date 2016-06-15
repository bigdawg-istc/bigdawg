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

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
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
	 * The format in which data should be written to the file/pipe/output strea.
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

	/**
	 * Declare only the file format in which the data should be exported. The
	 * remaining parameters should be added when the migration is prepared.
	 * 
	 * @param fileFormat
	 *            File format in which the data should be exported.
	 */
	public ExportPostgres(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	/**
	 * see: {@link #ExportPostgres(FileFormat)}
	 * 
	 * @param fileFormat
	 * @return Instance of ExportPostgres which will export data in the
	 *         fileFormat.
	 */
	public static ExportPostgres exportFormat(FileFormat fileFormat) {
		return new ExportPostgres(fileFormat);
	}

	public ExportPostgres(Connection connectionPostgreSQL,
			final String copyFromString, OutputStream output)
					throws SQLException {
		this.connectionFrom = connectionPostgreSQL;
		this.copyFromString = copyFromString;
		this.output = output;
		this.cpFrom = new CopyManager((BaseConnection) connectionFrom);
	}

	public ExportPostgres(PostgreSQLConnectionInfo connectionPostgreSQL,
			final String copyFromString, final String outputFile)
					throws SQLException {
		connectionFrom = PostgreSQLHandler.getConnection(connectionPostgreSQL);
		connectionFrom.setAutoCommit(false);
		connectionFrom.setReadOnly(true);
		this.copyFromString = copyFromString;
		this.outputFile = outputFile;
		this.cpFrom = new CopyManager((BaseConnection) connectionFrom);
	}

	/**
	 * Initalize the required objects for the migration.  
	 * 
	 * @throws SQLException
	 * @throws FileNotFoundException
	 */
	private void lazyInitialization()
			throws SQLException, FileNotFoundException {
		if (connectionFrom == null) {
			connectionFrom = PostgreSQLHandler
					.getConnection((PostgreSQLConnectionInfo) migrationInfo
							.getConnectionFrom());
		}
		if (output == null) {
			try {
				if (outputFile == null) {
					String msg = "The output file is unknown for ExportPostgres.!";
					log.error(msg);
					throw new IllegalStateException(msg);
				}
				output = new BufferedOutputStream(
						new FileOutputStream(outputFile));
			} catch (FileNotFoundException e) {
				String msg = e.getMessage()
						+ " Problem with thread for PostgreSQL copy manager "
						+ "while copying (extracting) data from PostgreSQL.";
				log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
				throw e;
			}
		}
		if (copyFromString == null) {
			if (fileFormat == FileFormat.CSV) {
				copyFromString = 
			} else if (fileFormat == FileFormat.BIN_POSTGRES) {

			} else {
				String msg = "Usupported type: " + fileFormat;
				log.error(msg);
				throw new IllegalArgumentException(msg);
			}
		}
	}

	/**
	 * Copy data from PostgreSQL.
	 * 
	 * @return number of extracted rows
	 * @throws Exception
	 */
	public Long call() throws Exception {
		log.debug("start call: Copy from PostgreSQL (Executor)");
		lazyInitialization();
		Long countExtractedRows = 0L;
		log.debug(
				"issue command to PostgreSQL: Copy from PostgreSQL (Executor)");
		try {
			countExtractedRows = cpFrom.copyOut(copyFromString, output);
			// log.debug("psql statement: " + copyFromString);
			// PostgreSQLHandler.executeStatement(connection, copyFromString);
			connectionFrom.commit();
			output.close();
		} catch (IOException | SQLException e) {
			String msg = e.getMessage()
					+ " Problem with thread for PostgreSQL copy manager "
					+ "while copying (extracting) data from PostgreSQL.";
			log.error(msg + StackTrace.getFullStackTrace(e), e);
			throw e;
		}
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

	/**
	 * @return the fileFormat in which the data is exported
	 */
	public FileFormat getFileFormat() {
		return fileFormat;
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
		outputFile = filePath;
	}
}