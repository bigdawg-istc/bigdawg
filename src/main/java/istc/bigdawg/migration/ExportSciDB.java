/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 4:39:08 PM
 */
public class ExportSciDB implements Export {

	/* log */
	private static Logger log = Logger.getLogger(LoadSciDB.class);

	/*
	 * Define the SciDB connection info.
	 */
	private ConnectionInfo connection;
	
	
	private final SciDBArrays arrays;

	/**
	 * The format in which data should be written to the file/pipe/output
	 * stream.
	 */
	private FileFormat fileFormat;

	/**
	 * The full path to the output file - where the data should be extracted to.
	 */
	private String outputFile = null;

	/**
	 * The full specification of the binary format e.g.: (int32, string, string)
	 */
	private String binFullFormat;

	/**
	 * Information about the migration process.
	 * 
	 * {@link #setMigrationInfo(MigrationInfo)}
	 */
	private MigrationInfo migrationInfo = null;

	public ExportSciDB(ConnectionInfo connection, SciDBArrays arrays,
			String scidbFilePath, FileFormat fileFormat, String binFullFormat) {
		this.connection = connection;
		this.arrays = arrays;
		this.outputFile = scidbFilePath;
		this.fileFormat = fileFormat;
		this.binFullFormat = binFullFormat;
	}

	/**
	 * Load the data to SciDB (identified by connectionTo): to a given array
	 * from a given file.
	 * 
	 * @param connectionTo
	 * @param arrays
	 * @param outputFile
	 * @return
	 * @throws SQLException
	 */
	public String call() throws MigrationException {
		StringBuilder saveCommand = new StringBuilder();
		String saveCommandFinal = null;
		if (fileFormat == FileFormat.CSV) {
			String csvFormat = "csv+";
			String array = null;
			if (arrays.getMultiDimensional() != null) {
				array = arrays.getMultiDimensional().getName();
			}
			/* this is only a flat array so export only the attributes */
			if (arrays.getFlat() != null) {
				/* for flat array we export only the attributes */
				csvFormat = "csv";
				array = arrays.getFlat().getName();
			}
			saveCommandFinal = "save(" + array + ",'" + outputFile + "',-2,'"
					+ csvFormat + "')";
		} else if (fileFormat == FileFormat.BIN_SCIDB) {
			String array = null;
			if (arrays.getMultiDimensional() != null) {
				String multiDimArray = arrays.getMultiDimensional().getName();
				String flatArray = arrays.getFlat().getName();
				array = "store(redimension(" + multiDimArray + "," + flatArray
						+ ")," + flatArray + ")";
			} else {
				/* only the flat array */
				array = arrays.getFlat().getName();
			}
			saveCommand.append("save(" + array + ", '" + outputFile + "'");
			saveCommand.append(",-2,'");

			saveCommand.append("(" + binFullFormat + ")");
			saveCommand.append("')");
			saveCommandFinal = saveCommand.toString();
		} else {

		}
		log.debug("save command: " + LogUtils.replace(saveCommandFinal));
		SciDBHandler handler;
		try {
			handler = new SciDBHandler(connection);
			handler.executeStatementAFL(saveCommandFinal);
			handler.commit();
			handler.close();
		} catch (SQLException e) {
			log.error(e.getMessage() + StackTrace.getFullStackTrace(e));
			throw new MigrationException(e.getMessage(), e);
		}
		log.debug("Data successfuly exported from SciDB");
		return null;
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
		if (connection instanceof SciDBConnectionInfo) {
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
	public DBHandler getHandler() throws MigrationException {
		try {
			return new SciDBHandler(migrationInfo.getConnectionFrom());
		} catch (SQLException e) {
			throw new MigrationException(
					e.getMessage() + " Cannot instantiate the SciDBHandler.");
		}
	}

}
