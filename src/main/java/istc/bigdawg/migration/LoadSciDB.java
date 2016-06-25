/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 4:39:08 PM
 * 
 *         Load data to the SciDB database.
 */
public class LoadSciDB implements Load {

	/* log */
	private static Logger log = Logger.getLogger(LoadSciDB.class);

	/*
	 * SciDB connection info - to which database we want to load the data.
	 * Information about the migration process.
	 * 
	 * {@link #setMigrationInfo(MigrationInfo)}
	 */
	private MigrationInfo migrationInfo = null;

	private SciDBArrays arrays;

	/** Path to the file from which the data should be loaded. */
	private String scidbFilePath;

	/**
	 * The load operator uses the format string as a guide for interpreting the
	 * contents of the binary file.
	 */
	private String binaryFormatString = null;

	/**
	 * The format in which data should be written to the file/pipe/output
	 * stream.
	 */
	private FileFormat fileFormat;

	/**
	 * Declare only the file format in which the data should be exported. The
	 * remaining parameters should be added when the migration is prepared.
	 * 
	 * @param fileFormat
	 *            File format in which the data should be exported.
	 */
	private LoadSciDB(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	/**
	 * see: {@link #ExportPostgres(FileFormat)}
	 * 
	 * @param fileFormat
	 * @return Instance of ExportPostgres which will export data in the
	 *         fileFormat.
	 */
	public static LoadSciDB ofFormat(FileFormat fileFormat) {
		return new LoadSciDB(fileFormat);
	}

	public LoadSciDB(ConnectionInfo connectionTo, SciDBArrays arrays,
			String scidbFilePath) {
		this.migrationInfo = MigrationInfo.forConnectionTo(connectionTo);
		this.arrays = arrays;
		this.scidbFilePath = scidbFilePath;
		/* declare the default file format - native SciDB format. */
		this.fileFormat = FileFormat.SCIDB_TEXT_FORMAT;
	}

	/**
	 * @param connectionTo
	 * @param arrays
	 * @param scidbFilePath
	 */
	public LoadSciDB(ConnectionInfo connectionTo, SciDBArrays arrays,
			String scidbFilePath, String binaryFormat) {

		this.migrationInfo = MigrationInfo.forConnectionTo(connectionTo);
		this.arrays = arrays;
		this.scidbFilePath = scidbFilePath;
		this.binaryFormatString = binaryFormat;
		if (this.binaryFormatString != null) {
			this.fileFormat = FileFormat.BIN_SCIDB;
		}
	}

	/**
	 * Get the Csv format String for CSV loading to SciDB.
	 * 
	 * http://www.paradigm4.com/HTMLmanual/14.12/scidb_ug/re30.html
	 */
	public static String getSciDBCsvString() {
		StringBuilder csvString = new StringBuilder();
		csvString.append("'");
		csvString.append("CSV");
		csvString.append(":");
		final String delimiter = FileFormat.getCsvDelimiter();
		switch (delimiter) {
		case "|":
			csvString.append("p");
			break;
		case ",":
			csvString.append("c");
			break;
		case "\t":
			csvString.append("t");
			break;
		default:
			String msg = "SciDB does not support the CSV delimiter: "
					+ delimiter + " It only supports:"
					+ " | (vertical pipe) , (comma) and tab. ";
			log.error(msg);
			throw new IllegalStateException(msg);
		}
		final String quoteCharacter = FileFormat.getQuoteCharacter();
		switch (quoteCharacter) {
		case "\"":
			csvString.append("d");
			break;
		case "'":
			csvString.append("s");
			break;
		default:
			String msg = "SciDB does not support the quote: " + quoteCharacter
					+ " It only supports:" + " \" and '.";
			log.error(msg);
			throw new IllegalStateException(msg);
		}
		csvString.append("'");
		return csvString.toString();
	}

	/**
	 * 
	 * @return binary format string required for binary loading to SciDB:
	 *         http://www.paradigm4.com/HTMLmanual/14.12/scidb_ug/re30.html
	 * @throws UnsupportedTypeException
	 * @throws SQLException
	 */
	public String getBinaryFormatString()
			throws UnsupportedTypeException, SQLException {
		String binaryFormatString = null;
		if (migrationInfo
				.getConnectionFrom() instanceof PostgreSQLConnectionInfo) {
			binaryFormatString = MigrationUtils
					.getSciDBBinFormat(new PostgreSQLHandler(
							(PostgreSQLConnectionInfo) migrationInfo
									.getConnectionFrom()).getObjectMetaData(
											migrationInfo.getObjectFrom()));
			return binaryFormatString;
		} else {
			throw new IllegalArgumentException(
					"Could not infer binary format string for SciDB loading.");
		}
	}

	/**
	 * Load the data to SciDB (identified by connectionTo): to a given array
	 * from a given file.
	 * 
	 * @return Information about the process.
	 * @throws Exception
	 */
	public Object call() throws Exception {
		/*
		 * we have to create a flat array and re-dimension it to the final
		 * result
		 */
		/*
		 * AFL% store(re-dimension(load(test_waveform_flat,'/home/adam/data/
		 * waveform_test.scidb'),test_waveform_),test_waveform_);
		 */

		/* remove the auxiliary flat array if the target was not flat */

		// InputStream resultInStream =
		// RunShell.executeAQLcommandSciDB(conTo.getHost(), conTo.getPort(),
		// conTo.getBinPath(), "load " + arrayTo + " from '" + dataFile + "'");
		// String resultString = IOUtils.toString(resultInStream,
		// Constants.ENCODING);
		// log.debug("Load data to SciDB: " + resultString);
		SciDBHandler handler = new SciDBHandler(
				migrationInfo.getConnectionTo());

		ObjectMetaData fromObjectMetaData = new PostgreSQLHandler(
				migrationInfo.getConnectionFrom())
						.getObjectMetaData(migrationInfo.getObjectFrom());

		arrays = MigrationUtils.prepareFlatTargetArrays(migrationInfo,
				fromObjectMetaData);
		StringBuilder loadCommand = new StringBuilder(
				"load(" + arrays.getFlat() + ", '" + scidbFilePath + "'");

		// StringBuilder loadCommand = new StringBuilder("load("
		// + migrationInfo.getObjectTo() + ", '" + scidbFilePath + "'");
		if (this.fileFormat != FileFormat.SCIDB_TEXT_FORMAT) {
			/*
			 * -2: Load all data using the coordinator instance of the query.
			 * This is the default.
			 */
			loadCommand.append(",-2,");
			if (this.fileFormat == FileFormat.BIN_SCIDB) {
				if (binaryFormatString == null) {
					/*
					 * We have to construct the binary format string for SciDB.
					 */
					binaryFormatString = getBinaryFormatString();
				} else {
					throw new IllegalStateException(
							"Could not create the binary format string for SciDB. "
									+ "Check the supported data types and data formats.");
				}
				loadCommand.append("'(" + binaryFormatString + ")'");
			} else if (this.fileFormat == FileFormat.CSV) {
				loadCommand.append(getSciDBCsvString());
			}
		}
		loadCommand.append(")");
		String finalLoadCommand = loadCommand.toString();
		if (arrays != null && arrays.getMultiDimensional() != null) {
			finalLoadCommand = "store(redimension(" + finalLoadCommand + ","
					+ arrays.getMultiDimensional() + "),"
					+ arrays.getMultiDimensional() + ")";
		}
		log.debug("load command: " + finalLoadCommand);
		handler.executeStatementAFL(finalLoadCommand);
		handler.commit();
		handler.close();
		/*
		 * SciDB does not provide us with the number of loaded elements (or
		 * cells/rows) and doing count on a big amount of data can take long
		 * time.
		 */
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

	/**
	 * @return the migrationInfo
	 */
	public MigrationInfo getMigrationInfo() {
		return migrationInfo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.Load#setLoadFrom(java.lang.String)
	 */
	@Override
	public void setLoadFrom(String filePath) {
		this.scidbFilePath = filePath;
	}

}
