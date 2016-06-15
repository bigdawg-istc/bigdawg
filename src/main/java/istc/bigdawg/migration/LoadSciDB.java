/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;

import org.apache.log4j.Logger;

import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.LogUtils;

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

	/* SciDB connection info - to which database we want to load the data. */
	private SciDBConnectionInfo connectionTo;
	private final SciDBArrays arrays;
	private String scidbFilePath;
	private String binaryFormat = null;

	/**
	 * Information about the migration process.
	 * 
	 * {@link #setMigrationInfo(MigrationInfo)}
	 */
	private MigrationInfo migrationInfo = null;

	public LoadSciDB(SciDBConnectionInfo connectionTo, SciDBArrays arrays,
			String scidbFilePath) {
		this.connectionTo = connectionTo;
		this.arrays = arrays;
		this.scidbFilePath = scidbFilePath;
	}

	/**
	 * @param connectionTo
	 * @param arrays
	 * @param scidbFilePath
	 */
	public LoadSciDB(SciDBConnectionInfo connectionTo, SciDBArrays arrays,
			String scidbFilePath, String binaryFormat) {
		this.connectionTo = connectionTo;
		this.arrays = arrays;
		this.scidbFilePath = scidbFilePath;
		this.binaryFormat = binaryFormat;
	}

	/**
	 * Load the data to SciDB (identified by connectionTo): to a given array
	 * from a given file.
	 * 
	 * @param connectionTo
	 * @param arrays
	 * @param scidbFilePath
	 * @return
	 * @throws SQLException
	 */
	public Object call() throws Exception {
		/*
		 * we have to create a flat array and redimension it to the final result
		 */
		/*
		 * AFL% store(redimension(load(test_waveform_flat,'/home/adam/data/
		 * waveform_test.scidb'),test_waveform_),test_waveform_);
		 */

		/* remove the auxiliary flat array if the target was not flat */

		// InputStream resultInStream =
		// RunShell.executeAQLcommandSciDB(conTo.getHost(), conTo.getPort(),
		// conTo.getBinPath(), "load " + arrayTo + " from '" + dataFile + "'");
		// String resultString = IOUtils.toString(resultInStream,
		// Constants.ENCODING);
		// log.debug("Load data to SciDB: " + resultString);
		SciDBHandler handler = new SciDBHandler(connectionTo);
		String loadCommand = null;
		loadCommand = "load(" + arrays.getFlat() + ", '" + scidbFilePath + "'";
		if (binaryFormat != null) {
			loadCommand += ",-2,'(" + binaryFormat + ")'";
		}
		loadCommand += ")";
		if (arrays.getMultiDimensional() != null) {
			loadCommand = "store(redimension(" + loadCommand + ","
					+ arrays.getMultiDimensional() + "),"
					+ arrays.getMultiDimensional() + ")";
		}
		log.debug("load command: " + LogUtils.replace(loadCommand));
		handler.executeStatementAFL(loadCommand);
		handler.commit();
		handler.close();
		return "Data successfuly loaded to SciDB";
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

	/* (non-Javadoc)
	 * @see istc.bigdawg.migration.Load#setLoadFrom(java.lang.String)
	 */
	@Override
	public void setLoadFrom(String filePath) {
		this.scidbFilePath = filePath;
	}

}
