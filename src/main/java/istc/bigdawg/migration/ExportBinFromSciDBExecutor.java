/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 4:39:08 PM
 */
public class ExportBinFromSciDBExecutor implements Callable<String> {

	/* log */
	private static Logger log = Logger.getLogger(LoadToSciDBExecutor.class);

	/* SciDB connection info */
	private SciDBConnectionInfo connection;
	private final String array;
	private final String scidbFilePath;
	private final String binaryFormat;

	public ExportBinFromSciDBExecutor(SciDBConnectionInfo connection, String array, String scidbFilePath, String binaryFormat) {
		this.connection = connection;
		this.array = array;
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
	public String call() throws SQLException {
		SciDBHandler handler = new SciDBHandler(connection);
		String saveCommand = null;
		saveCommand = "save(" + array + ", '" + scidbFilePath + "',-2,'(" + binaryFormat + ")')";
		log.debug("load command: " + saveCommand.replace("'", ""));
		handler.executeStatementAFL(saveCommand);
		handler.commit();
		handler.close();
		return "Data successfuly exported from SciDB";
	}

}
