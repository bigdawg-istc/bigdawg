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
public class ExportFromSciDBExecutor implements Callable<String> {

	/* log */
	private static Logger log = Logger.getLogger(LoadToSciDBExecutor.class);

	/* SciDB connection info */
	private SciDBConnectionInfo connection;
	private final String array;
	private final String scidbFilePath;
	private final String format;
	private boolean isBinary;

	public ExportFromSciDBExecutor(SciDBConnectionInfo connection, String array,
			String scidbFilePath, String format, boolean isBinary) {
		this.connection = connection;
		this.array = array;
		this.scidbFilePath = scidbFilePath;
		this.format = format;
		this.isBinary = isBinary;
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
		StringBuilder saveCommand = new StringBuilder();
		saveCommand.append("save(" + array + ", '" + scidbFilePath + "'");
		saveCommand.append(",-2,'");
		if (isBinary) {
			saveCommand.append("(" + format + ")");
		}
		else {
			saveCommand.append(format);
		}
		saveCommand.append("')");
		String saveCommandFinal = saveCommand.toString();
		log.debug("save command: " + saveCommandFinal.replace("'", ""));
		handler.executeStatementAFL(saveCommandFinal);
		handler.commit();
		handler.close();
		return "Data successfuly exported from SciDB";
	}

}
