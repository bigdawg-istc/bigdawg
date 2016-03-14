/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.LogUtils;

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
	private final SciDBArrays arrays;
	private final String scidbFilePath;
	private final String format;
	private boolean isBinary;

	public ExportFromSciDBExecutor(SciDBConnectionInfo connection, SciDBArrays arrays, String scidbFilePath,
			String format, boolean isBinary) {
		this.connection = connection;
		this.arrays = arrays;
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
		String saveCommandFinal = null;
		if (!isBinary) {
			String csvFormat = "csv+";
			String array = arrays.getMultiDimensional();
			/* this is only a flat array so export only the attributes */
			if (arrays.getFlat() != null) {
				csvFormat = "csv";
				array = arrays.getFlat();
			}
			saveCommandFinal = "save(" + array + ",'" + scidbFilePath + "',-2,'" + csvFormat + "')";
		} else { /* this is the binary migration */
			String array = null;
			if (arrays.getMultiDimensional() != null) {
				String multiDimArray = arrays.getMultiDimensional();
				String flatArray = arrays.getFlat();
				array = "store(redimension(" + multiDimArray + "," + flatArray + ")," + flatArray + ")";
			} else {
				/* only the flat array */
				array = arrays.getFlat();
			}
			saveCommand.append("save(" + array + ", '" + scidbFilePath + "'");
			saveCommand.append(",-2,'");
			saveCommand.append("(" + format + ")");
			saveCommand.append("')");
			saveCommandFinal = saveCommand.toString();
		}
		log.debug("save command: " + LogUtils.replace(saveCommandFinal));
		handler.executeStatementAFL(saveCommandFinal);
		handler.commit();
		handler.close();
		return "Data successfuly exported from SciDB";
	}

}
