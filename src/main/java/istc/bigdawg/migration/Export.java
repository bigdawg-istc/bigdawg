/**
 * 
 */
package istc.bigdawg.migration;

import java.util.concurrent.Callable;

/**
 * @author Adam Dziedzic
 * 
 *         Export interface - export data from a database.
 */
public interface Export extends Callable<Object>, DataPump {

	/**
	 * Export data from a database.
	 * 
	 * @return Number of exported elements (rows/tuples/cells, etc.)
	 * @throws Exception
	 *             Problem during export.
	 */
	public Object call() throws Exception;

	/**
	 * 
	 * @param filePath
	 *            Full path to the file to which the data should be exported.
	 */
	public void setExportTo(String filePath);
}
