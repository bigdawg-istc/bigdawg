/**
 * 
 */
package istc.bigdawg.migration;

import java.util.concurrent.Callable;

import istc.bigdawg.query.DBHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Load interface - load data to a database.
 */
public interface Load extends Callable<Object>, DataPump {

	/**
	 * Load data to the database.
	 * 
	 * @return Number of rows loaded to a database.
	 */
	public Object call() throws Exception;

	/**
	 * 
	 * @param filePath
	 *            Full path to the file from which the data should be loaded.
	 */
	public void setLoadFrom(String filePath);
	
	/**
	 * 
	 * @param fromHandler the handler to the database from which we export the data
	 */
	public void setHandlerFrom(DBHandler fromHandler);

	/**
	 * @return Handler to the database to which we load the data.
	 */
	public DBHandler getHandler();

}
