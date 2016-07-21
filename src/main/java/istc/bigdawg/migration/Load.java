/**
 * 
 */
package istc.bigdawg.migration;

import java.io.Serializable;
import java.util.concurrent.Callable;

import istc.bigdawg.query.DBHandler;

/**
 * Load interface - load data to a database.
 * 
 * @author Adam Dziedzic
 */
public interface Load extends Callable<Object>, DataPump, Serializable {

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
	 * @param fromHandler
	 *            the handler to the database from which we export the data
	 */
	public void setHandlerFrom(DBHandler fromHandler);

	/**
	 * @return Handler to the database to which we load the data.
	 */
	public DBHandler getHandler();

	/**
	 * 
	 * @return The {@link istc.bigdawg.migration.MigrationInfo} object which
	 *         contains information about the migration process (databases and
	 *         objects/tables/arrays between we migrate data)
	 */
	public MigrationInfo getMigrationInfo();

}
