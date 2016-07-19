/**
 * 
 */
package istc.bigdawg.migration;

import java.util.concurrent.Callable;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.query.DBHandler;

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
	public Object call() throws MigrationException;

	/**
	 * 
	 * @param filePath
	 *            Full path to the file to which the data should be exported.
	 */
	public void setExportTo(String filePath);

	/**
	 * 
	 * @return DBHanlder which is native for this export object (for example for
	 *         ExportPostgres it should return PostgreSQLHandler).
	 */
	public DBHandler getHandler() throws MigrationException;

	/**
	 * Set the handler to the database to which we load the data.
	 */
	public void setHandlerTo(DBHandler handlerto) throws MigrationException;

}
