/**
 * 
 */
package istc.bigdawg.migration;

import java.util.concurrent.Callable;

/**
 * @author Adam Dziedzic
 * 
 * Load interface - load data to a database.
 */
public interface Load extends Callable<Object> {
	
	/**
	 * Load data to the database.
	 * 
	 * @return Number of rows loaded to a database.
	 */
	Object call() throws Exception;

}
