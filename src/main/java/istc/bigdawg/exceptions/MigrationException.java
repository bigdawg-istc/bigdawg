/**
 * 
 */
package istc.bigdawg.exceptions;

/**
 * Problem with a data migration.
 * 
 * @author Adam Dziedzic
 */
public class MigrationException extends BigDawgException {

	/**
	 * Default serial version id.
	 */
	private static final long serialVersionUID = 1L;

	public MigrationException(String msg) {
		super(msg);
	}

	public MigrationException(String msg, Throwable cause) {
		super(msg, cause);
	}

}
