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
	 * generated serial version id
	 */
	private static final long serialVersionUID = -1380667796547079555L;
	private static final String MIGRATION_MSG = "Migration problem. "; 

	public MigrationException(String msg) {
		super(MIGRATION_MSG+msg);
	}
	
	public MigrationException(String msg, Throwable cause) {
		super(MIGRATION_MSG+msg,cause);
	}

}
