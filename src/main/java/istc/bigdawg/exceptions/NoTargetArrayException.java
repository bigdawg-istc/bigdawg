/**
 * 
 */
package istc.bigdawg.exceptions;

/**
 * The exception indicates that there is no target array in SciDB to which we
 * want to migrate some data.
 * 
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 11:54:2 AM
 */
public class NoTargetArrayException extends BigDawgException {

	/**
	 * Default serial version id.
	 */
	private static final long serialVersionUID = 1L;

	public NoTargetArrayException() {
		super("No target array in SciDB.");
	}

	public NoTargetArrayException(String msg) {
		super(msg);
	}

	public NoTargetArrayException(String msg, Throwable ex) {
		super(msg, ex);
	}

}
