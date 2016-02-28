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
 *         Feb 24, 2016 11:54:12 AM
 */
public class NoTargetArrayException extends BigDawgException {
	
	public NoTargetArrayException() {
		super("No target array in SciDB.");
	}

	public NoTargetArrayException(String msg) {
		super(msg);
	}

}
