/**
 * 
 */
package istc.bigdawg.exceptions;

/**
 * Throw the exception when a given type from a database cannot be mapped to a
 * type in another database.
 * 
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 12:45:58 PM
 */
public class UnsupportedTypeException extends BigDawgException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public UnsupportedTypeException(String msg) {
		super(msg);
	}

}
