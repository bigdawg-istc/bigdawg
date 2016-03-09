/**
 * 
 */
package istc.bigdawg.exceptions;

/**
 * @author Adam Dziedzic
 * 
 * Mar 9, 2016 11:30:41 AM
 */
public class NetworkException extends BigDawgException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param msg
	 */
	public NetworkException(String msg) {
		super(msg);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public NetworkException(String message, Throwable cause) {
		super(message, cause);
	}

}
