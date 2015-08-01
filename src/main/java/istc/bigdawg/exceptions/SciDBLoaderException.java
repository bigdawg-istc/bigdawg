/**
 * 
 */
package istc.bigdawg.exceptions;

/**
 * @author Adam Dziedzic
 *
 */
public class SciDBLoaderException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6096497779298872625L;

	/**
	 * 
	 */
	public SciDBLoaderException() {
	}

	/**
	 * @param message
	 */
	public SciDBLoaderException(String message) {
		super(message);
	}

	/**
	 * @param cause
	 */
	public SciDBLoaderException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param message
	 * @param cause
	 */
	public SciDBLoaderException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SciDBLoaderException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
