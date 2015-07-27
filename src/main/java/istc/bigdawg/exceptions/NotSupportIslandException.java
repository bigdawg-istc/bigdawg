package istc.bigdawg.exceptions;

public class NotSupportIslandException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4205046404641840097L;

	public NotSupportIslandException() {
	}

	public NotSupportIslandException(String message) {
		super(message);
	}

	public NotSupportIslandException(Throwable cause) {
		super(cause);
	}

	public NotSupportIslandException(String message, Throwable cause) {
		super(message, cause);
	}

	public NotSupportIslandException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
			}

}
