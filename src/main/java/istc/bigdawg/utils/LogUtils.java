/**
 * 
 */
package istc.bigdawg.utils;

/**
 * Utilities for logging (we use log4j).
 * 
 * @author Adam Dziedzic
 */
public class LogUtils {

	/**
	 * the replacement character for quotation marks for logs sent to PostgreSQL
	 */
	public static final String REPLACEMENT = "*";

	/** log4j does not accept it for PostgreSQL logger */
	public static final String QUOTATION_MARK = "'";

	/**
	 * The logging to PostgreSQL cannot accept ' (quotation marks). We replace
	 * them with asterisks.
	 * 
	 * @param message
	 *            the message to be logged
	 * @return the message with quotation marks replaced with "*" (asterisks)
	 */
	public static String replace(String message) {
		return message.replace(QUOTATION_MARK, REPLACEMENT);
	}

}
