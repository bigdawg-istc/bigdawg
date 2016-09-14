/**
 * 
 */
package istc.bigdawg.migration;

/**
 * File formats for the migration; current types:
 * 
 * CSV - a typical comma separated values (a form of text format).
 * 
 * BIN_POSTGRES - binary format supported by PostgreSQL.
 * 
 * BIN_SCIDB - binary format supported by SciDB.
 * 
 * @author Adam Dziedzic
 */
public enum FileFormat {
	CSV, BIN_POSTGRES, BIN_SCIDB, SCIDB_TEXT_FORMAT;

	/**
	 * 
	 * @return CSV delimiter (e.g. comma, vertical bar, etc). In some cases it
	 *         can be longer than one character.
	 */
	public static String getCsvDelimiter() {
		/* Vertical pipe '|' as the default CSV delimiter. */
		return "|";
	}

	/**
	 * 
	 * @return quote character " or ' (single or double quote)
	 */
	public static String getQuoteCharacter() {
		return "'";
	}

}
