/**
 * 
 */
package istc.bigdawg.migration;

/**
 * @author Adam Dziedzic
 * 
 *         File formats for the migration; current types:
 * 
 *         CSV - a typical comma separated values (a form of text format).
 * 
 *         BIN_POSTGRES - binary format supported by PostgreSQL.
 * 
 *         BIN_SCIDB - binary format supported by SciDB.
 */
public enum FileFormat {
	CSV, BIN_POSTGRES, BIN_SCIDB;

	/**
	 * 
	 * @return CSV delimiter (e.g. comma, vertical bar, etc). In some cases it
	 *         can be longer than one character.
	 */
	public static String getCsvDelimiter() {
		/* Vertical pipe '|' as the default CSV delimiter. */
		return "|";
	}
}
