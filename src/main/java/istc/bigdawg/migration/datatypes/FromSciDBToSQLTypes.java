/**
 * 
 */
package istc.bigdawg.migration.datatypes;

import java.util.HashMap;
import java.util.Map;

import istc.bigdawg.exceptions.UnsupportedTypeException;

/**
 * Change the types supported by SciDB to SQL General Data Types (e.g.
 * http://www.w3schools.com/sql/sql_datatypes_general.asp).
 * 
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 6:01:58 PM
 */
public class FromSciDBToSQLTypes {

	private static Map<String, String> map;

	static {
		map = new HashMap<>();
		// boolean
		map.put("bool", "boolean");
		// integers
		map.put("int16", "smallint");
		map.put("int32", "integer");
		map.put("int64", "bigint");
		// float
		map.put("float", "real");
		map.put("double", "double precision");
		// string
		map.put("string", "text");
		map.put("char", "character(1)");
		// time
		map.put("datetime", "timestamp (0)");
		map.put("datetimetz", "timestamp (0) with time zone");
	}

	/**
	 * Returns an SQL data type for a given SciDB data type.
	 * 
	 * @param scidbType
	 *            a data type from SciDB
	 * @return an SQL data type for a given SciDB data type
	 * @throws UnsupportedTypeException
	 */
	public static String getSQLTypeFromSciDBType(String scidbType)
			throws UnsupportedTypeException {
		String postgresType = map.get(scidbType);
		if (postgresType != null) {
			return postgresType;
		} else {
			throw new UnsupportedTypeException("The type from SciDB: "
					+ scidbType + " is not supported in SQL standard.");
		}
	}
}
