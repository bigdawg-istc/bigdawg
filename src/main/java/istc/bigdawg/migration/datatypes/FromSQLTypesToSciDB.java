/**
 * 
 */
package istc.bigdawg.migration.datatypes;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.UnsupportedTypeException;

/**
 * Change the SQL General Data Types (e.g.
 * http://www.w3schools.com/sql/sql_datatypes_general.asp) to types supported in
 * SciDB.
 * 
 * 
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 12:24:52 PM
 */
public class FromSQLTypesToSciDB {

	private static Logger log = Logger
			.getLogger(FromSQLTypesToSciDB.class.getName());

	private static Map<String, String> map;

	static {
		map = new HashMap<>();
		// boolean
		map.put("boolean", "bool");
		// integers
		map.put("smallint", "int16");
		map.put("integer", "int32");
		map.put("bigint", "int64");
		map.put("smallserial", "int16");
		map.put("serial", "int32");
		map.put("bigserial", "int64");
		map.put("json", "string");
		// float
		map.put("real", "float");
		map.put("double precision", "double");
		// string
		map.put("text", "string");
	}

	/**
	 * Returns a SciDB data type for a given SQL data type.
	 * 
	 * @param SQLType
	 * @return
	 * @throws UnsupportedTypeException
	 */
	public static String getSciDBTypeFromSQLType(String SQLType)
			throws UnsupportedTypeException {
		String scidbType = map.get(SQLType);
		if (scidbType != null) {
			return scidbType;
		} else if (SQLType.contains("character(1)")
				|| SQLType.contains("char(1)")) {
			return "char";
		} else if (SQLType.contains("character varying")
				|| SQLType.contains("varchar") || SQLType.contains("character")
				|| SQLType.contains("char")) {
			return "string";
		} else if (SQLType.contains("timestamp")
				&& SQLType.contains("with time zone")) {
			return "datetimetz";
		} else if (SQLType.contains("timestamp") || SQLType.contains("date")) {
			return "datetime";
		} else if (SQLType.contains("decimal") || SQLType.contains("numeric")) {
			log.warn(
					"The decimal or numeric types cannot be cast precisely to types in SciDB.");
			if (SQLType.contains(",") || SQLType.equals("decimal")
					|| SQLType.equals("numeric")) {
				return "double";
			} else { /*
						 * the decimal type does not contain any digits after
						 * the decimal point
						 */
				return "int64"; /* this type can be insufficient */
			}
		} else {
			throw new UnsupportedTypeException("The SQL type: "
					+ SQLType + " is not supported in SciDB.");
		}
	}

}
