/**
 * 
 */
package istc.bigdawg.migration.datatypes;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.UnsupportedTypeException;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 12:24:52 PM
 */
public class DataTypesFromPostgreSQLToSciDB {
	
	private static Logger log = Logger.getLogger(DataTypesFromPostgreSQLToSciDB.class.getName());

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
		// float
		map.put("real", "float");
		map.put("double precision", "double");
		// string
		map.put("text", "string");
	}

	/**
	 * Returns a SciDB data type for a given PostgreSQL data type.
	 * 
	 * @param postgreSQLType
	 * @return
	 * @throws UnsupportedTypeException
	 */
	public static String getSciDBTypeFromPostgreSQLType(String postgreSQLType)
			throws UnsupportedTypeException {
		String scidbType = map.get(postgreSQLType);
		if (scidbType != null) {
			return scidbType;
		} else if (postgreSQLType.contains("character(1)")
				|| postgreSQLType.contains("char(1)")) {
			return "char";
		} else if (postgreSQLType.contains("character varying")
				|| postgreSQLType.contains("varchar")
				|| postgreSQLType.contains("character")
				|| postgreSQLType.contains("char")) {
			return "string";
		} else if (postgreSQLType.contains("timestamp")
				&& postgreSQLType.contains("with time zone")) {
			return "datetimetz";
		} else if (postgreSQLType.contains("timestamp") || postgreSQLType.contains("date")) {
			return "datetime";
		} else if (postgreSQLType.contains("decimal") || postgreSQLType.contains("numeric")) {
			log.warn("The decimal or numeric types cannot be cast precisely to types in SciDB.");
			if(postgreSQLType.contains(",") || postgreSQLType.equals("decimal") || postgreSQLType.equals("numeric")) {
				return "double";
			}
			else { /* the decimal type does not contain any digits after the decimal point */
				return "int64"; /* this type can be insufficient */
			}
		}
		else {
			throw new UnsupportedTypeException("The type from PostgreSQL: "
					+ postgreSQLType + " is not supported in SciDB.");
		}
	}

}
