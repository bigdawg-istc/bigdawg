/**
 * 
 */
package istc.bigdawg.migration.datatypes;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.UnsupportedTypeException;
import jline.internal.Log;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 12:24:52 PM
 */
public class DataTypesFromSStoreSQLToSciDB {
	
	private static Logger log = Logger.getLogger(DataTypesFromSStoreSQLToSciDB.class.getName());

	private static Map<String, String> map;

	static {
		map = new HashMap<>();
		// boolean
		map.put("boolean", "bool");
		// integers
		map.put("int", "int32");
		map.put("tinyint", "int16");
		map.put("smallint", "int16");
		map.put("integer", "int32");
		map.put("bigint", "int64");
		// float
		map.put("float", "double");
		map.put("decimal", "double");
		// string
		map.put("string", "string");
	}

	/**
	 * Returns a SciDB data type for a given S-Store data type.
	 * 
	 * @param sstoreSQLType
	 * @return
	 * @throws UnsupportedTypeException
	 */
	public static String getSciDBTypeFromSStoreSQLType(String sstoreSQLType)
			throws UnsupportedTypeException {
		String scidbType = map.get(sstoreSQLType);
		if (scidbType != null) {
			return scidbType;
		} else if (sstoreSQLType.contains("character(1)")
				|| sstoreSQLType.contains("char(1)")) {
			return "char";
		} else if (sstoreSQLType.contains("character varying")
				|| sstoreSQLType.contains("varchar")
				|| sstoreSQLType.contains("character")
				|| sstoreSQLType.contains("char")) {
			return "string";
		} else if (sstoreSQLType.contains("timestamp")
				&& sstoreSQLType.contains("with time zone")) {
			return "datetimetz";
		} else if (sstoreSQLType.contains("timestamp") || sstoreSQLType.contains("date")) {
			return "datetime";
		} else if (sstoreSQLType.contains("decimal") || sstoreSQLType.contains("numeric")) {
			log.warn("The decimal or numeric types cannot be cast precisely to types in SciDB.");
			if(sstoreSQLType.contains(",") || sstoreSQLType.equals("decimal") || sstoreSQLType.equals("numeric")) {
				return "double";
			}
			else { /* the decimal type does not contain any digits after the decimal point */
				return "int64"; /* this type can be insufficient */
			}
		}
		else {
			throw new UnsupportedTypeException("The type from SStore: "
					+ sstoreSQLType + " is not supported in SciDB.");
		}
	}

}
