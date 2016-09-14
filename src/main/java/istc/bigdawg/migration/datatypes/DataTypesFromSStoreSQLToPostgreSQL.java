package istc.bigdawg.migration.datatypes;

import java.util.HashMap;
import java.util.Map;

import istc.bigdawg.exceptions.UnsupportedTypeException;

public class DataTypesFromSStoreSQLToPostgreSQL {
    
    private static Map<String, String> map;

	static {
		map = new HashMap<>();
		// boolean
		map.put("boolean","boolean");
		// integers
		map.put("int", "integer");
		map.put("tinyint", "smallint");
		map.put("smallint", "smallint");
		map.put("integer", "integer");
		map.put("bigint", "bigint");
		// float
//		map.put("float", "real");
		map.put("float", "double precision");
		map.put("decimal", "double precision");
		// string
		map.put("string", "varchar");
//		map.put("char", "character(1)");
		// time
		map.put("timestamp", "bigint");
	}

    public static String getPostgreSQLTypeFromSStoreType(String sStoreType) throws UnsupportedTypeException {
	String postgresType = map.get(sStoreType.toLowerCase());
	if (postgresType != null) {
		return postgresType;
	} else {
		throw new UnsupportedTypeException("The type from SciDB: "
				+ sStoreType + " is not supported in PostgreSQL.");
	}
    }

}
