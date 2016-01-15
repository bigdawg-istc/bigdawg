/**
 * 
 */
package istc.bigdawg.postgresql;

/**
 * @author Adam Dziedzic
 * 
 *         Jan 13, 2016 6:39:30 PM
 * 
 *         Deal with the name of a table and schema.
 */
public class PostgreSQLSchemaTableName {

	public static final String DEFAULT_SCHEMA = "public";

	private String schemaName;
	private String tableName;

	/**
	 * If a table name is without a dot (.) then there is no schema in the table
	 * and we return the default schema name: public.
	 * 
	 * If there is a dot (.) in the table name then we divide the name into 2
	 * parts: - the schema name: before the dot - the table name: after the dot.
	 * 
	 * @param tableName
	 */
	public PostgreSQLSchemaTableName(String tableNameInitial) {
		this.tableName = tableNameInitial;
		this.schemaName = DEFAULT_SCHEMA;
		if (tableNameInitial.contains(".")) {
			// divide the given string based on dot: "."
			String[] schemaTable = tableName.split("\\.");
			if (schemaTable.length > 2) {
				throw new IllegalArgumentException("The table name contains more than one dot (.)!");
			}
			this.schemaName = schemaTable[0];
			this.tableName = schemaTable[1];
		}
	}

	/**
	 * 
	 * @param schemaName
	 * @param tableName
	 */
	public PostgreSQLSchemaTableName(String schemaName, String tableName) {
		this.schemaName = schemaName;
		this.tableName = tableName;
	}

	/**
	 * 
	 * @return schema name
	 */
	public String getSchemaName() {
		return schemaName;
	}

	/**
	 * 
	 * @return table name
	 */
	public String getTableName() {
		return tableName;
	}

}
