/**
 * 
 */
package istc.bigdawg.mysql;

import istc.bigdawg.relational.RelationalSchemaTableName;

/**
 * @author Kate Yu
 * Deal with the name of a table and schema.
 */
public class MySQLSchemaTableName implements RelationalSchemaTableName {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final String DEFAULT_SCHEMA = "test";

	private String schemaName;
	private String tableName;

	/**
	 * If a table name is without a dot (.) then there is no schema in the table
	 * and we return the default schema name: public.
	 * 
	 * If there is a dot (.) in the table name then we divide the name into 2
	 * parts: - the schema name: before the dot - the table name: after the dot.
	 * 
	 * @param tableNameInitial
	 */
	public MySQLSchemaTableName(String tableNameInitial) {
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
	public MySQLSchemaTableName(String schemaName, String tableName) {
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
	
	public String getFullName() {
		return schemaName+"."+tableName;
	}
}
