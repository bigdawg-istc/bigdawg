/**
 * 
 */
package istc.bigdawg.mysql;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.relational.RelationalTableMetaData;

import java.util.List;
import java.util.Map;

/**
 * Meta data about a table in MySQL.
 */
public class MySQLTableMetaData implements RelationalTableMetaData {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/** Name of the schema (by default public) and name of the table. */
	private MySQLSchemaTableName schemaTable;
	private Map<String, AttributeMetaData> columnsMap;
	private List<AttributeMetaData> columnsOrdered;

	public MySQLTableMetaData(MySQLSchemaTableName schemaTable,
                              Map<String, AttributeMetaData> columnsMap,
                              List<AttributeMetaData> columnsOrdered) {
		this.schemaTable = schemaTable;
		this.columnsMap = columnsMap;
		this.columnsOrdered = columnsOrdered;
	}

	/**
	 * @return the schemaTable
	 */
	public MySQLSchemaTableName getSchemaTable() {
		return schemaTable;
	}

	public Map<String, AttributeMetaData> getColumnsMap() {
		return columnsMap;
	}

	@Override
	public String getName() {
		return getSchemaTable().getFullName();
	}

	@Override
	public List<AttributeMetaData> getAttributesOrdered() {
		return columnsOrdered;
	}
}
