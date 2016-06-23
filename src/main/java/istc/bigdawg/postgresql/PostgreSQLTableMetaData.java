/**
 * 
 */
package istc.bigdawg.postgresql;

import java.util.List;
import java.util.Map;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;

/**
 * Meta data about a table in PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *
 */
public class PostgreSQLTableMetaData implements ObjectMetaData {

	/** Name of the schema (by default public) and name of the table. */
	private PostgreSQLSchemaTableName schemaTable;
	private Map<String, AttributeMetaData> columnsMap;
	private List<AttributeMetaData> columnsOrdered;

	public PostgreSQLTableMetaData(PostgreSQLSchemaTableName schemaTable,
			Map<String, AttributeMetaData> columnsMap,
			List<AttributeMetaData> columnsOrdered) {
		this.schemaTable = schemaTable;
		this.columnsMap = columnsMap;
		this.columnsOrdered = columnsOrdered;
	}

	/**
	 * @return the schemaTable
	 */
	public PostgreSQLSchemaTableName getSchemaTable() {
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
