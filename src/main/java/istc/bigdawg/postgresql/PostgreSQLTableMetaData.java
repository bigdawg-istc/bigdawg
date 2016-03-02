/**
 * 
 */
package istc.bigdawg.postgresql;

import java.util.List;
import java.util.Map;

/**
 * Meta data about a table in PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *
 */
public class PostgreSQLTableMetaData {

	private PostgreSQLSchemaTableName schemaTable;
	private Map<String, PostgreSQLColumnMetaData> columnsMap;
	private List<PostgreSQLColumnMetaData> columnsOrdered;

	public PostgreSQLTableMetaData(PostgreSQLSchemaTableName schemaTable, Map<String, PostgreSQLColumnMetaData> columnsMap,
			List<PostgreSQLColumnMetaData> columnsOrdered) {
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

	public Map<String, PostgreSQLColumnMetaData> getColumnsMap() {
		return columnsMap;
	}

	public List<PostgreSQLColumnMetaData> getColumnsOrdered() {
		return columnsOrdered;
	}
}
