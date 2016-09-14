package istc.bigdawg.sstore;

import java.util.List;
import java.util.Map;

public class SStoreSQLTableMetaData {
    
	private Map<String, SStoreSQLColumnMetaData> columnsMap;
	private List<SStoreSQLColumnMetaData> columnsOrdered;

	public SStoreSQLTableMetaData(Map<String, SStoreSQLColumnMetaData> columnsMap,
			List<SStoreSQLColumnMetaData> columnsOrdered) {
		this.columnsMap = columnsMap;
		this.columnsOrdered = columnsOrdered;
	}

	/**
	 * @return the schemaTable
	 */

	public Map<String, SStoreSQLColumnMetaData> getColumnsMap() {
		return columnsMap;
	}

	public List<SStoreSQLColumnMetaData> getColumnsOrdered() {
		return columnsOrdered;
	}

}
