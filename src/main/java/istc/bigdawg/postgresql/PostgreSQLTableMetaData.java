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

	private Map<String, PostgreSQLColumnMetaData> columnsMap;
	private List<PostgreSQLColumnMetaData> columnsOrdered;

	public PostgreSQLTableMetaData(Map<String, PostgreSQLColumnMetaData> columnsMap,
			List<PostgreSQLColumnMetaData> columnsOrdered) {
		super();
		this.columnsMap = columnsMap;
		this.columnsOrdered = columnsOrdered;
	}

	public Map<String, PostgreSQLColumnMetaData> getColumnsMap() {
		return columnsMap;
	}

	public List<PostgreSQLColumnMetaData> getColumnsOrdered() {
		return columnsOrdered;
	}

	@Override
	public String toString() {
		return "PostgreSQLTableMetaData [columnsMap=" + columnsMap + ", columnsOrdered=" + columnsOrdered + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnsMap == null) ? 0 : columnsMap.hashCode());
		result = prime * result + ((columnsOrdered == null) ? 0 : columnsOrdered.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PostgreSQLTableMetaData other = (PostgreSQLTableMetaData) obj;
		if (columnsMap == null) {
			if (other.columnsMap != null)
				return false;
		} else if (!columnsMap.equals(other.columnsMap))
			return false;
		if (columnsOrdered == null) {
			if (other.columnsOrdered != null)
				return false;
		} else if (!columnsOrdered.equals(other.columnsOrdered))
			return false;
		return true;
	}

}
