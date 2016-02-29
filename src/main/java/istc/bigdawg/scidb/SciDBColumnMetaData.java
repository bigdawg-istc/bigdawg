/**
 * 
 */
package istc.bigdawg.scidb;

/**
 * Meta data about column/attributes in an array in SciDB.
 * 
 * @author Adam Dziedzic
 * 
 *
 */
public class SciDBColumnMetaData {
	private String columnName;
	private String columnType;
	private boolean isNullable;
	
	public SciDBColumnMetaData(String columnName, String columnType, boolean isNullable) {
		this.columnName = columnName;
		this.columnType = columnType;
		this.isNullable = isNullable;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getColumnType() {
		return columnType;
	}
	
	public boolean isNullable() {
		return isNullable;
	}

	@Override
	public String toString() {
		return "SciDBColumnMetaData [columnName=" + columnName + ", columnType=" + columnType + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
		result = prime * result + ((columnType == null) ? 0 : columnType.hashCode());
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
		SciDBColumnMetaData other = (SciDBColumnMetaData) obj;
		if (columnName == null) {
			if (other.columnName != null)
				return false;
		} else if (!columnName.equals(other.columnName))
			return false;
		if (columnType == null) {
			if (other.columnType != null)
				return false;
		} else if (!columnType.equals(other.columnType))
			return false;
		return true;
	}


}
