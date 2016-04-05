package istc.bigdawg.sstore;

public class SStoreSQLColumnMetaData {
    	private String name;
	private boolean isNullable;
	private String dataType;

	public SStoreSQLColumnMetaData(String name, String dataType, boolean isNullable) {
		this.name = name;
		this.isNullable = isNullable;
		this.dataType = dataType;
	}
	
	public String getName() {
		return name;
	}

	public String getdataType() {
		return dataType;
	}
	
	public boolean isNullable() {
		return isNullable;
	}

	@Override
	public String toString() {
		return "SStoreColumnMetaData [columnName=" + name + ", columnType=" + dataType + "]";
	}
}
