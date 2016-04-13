package istc.bigdawg.sstore;

public class SStoreSQLColumnMetaData {
    	private String name;
	private boolean isNullable;
	private String dataType;
	private int position;
	private int characterMaximumLength;
	private int numericPrecision;

	public SStoreSQLColumnMetaData(String name, String dataType, boolean isNullable, int postion, int size) {
		this.name = name;
		this.isNullable = isNullable;
		this.dataType = dataType;
		this.position = postion;
		this.characterMaximumLength = size;
		this.numericPrecision = size;
		
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
	
	public int getPosition() {
	    	return position;
	}
	
	public int getCharacterMaximumLength() {
		return characterMaximumLength;
	}

	public int getNumericPrecision() {
		return numericPrecision;
	}

	@Override
	public String toString() {
	    return "SStoreSQLColumnMetaData [name=" + name + ", position=" + position + ", isNullable=" + isNullable
			+ ", dataType=" + dataType + ", characterMaximumLength=" + characterMaximumLength
			+ ", numericPrecision=" + numericPrecision + "]";
	}
}
