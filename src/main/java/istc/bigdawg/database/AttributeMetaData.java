/**
 * 
 */
package istc.bigdawg.database;

/**
 * Meta data about an attribute/column/cell in a table/array or another object
 * in a database.
 * 
 * The meta data contain information such as: name of the attribute, type, if
 * the attribute can be null, etc.
 * 
 * @author Adam Dziedzic
 * 
 *         2016 Jan 13, 2016 2:23:23 PM
 */
public class AttributeMetaData {

	/** The name of the attribute. */
	private String name;
	
	
	private int position;
	private boolean isNullable;
	private String dataType;
	private int characterMaximumLength;
	private int numericPrecision;
	private int numericScale;

	public AttributeMetaData(String name, int position, boolean isNullable,
			String dataType, int characterMaximumLength, int numericPrecision,
			int numericScale) {
		this.name = name;
		this.position = position;
		this.isNullable = isNullable;
		this.dataType = dataType;
		this.characterMaximumLength = characterMaximumLength;
		this.numericPrecision = numericPrecision;
		this.numericScale = numericScale;
	}

	public AttributeMetaData(String name, String dataType,
			boolean isNullable) {
		this.name = name;
		this.dataType = dataType;
		this.isNullable = isNullable;
	}

	@Override
	public String toString() {
		return "PostgreSQLColumnMetaData [name=" + name + ", position="
				+ position + ", isNullable=" + isNullable + ", dataType="
				+ dataType + ", characterMaximumLength="
				+ characterMaximumLength + ", numericPrecision="
				+ numericPrecision + ", numericScale=" + numericScale + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + characterMaximumLength;
		result = prime * result
				+ ((dataType == null) ? 0 : dataType.hashCode());
		result = prime * result + (isNullable ? 1231 : 1237);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + numericPrecision;
		result = prime * result + numericScale;
		result = prime * result + position;
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
		AttributeMetaData other = (AttributeMetaData) obj;
		if (characterMaximumLength != other.characterMaximumLength)
			return false;
		if (dataType == null) {
			if (other.dataType != null)
				return false;
		} else if (!dataType.equals(other.dataType))
			return false;
		if (isNullable != other.isNullable)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;	
		} else if (!name.equals(other.name))	
			return false;	
		if (numericPrecision != other.numericPrecision)	
			return false;	
		if (numericScale != other.numericScale)	
			return false;
		if (position != other.position)
			return false;
		return true;
	}

	public String getName() {
		return name;
	}

	public int getPosition() {
		return position;
	}

	public boolean isNullable() {
		return isNullable;
	}

	public String getDataType() {
		return dataType;
	}

	public int getCharacterMaximumLength() {
		return characterMaximumLength;
	}

	public int getNumericPrecision() {
		return numericPrecision;
	}

	public int getNumericScale() {
		return numericScale;
	}

}
