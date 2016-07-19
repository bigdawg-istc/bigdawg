/**
 * 
 */
package istc.bigdawg.database;

import java.io.Serializable;
import java.util.Optional;

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
public class AttributeMetaData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/** The name of the attribute. */
	private String name;

	/** Specified position of the attribute in an array/object/table. */
	private int position;

	/** Can the value of the attribute be null. */
	private boolean isNullable;

	/** The type of the attribute. */
	private String sqlDataType;

	private int characterMaximumLength;
	private int numericPrecision;
	private int numericScale;

	/** Specific data type in SciDB. It can be null. */
	private String scidbDataType;

	/** Specify if the attribute can be treated as a dimension in an array. */
	private boolean isDimension = false;

	public AttributeMetaData(String name, int position, boolean isNullable,
			String sqlDataType, int characterMaximumLength,
			int numericPrecision, int numericScale) {
		this.name = name;
		this.position = position;
		this.isNullable = isNullable;
		this.sqlDataType = sqlDataType;
		this.characterMaximumLength = characterMaximumLength;
		this.numericPrecision = numericPrecision;
		this.numericScale = numericScale;
	}

	public AttributeMetaData(String name, int position, boolean isNullable,
			String sqlDataType, int characterMaximumLength,
			int numericPrecision, int numericScale, boolean isDimension) {
		this.name = name;
		this.position = position;
		this.isNullable = isNullable;
		this.sqlDataType = sqlDataType;
		this.characterMaximumLength = characterMaximumLength;
		this.numericPrecision = numericPrecision;
		this.numericScale = numericScale;
		this.isDimension = isDimension;
	}

	public AttributeMetaData(String name, String sqlDataType,
			boolean isNullable) {
		this.name = name;
		this.sqlDataType = sqlDataType;
		this.isNullable = isNullable;
	}

	public AttributeMetaData(String name, String sqlDataType,
			boolean isNullable, boolean isDimension) {
		this.name = name;
		this.sqlDataType = sqlDataType;
		this.isNullable = isNullable;
		this.isDimension = isDimension;
	}

	public AttributeMetaData(String name, String sqlDataType,
			String scidbDataType, boolean isNullable, boolean isDimension) {
		this.name = name;
		this.sqlDataType = sqlDataType;
		this.scidbDataType = scidbDataType;
		this.isNullable = isNullable;
		this.isDimension = isDimension;
	}

	@Override
	public String toString() {
		return "[name=" + name + ", position=" + position + ", isNullable="
				+ isNullable + ", sqlDataType=" + sqlDataType
				+ ", characterMaximumLength=" + characterMaximumLength
				+ ", numericPrecision=" + numericPrecision + ", numericScale="
				+ numericScale + "]";
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + characterMaximumLength;
		result = prime * result
				+ ((sqlDataType == null) ? 0 : sqlDataType.hashCode());
		result = prime * result + (isDimension ? 1231 : 1237);
		result = prime * result + (isNullable ? 1231 : 1237);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + numericPrecision;
		result = prime * result + numericScale;
		result = prime * result + position;
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
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
		if (sqlDataType == null) {
			if (other.sqlDataType != null)
				return false;
		} else if (!sqlDataType.equals(other.sqlDataType))
			return false;
		if (isDimension != other.isDimension)
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

	public String getSqlDataType() {
		return sqlDataType;
	}

	public Optional<String> getSciDBDataType() {
		return Optional.ofNullable(scidbDataType);
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

	/**
	 * @return the isDimension
	 */
	public boolean isDimension() {
		return isDimension;
	}

}
