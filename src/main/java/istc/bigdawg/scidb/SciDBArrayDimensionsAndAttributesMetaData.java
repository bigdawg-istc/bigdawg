/**
 * 
 */
package istc.bigdawg.scidb;

import java.util.List;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;

/**
 * Store together meta data for dimensions and attributes in an array in SciDB.
 * 
 * @author Adam Dziedzic
 */
public class SciDBArrayDimensionsAndAttributesMetaData
		implements ObjectMetaData {

	/** The name of the array in SciDB. */
	private String arrayName;

	/** List of dimensions and attributes. */
	private List<AttributeMetaData> attributesMetaData;

	/**
	 * 
	 * @param arrayName The name of the array in SciDB.
	 * @param attributesMetaData List of dimensions and attributes.
	 */
	public SciDBArrayDimensionsAndAttributesMetaData(String arrayName,
			List<AttributeMetaData> attributesMetaData) {
		this.arrayName = arrayName;
		this.attributesMetaData = attributesMetaData;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.database.ObjectMetaData#getName()
	 */
	@Override
	public String getName() {
		return arrayName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.database.ObjectMetaData#getAttributesOrdered()
	 */
	@Override
	public List<AttributeMetaData> getAttributesOrdered() {
		return attributesMetaData;
	}

}
