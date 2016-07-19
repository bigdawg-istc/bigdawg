/**
 * 
 */
package istc.bigdawg.scidb;

import java.util.List;
import java.util.Map;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;

/**
 * The meta data about an array in SciDB.
 * 
 * @author Adam Dziedzic
 * 
 */
public class SciDBArrayMetaData implements ObjectMetaData {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String arrayName;
	private Map<String, AttributeMetaData> dimensionsMap;
	private List<AttributeMetaData> dimensionsOrdered;
	private Map<String, AttributeMetaData> attributesMap;
	private List<AttributeMetaData> attributesOrdered;

	public SciDBArrayMetaData(String arrayName,
			Map<String, AttributeMetaData> dimensionsMap,
			List<AttributeMetaData> dimensionsOrdered,
			Map<String, AttributeMetaData> attributesMap,
			List<AttributeMetaData> attributesOrdered) {
		this.arrayName = arrayName;
		this.dimensionsMap = dimensionsMap;
		this.dimensionsOrdered = dimensionsOrdered;
		this.attributesMap = attributesMap;
		this.attributesOrdered = attributesOrdered;
	}

	/**
	 * @return the arrayName
	 */
	public String getArrayName() {
		return arrayName;
	}

	public Map<String, AttributeMetaData> getAttributesMap() {
		return attributesMap;
	}

	@Override
	public List<AttributeMetaData> getAttributesOrdered() {
		return attributesOrdered;
	}

	public Map<String, AttributeMetaData> getDimensionsMap() {
		return dimensionsMap;
	}

	public List<AttributeMetaData> getDimensionsOrdered() {
		return dimensionsOrdered;
	}

	/* (non-Javadoc)
	 * @see istc.bigdawg.database.ObjectMetaData#getName()
	 */
	@Override
	public String getName() {
		return getArrayName();
	}

}
