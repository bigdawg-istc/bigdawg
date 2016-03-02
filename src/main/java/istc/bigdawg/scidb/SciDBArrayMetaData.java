/**
 * 
 */
package istc.bigdawg.scidb;

import java.util.List;
import java.util.Map;

/**
 * The meta data about an array in SciDB.
 * 
 * @author Adam Dziedzic
 * 
 */
public class SciDBArrayMetaData {

	private String arrayName;
	private Map<String, SciDBColumnMetaData> dimensionsMap;
	private List<SciDBColumnMetaData> dimensionsOrdered;
	private Map<String, SciDBColumnMetaData> attributesMap;
	private List<SciDBColumnMetaData> attributesOrdered;

	public SciDBArrayMetaData(String arrayName,
			Map<String, SciDBColumnMetaData> dimensionsMap,
			List<SciDBColumnMetaData> dimensionsOrdered,
			Map<String, SciDBColumnMetaData> attributesMap,
			List<SciDBColumnMetaData> attributesOrdered) {
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

	public Map<String, SciDBColumnMetaData> getAttributesMap() {
		return attributesMap;
	}

	public List<SciDBColumnMetaData> getAttributesOrdered() {
		return attributesOrdered;
	}

	public Map<String, SciDBColumnMetaData> getDimensionsMap() {
		return dimensionsMap;
	}

	public List<SciDBColumnMetaData> getDimensionsOrdered() {
		return dimensionsOrdered;
	}

}
