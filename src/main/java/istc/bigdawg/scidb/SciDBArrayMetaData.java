/**
 * 
 */
package istc.bigdawg.scidb;

import java.util.List;
import java.util.Map;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class SciDBArrayMetaData {

	private Map<String, SciDBColumnMetaData> dimensionsMap;
	private List<SciDBColumnMetaData> dimensionsOrdered;
	private Map<String, SciDBColumnMetaData> attributesMap;
	private List<SciDBColumnMetaData> attributesOrdered;

	public SciDBArrayMetaData(Map<String, SciDBColumnMetaData> dimensionsMap, List<SciDBColumnMetaData> dimensionsOrdered,
			Map<String, SciDBColumnMetaData> attributesMap, List<SciDBColumnMetaData> attributesOrdered) {
		this.dimensionsMap = dimensionsMap;
		this.dimensionsOrdered = dimensionsOrdered;
		this.attributesMap = attributesMap;
		this.attributesOrdered = attributesOrdered;
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
