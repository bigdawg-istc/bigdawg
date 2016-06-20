/**
 * 
 */
package istc.bigdawg.database;

import java.util.List;

/**
 * 
 * Meta data about an object/table/array etc.
 * 
 * @author Adam Dziedzic
 * 
 *
 */
public interface ObjectMetaData {

	/**
	 * Get ordered attributes for this object. If the attributes are not
	 * changed, then each call of the method should return the same result.
	 */
	public List<AttributeMetaData> getAttributesOrdered();
}
