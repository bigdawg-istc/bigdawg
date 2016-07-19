/**
 * 
 */
package istc.bigdawg.database;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Meta data about an object/table/array etc.
 * 
 * @author Adam Dziedzic
 */
public interface ObjectMetaData extends Serializable {

	/**
	 * Get full name of the table/array/object. For example, the full name of a
	 * table in PostgreSQL consists of the schema and table name: schema.table.
	 * 
	 * @return Full name of the object.
	 */
	public String getName();

	/**
	 * Get ordered attributes for this object. This method must be idempotent
	 * (attributes should be static).
	 * 
	 * @return list of objects representing attributes
	 */
	public List<AttributeMetaData> getAttributesOrdered();

	/**
	 * Get ordered attributes representing dimensions for the array or another
	 * object.
	 * 
	 * @return list of attribute objects representing dimensions
	 */
	public default List<AttributeMetaData> getDimensionsOrdered() {
		/** by default, return an empty list of dimensions */
		return new ArrayList<>();
	}

	/**
	 * 
	 * @return All attributes including dimensions and common attributes.
	 */
	public default List<AttributeMetaData> getAllAttributesOrdered() {
		List<AttributeMetaData> allAttributes = getDimensionsOrdered();
		allAttributes.addAll(getAttributesOrdered());
		return allAttributes;
	}

}
