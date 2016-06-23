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
	 * Get full name of the table/array/object. For example, the full name of a
	 * table in PostgreSQL consists of the schema and table name: schema.table.
	 * 
	 * @return Full name of the object.
	 */
	public String getName();

	/**
	 * Get ordered attributes for this object. If the attributes are not
	 * changed, then each call of the method should return the same result.
	 */
	public List<AttributeMetaData> getAttributesOrdered();
}
