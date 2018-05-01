/**
 * 
 */
package istc.bigdawg.relational;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;

import java.util.List;
import java.util.Map;

/**
 * Meta data about a table in a relational engine.
 * 
 * @author Kate Yu
 */
public interface RelationalTableMetaData extends ObjectMetaData {

	/**
	 * @return the schemaTable
	 */
	public RelationalSchemaTableName getSchemaTable();

	public Map<String, AttributeMetaData> getColumnsMap();

	@Override
	public String getName();

	@Override
	public List<AttributeMetaData> getAttributesOrdered();
}
