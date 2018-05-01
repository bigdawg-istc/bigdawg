/**
 * 
 */
package istc.bigdawg.relational;

import java.io.Serializable;

/**
 * @author Kate Yu
 * 
 *         Deal with the name of a table and schema.
 */
public interface RelationalSchemaTableName extends Serializable {
	/**
	 * 
	 * @return schema name
	 */
	public String getSchemaName();

	/**
	 * 
	 * @return table name
	 */
	public String getTableName();
	
	public String getFullName();

}
