/**
 * 
 */
package istc.bigdawg.migration;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public interface SetMigrationInfo {

	/**
	 * Set information about the migration process (connections from/to, tables
	 * from/to, etc.).
	 * 
	 * @param migrationInfo
	 */
	public void setMigrationInfo(MigrationInfo migrationInfo);
}
