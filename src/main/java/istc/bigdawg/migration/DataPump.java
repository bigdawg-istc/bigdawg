/**
 * 
 */
package istc.bigdawg.migration;

/**
 * Shim for migration represents a layer between physical connection to a
 * database and the remaining part of the code which sits above migrator.
 * 
 * Shim has to be able to check which physical database is supported and should
 * accept information about migration (migration info).
 * 
 * @author Adam Dziedzic
 */
public interface DataPump extends ConnectorChecker, SetMigrationInfo {

	/**
	 * Close all the resources allocated for data pump (migration/export/load
	 * etc.).
	 * 
	 * @throws Exception
	 *             For example, when a connection to a database cannot be
	 *             closed.
	 */
	default public void close() throws Exception {
	};

}
