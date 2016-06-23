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

}
