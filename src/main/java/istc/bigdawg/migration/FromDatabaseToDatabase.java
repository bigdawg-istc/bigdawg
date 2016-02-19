/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.query.ConnectionInfo;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public interface FromDatabaseToDatabase {

	MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
			String objectTo) throws MigrationException;
}
