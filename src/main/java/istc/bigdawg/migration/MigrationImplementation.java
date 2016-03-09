/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.exceptions.MigrationException;

/**
 * The interface for Migration Implementation.
 * 
 * @author Adam Dziedzic
 */
public interface MigrationImplementation {

	/**
	 * migrate data
	 * 
	 * @return {@link MigrationResult} the information about migration
	 */
	public MigrationResult migrate() throws MigrationException;

}
