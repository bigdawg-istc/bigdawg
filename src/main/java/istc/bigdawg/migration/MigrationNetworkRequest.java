/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.network.NetworkObject;

/**
 * This is the request which is sent via network to migrate some data on a
 * remote host.
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 9, 2016 12:48:26 PM
 */
public interface MigrationNetworkRequest extends NetworkObject {

	public MigrationResult execute() throws MigrationException;
}
