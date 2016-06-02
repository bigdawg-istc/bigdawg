/**
 * 
 */
package istc.bigdawg.migration;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;
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

	/**
	 * Execute the command (it should migrate some data).
	 */
	public MigrationResult execute() throws MigrationException;

	/**
	 * Process the result returned by the remote request to migrate some data.
	 * 
	 * @param result
	 *            the result (Object) returned by the request.
	 * @return {@link MigrationResult} if request was successful
	 * @throws MigrationException
	 */
	default MigrationResult processResult(Object result)
			throws MigrationException {
		/* log */
		Logger log = Logger.getLogger(MigrationNetworkRequest.class);
		if (result instanceof MigrationResult) {
			return (MigrationResult) result;
		}
		if (result instanceof MigrationException) {
			throw (MigrationException) result;
		}
		if (result instanceof NetworkException) {
			NetworkException ex = (NetworkException) result;
			log.error("Problem on the other host: " + ex.getMessage());
			throw new MigrationException(
					"Problem on the other host: " + ex.getMessage());
		}
		String message = "Migration was executed on a remote host but the returned result is unexepcted: "
				+ result.toString();
		log.error(message);
		throw new MigrationException(message);
	}
}
