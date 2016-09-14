/**
 * 
 */
package istc.bigdawg.network;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.NetworkException;

/**
 * This Callable object should be executed in a separate thread. The intention
 * is that we wait for the response in a thread but in another thread we control
 * if the remote machine to which we sent the request is up and running. If the
 * remote machine fails, then we stop the process.
 * 
 * @param hostname
 *            to which node/machine we should send the network request
 * @return result (response) in reply to the request
 * 
 * @author Adam Dziedzic
 */
public class RemoteRequest implements Callable<Object> {

	/*
	 * log
	 */
	private static Logger log = Logger.getLogger(DataOut.class);

	/**
	 * @see @param networkObject at {@link #RemoteRequest(NetworkObject, String)
	 */
	private NetworkObject networkObject;

	/**
	 * @see @param hostname at {@link #RemoteRequest(NetworkObject, String)}
	 */
	private String hostname;

	/**
	 * 
	 * @param networkObject
	 *            Object that should be sent via network.
	 * @param hostname
	 *            The name of the host to which we should send the request.
	 */
	public RemoteRequest(NetworkObject networkObject, String hostname) {
		this.networkObject = networkObject;
		this.hostname = hostname;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Object call() {
		try {
			log.debug("Send object: " + networkObject.toString()
					+ " via network to node: " + hostname);
			Object result = NetworkOut.send(networkObject, hostname);
			return result;
		} catch (NetworkException e) {
			return e;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RemoteRequest [networkObject=" + networkObject + ", hostname="
				+ hostname + "]";
	}

}
