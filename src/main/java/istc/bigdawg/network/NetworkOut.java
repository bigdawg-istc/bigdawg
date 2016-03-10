/**
 * 
 */
package istc.bigdawg.network;

import static istc.bigdawg.network.NetworkUtils.serialize;
import static istc.bigdawg.network.NetworkUtils.deserialize;
import static istc.bigdawg.network.NetworkUtils.TIMEOUT;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * Send message (out via the network) to another host.
 * 
 * @author Adam Dziedzic
 */
public class NetworkOut {

	/* log */
	private static Logger log = Logger.getLogger(NetworkOut.class);

	/**
	 * Send the java object to the specified host.
	 * 
	 * @param object
	 * @param host
	 * @return another object result
	 * @throws NetworkException
	 */
	public static Object send(Object object, String host)
			throws NetworkException {
		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket requester = context.socket(ZMQ.REQ);
		String fullAddress = host + ":"
				+ BigDawgConfigProperties.INSTANCE.getNetworkMessagePort();
		try {
			requester.setLinger(TIMEOUT);
			log.debug("Connecting to host: " + fullAddress);
			requester.connect("tcp://" + fullAddress);
			// check if the connection is active (the heart beat)
			String errorMessage = " Probably, "
					+ BigDawgConfigProperties.PROJECT_NAME
					+ " is not running on the remote machine or the remote machine is inactive!"
					+ " The default timeout for the heart beat is: " + TIMEOUT;
			boolean isRemoteActive = false;
			try {
				isRemoteActive = (boolean) sendReceiveReply(new HeartBeat(),
						requester, TIMEOUT);
			} catch (NetworkException ex) {
				String exceptionMessge = "There was a problem when trying to check if the host: "
						+ fullAddress + " is active: " + ex.getMessage()
						+ errorMessage;
				throw new NetworkException(exceptionMessge);
			}
			if (!isRemoteActive) {
				String noConnectionMessage = "Could not connect to the host: "
						+ fullAddress + errorMessage;
				throw new NetworkException(noConnectionMessage);
			}
			// -1 is no timeout (wait forever)
			int timeout = -1;
			log.debug("send request and wait for the reply from: " + fullAddress);
			Object reply = sendReceiveReply(object, requester, timeout);
			log.debug("Reply was received from: " + fullAddress);
			return reply;
		} finally {
			requester.close();
			context.term();
		}
	}

	/**
	 * Send the message via network to a remote host and return the reply.
	 * 
	 * @param object
	 * @param requester
	 * @param timeout how long should we wait for the response
	 * @return the reply (the object returned from the remote host)
	 * @throws NetworkException
	 */
	private static Object sendReceiveReply(Object object, ZMQ.Socket requester,
			int timeout) throws NetworkException {
		boolean isSuccess = requester.send(serialize(object), 0);
		if (!isSuccess) {
			String message = "The message " + object.toString()
					+ " could not be sent!";
			log.error(message);
			throw new NetworkException(message);
		}
		requester.setReceiveTimeOut(timeout);
		byte[] replyBytes = requester.recv(0);
		if (replyBytes == null) {
			String message = "No reply was received (the message was sent: "
					+ object.toString() + "). The default timeout is: "
					+ TIMEOUT + " ms.";
			log.error(message);
			throw new NetworkException(message);
		}
		return deserialize(replyBytes);
	}

	public static void main(String[] args) {
		ZMQ.Context context = ZMQ.context(1);

		// Socket to talk to server
		System.out.println("Connecting to hello world server...");

		ZMQ.Socket requester = context.socket(ZMQ.REQ);
		requester.connect("tcp://localhost:5555");

		for (int requestNbr = 0; requestNbr != 10; requestNbr++) {
			String request = "Hello";
			System.out.println("Sending Hello " + requestNbr);
			requester.send(request.getBytes(), 0);

			byte[] reply = requester.recv(0);
			System.out.println(
					"Received " + new String(reply) + " " + requestNbr);
		}
		requester.close();
		context.term();
	}

}
