/**
 * 
 */
package istc.bigdawg.network;

import static istc.bigdawg.network.NetworkUtils.TIMEOUT;
import static istc.bigdawg.network.NetworkUtils.deserialize;
import static istc.bigdawg.network.NetworkUtils.serialize;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import istc.bigdawg.LoggerSetup;
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
	 * First, it checks if the connection is active by sending the HeartBeat
	 * message and waits TIMEOUT (specified in the application configuration
	 * file) for a reply.
	 * 
	 * Second, it sends the specified object and waits infinitely for a
	 * response.
	 * 
	 * @param object
	 *            object to be sent via network (it is the control layer for the
	 *            data migration)
	 * @param host
	 *            - the IP address or name of the host which will receive the
	 *            message (if no problem with network)
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
			/*
			 * first check if the connection is active before sending the real
			 * object (the heart beat message is used)
			 */
			String errorMessage = " Probably, "
					+ BigDawgConfigProperties.PROJECT_NAME
					+ " is not running on the remote machine or the remote machine is inactive!"
					+ " The default timeout for the heart beat is: " + TIMEOUT;
			boolean isRemoteActive = false;
			try {
				isRemoteActive = (boolean) sendWaitForReply(new HeartBeat(),
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
			/* -1 is no timeout (wait forever) */
			int timeout = -1;
			log.debug(
					"the receiver is active now; send request and wait for the reply from: "
							+ fullAddress);

			Object reply = sendWaitForReply(object, requester, timeout);
			log.debug("Reply was received from: " + fullAddress);
			log.debug("Reply content: " + reply);
			return reply;
		} finally {
			requester.close();
			context.term();
		}
	}

	/**
	 * This is another way how to send a message. The
	 * {@link #send(Object, String) send} method is preferred.
	 * 
	 * Send the message via network to a remote host, wait for the reply for the
	 * specified amount of time and return the result.
	 * 
	 * @param object
	 *            to be sent
	 * @param requester
	 *            the zeromq object for networking
	 * @param timeout
	 *            how long should we wait for the response
	 * @return the reply (the object returned from the remote host)
	 * @throws NetworkException
	 *             if something bad happened
	 */
	private static Object sendWaitForReply(Object object, ZMQ.Socket requester,
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

	public static void main(String[] args)
			throws NetworkException, IOException {
		LoggerSetup.setLogging();

		/* Socket to talk to the server */
		System.out.println("Connecting to hello world server...");

		Object result = NetworkOut.send(new DebugMessage("hello"),
				BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress());
		String resultString = (String) result;
		log.info("The result returned: " + resultString);

		/*
		 * ZMQ.Socket requester = context.socket(ZMQ.REQ); String fullAddress =
		 * "tcp://" + BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress() +
		 * ":" + BigDawgConfigProperties.INSTANCE.getNetworkMessagePort();
		 * requester.connect(fullAddress);
		 * 
		 * for (int requestNbr = 0; requestNbr != 1; requestNbr++) { String
		 * request = "Hello"; System.out.println("Sending Hello " + requestNbr);
		 * requester.send(request.getBytes(StandardCharsets.UTF_8), 0);
		 * 
		 * byte[] reply = requester.recv(0); System.out.println( "Received " +
		 * new String(reply, StandardCharsets.UTF_8) + " " + requestNbr); }
		 * requester.close(); context.term();
		 */
	}

}
