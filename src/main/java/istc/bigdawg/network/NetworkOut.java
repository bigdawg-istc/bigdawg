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
 * Send message (out to the network).
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
		log.debug("Connecting to host: " + host);
		// Socket to talk to server
		ZMQ.Socket requester = context.socket(ZMQ.REQ);
		requester.setLinger(0);
		requester.connect("tcp://" + host + ":"
				+ BigDawgConfigProperties.INSTANCE.getNetworkMessagePort());
		boolean isSuccess = requester.send(serialize(object), 0);
		if (!isSuccess) {
			String message = "The message " + object.toString()
					+ " was not sent!";
			log.error(message);
			throw new NetworkException(message);
		}
		//requester.setReceiveTimeOut(TIMEOUT);
		byte[] replyBytes = requester.recv(0);
		if (replyBytes == null) {
			String message = "No reply was received (the message sent was: "
					+ object.toString() + "). The default timeout is: " + TIMEOUT + " ms.";
			log.error(message);
			throw new NetworkException(message);
		}
		requester.close();
		context.term();
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
