/**
 * 
 */
package istc.bigdawg.network;

import static istc.bigdawg.network.NetworkUtils.THIS_HOST_ADDRESS;
import static istc.bigdawg.network.NetworkUtils.deserialize;
import static istc.bigdawg.network.NetworkUtils.serialize;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;

import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * Receive a message from the network and execute the received object.
 * 
 * @author Adam Dziedzic
 */
public class NetworkIn {

	/* log */
	private static Logger log = Logger.getLogger(NetworkIn.class);

	public static void main(String[] args) throws Exception {
		System.out.println("network in: receive");
		new NetworkIn().receive();
//		ZMQ.Context context = ZMQ.context(1);
//
//		// Socket to talk to clients
//		ZMQ.Socket responder = context.socket(ZMQ.REP);
//		responder.bind("tcp://*:5555");
//
//		while (!Thread.currentThread().isInterrupted()) {
//			// Wait for next request from the client
//			byte[] request = responder.recv(0);
//			System.out.println("Received Hello: " + new String(request));
//
//			// Do some 'work'
//			Thread.sleep(1000);
//
//			// Send reply back to client
//			String reply = "World";
//			responder.send(reply.getBytes(), 0);
//		}
//		responder.close();
//		context.term();
	}

	public void receive() throws NetworkException {
		log.debug("network in: start listening for requests");
		ZMQ.Context context = ZMQ.context(1);

		// Socket to talk to clients
		ZMQ.Socket responder = context.socket(ZMQ.REP);
		responder.bind("tcp://*:"
				+ BigDawgConfigProperties.INSTANCE.getNetworkMessagePort());

		while (!Thread.currentThread().isInterrupted()) {
			// Wait for next request from the client
			byte[] requestBytes = responder.recv(0);
			if (requestBytes == null) {
				log.error("ZeroMQ: The message was not received properly!");
			}
			Object requestObject = null;
			try {
				requestObject = deserialize(requestBytes);
				NetworkObject requestCommand = (NetworkObject) requestObject;
				Object result = requestCommand.execute();
				// Send reply back
				boolean isSuccess = responder.send(serialize(result), 0);
				if (!isSuccess) {
					log.error("ZeroMQ: The response was not sent properly!");
				}
			} catch (NetworkException ex) {
				String message = "The request could not be processed properly! "
						+ ex.getMessage();
				log.error(message);
				byte[] exBytes = message.getBytes();
				/* try to send the exception message */
				try {
					exBytes = serialize(ex);
				} catch (NetworkException exSerialize) {
					exSerialize.printStackTrace();
				}
				boolean isSuccess = responder.send(exBytes, 0);
				if (!isSuccess) {
					log.error(
							"ZeroMQ: The response (from NetworkException) was not sent properly!");
				}
			} catch (Exception e) {
				e.printStackTrace();
				String message = "The request command could not be executed on the remote server (host: "
						+ THIS_HOST_ADDRESS + "). " + e.getMessage();
				log.error(message);
				boolean isSuccess = responder.send(serialize(message), 0);
				if (!isSuccess) {
					log.error(
							"ZeroMQ: The response (from Exception) was not sent properly!");
				}
			}
		}
		responder.close();
		context.term();
	}

}
