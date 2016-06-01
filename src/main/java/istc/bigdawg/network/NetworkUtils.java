/**
 * 
 */
package istc.bigdawg.network;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.StackTrace;

/**
 * Network utilities to send/receive Java objects via network.
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 9, 2016 1:13:35 PM
 */
public class NetworkUtils {

	/* log */
	private static Logger log = Logger.getLogger(NetworkUtils.class);

	/** the address of this host/machine */
	public static InetAddress THIS_HOST_ADDRESS;

	/** the default timeout to wait for a reply from a server */
	public static int TIMEOUT = 1000;

	static {
		try {
			THIS_HOST_ADDRESS = InetAddress.getLocalHost();
			TIMEOUT = BigDawgConfigProperties.INSTANCE
					.getNetworkRequestTimeout();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Change Java object into bytes.
	 * 
	 * @param object
	 * @return
	 * @throws NetworkException
	 */
	public static byte[] serialize(Object object) throws NetworkException {
		ByteArrayOutputStream binOut = new ByteArrayOutputStream();
		ObjectOutput out;
		try {
			out = new ObjectOutputStream(binOut);
			out.writeObject(object);
		} catch (IOException ex) {
			ex.printStackTrace();
			String message = "The object " + object.toString()
					+ " could not be serialized. (host: " + THIS_HOST_ADDRESS
					+ "). " + ex.getMessage();
			log.error(message + " " + StackTrace.getFullStackTrace(ex), ex);
			throw new NetworkException(message);
		}
		return binOut.toByteArray();
	}

	/**
	 * Change bytes into Java object.
	 * 
	 * @param bytes
	 * @return
	 * @throws NetworkException
	 */
	public static Object deserialize(byte[] bytes) throws NetworkException {
		ByteArrayInputStream binIn = new ByteArrayInputStream(bytes);
		ObjectInput in;
		try {
			in = new ObjectInputStream(binIn);
			return in.readObject();
		} catch (IOException | ClassNotFoundException ex) {
			ex.printStackTrace();
			String message = "The bytes given (bytes: " + bytes.toString()
					+ ") could not be deserialized (host: " + THIS_HOST_ADDRESS
					+ "). " + ex.getMessage();
			log.error(message + StackTrace.getFullStackTrace(ex));
			throw new NetworkException(message);
		}
	}

	/**
	 * Check if the given address is pointing to this local machine.
	 * 
	 * @param addr
	 *            the address to be checked
	 * @return true if the address is pointing to the local machine, false
	 *         otherwise
	 */
	public static boolean isThisMyIpAddress(InetAddress addr) {
		/* check if the address is a valid special local or loop back */
		if (addr.isAnyLocalAddress() || addr.isLoopbackAddress()) {
			return true;
		}
		/* check if the address is defined on any interface */
		try {
			return NetworkInterface.getByInetAddress(addr) != null;
		} catch (SocketException ex) {
			return false;
		}
	}

}
