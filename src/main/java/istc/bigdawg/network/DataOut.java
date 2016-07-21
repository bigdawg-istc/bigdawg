/**
 * 
 */
package istc.bigdawg.network;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.StackTrace;

/**
 * The client to send large data via network.
 *
 * @author Adam Dziedzic
 */
public class DataOut implements Callable<Object> {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(DataOut.class);

	/**
	 * Set timeout to wait for the socket to connect to a remote host (in
	 * Milliseconds).
	 */
	private static final int TIMEOUT;

	/**
	 * How many times should we retry the connection to the server to send the
	 * data?
	 */
	private static final int RETRY_CONNECTION;

	static {
		TIMEOUT = BigDawgConfigProperties.INSTANCE.getNetworkRequestTimeout();
		RETRY_CONNECTION = BigDawgConfigProperties.INSTANCE
				.getNetworkRetryConnection();
	}

	/** @see parameter host at {@link #send(String, int, String)} */
	private String host;

	/** @see parameter port at {@link #send(String, int, String)} */
	private int port;

	/** @see parameter filePath at {@link #send(String, int, String)} */
	private String filePath;

	/**
	 * Create the DataOut object that can read data from filePath and sent them
	 * via network to the (host,port).
	 * 
	 * @param host
	 * @see parameter host at {@link #send(String, int, String)}
	 * @param port
	 * @see parameter port at {@link #send(String, int, String)}
	 * @param filePath
	 * @see parameter filePath at {@link #send(String, int, String)}
	 */
	public DataOut(String host, int port, String filePath) {
		this.host = host;
		this.port = port;
		this.filePath = filePath;
	}

	/**
	 * Send data from this machine to a remote host.
	 * 
	 * @param host
	 *            The IP address of the remote machine (can be also localhost).
	 * @param port
	 *            The number of port for the remote machine.
	 * @param filePath
	 *            Full path to the file/pipe which should be transferred via
	 *            network.
	 * @return Total number of bytes sent via the network.
	 * @throws IOException
	 *             The socket for data transfer was not opened.
	 * @throws InterruptedException
	 */
	public static Long send(final String host, final int port,
			final String filePath) throws IOException, InterruptedException {
		/* Connect with timeout. */
		Socket socket = null;
		logger.debug(
				"Connection to socket with host: " + host + " port: " + port);
		for (int trialNumber = 0; trialNumber <= RETRY_CONNECTION; ++trialNumber) {
			try {
				/* The socket from which we send the data. */
				socket = new Socket();
				SocketAddress sockaddr = new InetSocketAddress(host, port);
				socket.connect(sockaddr, TIMEOUT * 3);
				break;
			} catch (Exception ex) {
				logger.info(ex.getMessage(), ex);
				TimeUnit.MILLISECONDS.sleep(TIMEOUT);
				try {
					socket.close();
				} catch (IOException e) {
					logger.error("Could not close (not-connected) socket. ", e);
				}
				if (trialNumber == RETRY_CONNECTION) {
					String message = "Problem with creating the socket to send data "
							+ "via network. There were: " + RETRY_CONNECTION
							+ " trials to reconnect.";
					logger.error(message + " " + ex.getMessage() + " "
							+ StackTrace.getFullStackTrace(ex), ex);
					try {
						socket.close();
					} catch (IOException e) {
						logger.error("Could not close (not-connected) socket. ",
								e);
					}
					throw ex;
				}
			}
		}

		File file = new File(filePath);
		byte[] bytes = new byte[64 * 1024];
		InputStream in = new BufferedInputStream(new FileInputStream(file));
		OutputStream out = socket.getOutputStream();
		/*
		 * Count how many bytes were read from the stream and how many bytes
		 * should be sent via network.
		 */
		int count;
		/* Count total number of bytes sent via network. */
		long totalCount = 0L;
		while ((count = in.read(bytes)) > 0)

		{
			out.write(bytes, 0, count);
			totalCount += count;
		}

		out.close();
		in.close();
		socket.close();
		return totalCount;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Object call() throws Exception {
		return send(host, port, filePath);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataOut [host=" + host + ", port=" + port + ", filePath="
				+ filePath + "]";
	}

}
