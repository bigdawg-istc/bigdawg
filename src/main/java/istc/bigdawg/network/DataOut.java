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
import java.net.Socket;

import org.apache.log4j.Logger;

import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         The client to send large data via network.
 */
public class DataOut {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(DataOut.class);

	/**
	 * Send data from this machine to a remote host.
	 * 
	 * @param host
	 *            The ip address of the remote machine (can be also localhost).
	 * @param port
	 *            The number of port for the remote machine.
	 * @param filePath
	 *            Full path to the file/pipe which should be transferred via
	 *            network.
	 * @throws IOException
	 *             The socket for data transfer was not opened.
	 */
	public static void send(final String host, final int port,
			final String filePath) throws IOException {
		/* The socket from which we send the data. */
		Socket socket = null;

		try {
			socket = new Socket(host, port);
		} catch (IOException e) {
			String message = "Problem with creating the socket to send data via network.";
			logger.error(message + " " + e.getMessage() + " "
					+ StackTrace.getFullStackTrace(e), e);
			throw e;
		}

		File file = new File(filePath);
		byte[] bytes = new byte[64 * 1024];
		InputStream in = new BufferedInputStream(new FileInputStream(file));
		OutputStream out = socket.getOutputStream();

		int count;
		while ((count = in.read(bytes)) > 0) {
			out.write(bytes, 0, count);
		}

		out.close();
		in.close();
		socket.close();
	}

}
