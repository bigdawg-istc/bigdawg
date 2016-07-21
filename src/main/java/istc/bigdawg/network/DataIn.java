/**
 * 
 */
package istc.bigdawg.network;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import istc.bigdawg.utils.StackTrace;
import scala.annotation.meta.param;

/**
 * The server which receives (large amount of) data from network.
 * 
 * @author Adam Dziedzic
 */
public class DataIn implements Callable<Object> {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(DataIn.class);

	/** Chunk size for the buffer to receive data. */
	private final static int CHUNK_SIZE = 64 * 1024;

	/**
	 * @see param port in {@link #receive(int, String)}
	 */
	private int port;

	/**
	 * @see param filePath in {@link #receive(int, String)}
	 */
	private String filePath;

	/**
	 * Create an instance of DataIn which will take data from network and relay
	 * it to the filePath.
	 * 
	 * @param port
	 * @see parameter port in {@link #receive(int, String)}
	 * @param filePath
	 * @see parameter filePath in {@link #receive(int, String)}
	 */
	public DataIn(int port, String filePath) {
		this.port = port;
		this.filePath = filePath;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public Object call() throws Exception {
		return receive(port, filePath);
	}

	/**
	 * Receive data to this machine from a remote host. Write the data received
	 * from network to the filePath.
	 * 
	 * @param port
	 *            The number of port on which we should listen to get the data
	 *            transfer request.
	 * @param filePath
	 *            Full path to the file/pipe to which we should write the data
	 *            received via network.
	 * @return Total number of bytes received from the network.
	 * @throws IOException
	 *             The socket for data transfer was not opened.
	 */
	public static Long receive(final int port, final String filePath)
			throws IOException {
		ServerSocket serverSocket = null;
		Socket socket = null;
		InputStream in = null;
		OutputStream out = null;
		try {
			try {
				serverSocket = new ServerSocket(port);
			} catch (IOException e) {
				String message = "Could not open socket (bounded to port: "
						+ port + ") server to receive data from network. ";
				logger.error(message + e.getMessage() + " "
						+ StackTrace.getFullStackTrace(e), e);
				throw e;
			}
			try {
				socket = serverSocket.accept();
			} catch (IOException e) {
				String message = "Socket problem when waiting for"
						+ " an incoming connection. ";
				logger.error(message + e.getMessage()
						+ StackTrace.getFullStackTrace(e), e);
				throw e;
			}
			try {
				in = socket.getInputStream();
			} catch (IOException e) {
				String message = "Could not get input stream from the socket "
						+ "to read the data from the network. ";
				logger.error(message + e.getMessage()
						+ StackTrace.getFullStackTrace(e), e);
				throw e;
			}
			try {
				out = new BufferedOutputStream(new FileOutputStream(filePath));
			} catch (FileNotFoundException e) {
				String message = "Did not find the target file to write the data. ";
				logger.error(message + e.getMessage()
						+ StackTrace.getFullStackTrace(e), e);
				throw e;
			}
			byte[] bytes = new byte[CHUNK_SIZE];
			/*
			 * Count how many bytes were read from the socket and how many bytes
			 * should be written to the stream.
			 */
			int count;
			/* Total number of bytes received from network. */
			long totalCount = 0L;
			try {
				while ((count = in.read(bytes)) > 0) {
					out.write(bytes, 0, count);
					totalCount += count;
				}
			} catch (IOException e) {
				String message = "Problem when reading data from the "
						+ "socket and writing to the file. ";
				logger.error(message + e.getMessage()
						+ StackTrace.getFullStackTrace(e), e);
				throw e;
			}
			logger.debug("All data were received from network (port: " + port
					+ " ; data relaied to filePath:" + filePath + ")");
			return totalCount;
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					String message = "Could not close the output file to which "
							+ "data were written from the network. ";
					logger.error(message + e.getMessage() + " "
							+ StackTrace.getFullStackTrace(e), e);
				}
			}
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					String message = "Could not close the input stream from "
							+ "the socket which received data via network. ";
					logger.error(message + e.getMessage() + " "
							+ StackTrace.getFullStackTrace(e), e);
				}
			}
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					String message = "Could not close the socket for "
							+ "receiving data from network.";
					logger.error(message + e.getMessage() + " "
							+ StackTrace.getFullStackTrace(e), e);
				}
			}
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					String message = "Could not close the server socket.";
					logger.error(message + e.getMessage() + " "
							+ StackTrace.getFullStackTrace(e), e);
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataIn [port=" + port + ", filePath=" + filePath + "]";
	}
}
