/**
 * 
 */
package istc.bigdawg.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.SystemUtilities;
import istc.bigdawg.utils.TaskExecutor;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class DataInOutTest {

	/* log */
	private static Logger logger = Logger.getLogger(DataInOutTest.class);

	/* The OS path to temp directory */
	private String systemTempDir;

	/** Chunk size for the buffer - how much data we read/write in one go. */
	private final static int CHUNK_SIZE = 64 * 1024;

	@Before
	public void setUp() {
		LoggerSetup.setLogging();
		systemTempDir = SystemUtilities.getSystemTempDir();
	}

	/* The separate thread to receive the data. */
	Callable<Object> getReceiverCallable(String fullPathIn) {
		return () -> {
			logger.debug("Start receiving data.");
			DataIn.receive(4444, fullPathIn);
			return null;
		};
	}

	/** The separate thread to send the data. */
	Callable<Object> getSenderCallable(String fullPathOut, int sleepSeconds) {
		return () -> {
			/* the main part of the test */
			TimeUnit.SECONDS.sleep(sleepSeconds);
			logger.debug("Start sending data.");
			DataOut.send("localhost", 4444, fullPathOut);
			return null;
		};
	}

	/**
	 * Write bytes to the file.
	 * 
	 * @param filePath
	 *            - path to the file or pipe
	 * @param bytesWritten
	 *            - number of bytes written to the file/pipe
	 * 
	 * @throws IOException
	 */
	Callable<Object> getWriteBytesCallable(String filePath, long bytesWritten)
			throws IOException {
		return () -> {
			logger.debug("Start writing data to the pipe/file: " + filePath);
			File file = new File(filePath);
			byte[] bytes = new byte[CHUNK_SIZE];
			OutputStream out = new BufferedOutputStream(
					new FileOutputStream(file));
			long iterationNumber = bytesWritten / CHUNK_SIZE;
			for (int i = 0; i < iterationNumber; ++i) {
				logger.debug("iteration number in writes: " + i);
				out.write(bytes, 0, CHUNK_SIZE);
			}
			out.close();
			return null;
		};
	}

	/**
	 * Read bytes from the file.
	 * 
	 * @param filePath
	 *            path to a file/pipe.
	 * @return number of bytes read
	 * @throws IOException
	 */
	Callable<Object> getReadBytesCallable(String filePath) throws IOException {
		return () -> {
			logger.debug("Start reading data from the pipe/file: " + filePath);
			File file = new File(filePath);
			byte[] bytes = new byte[CHUNK_SIZE];
			InputStream in = new BufferedInputStream(new FileInputStream(file));
			long totalBytesRead = 0;
			int bytesRead;
			do {
				bytesRead = in.read(bytes);
				totalBytesRead += bytesRead;
			} while (bytesRead > 0);
			in.close();
			return totalBytesRead;
		};
	}

	/**
	 * Test if the basic data transfer via network works.
	 * 
	 * @throws IOException
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	@Test
	public void testBasicDataInOutTest()
			throws IOException, InterruptedException, ExecutionException {

		String dataSend = "dummy data";

		/* prepare the data to be sent */
		String fileNameOut = "__test_file_send.txt";
		String fullPathOut = systemTempDir + "/" + fileNameOut;
		File fileSend = new File(fullPathOut);
		fileSend.createNewFile();
		OutputStream ioSend = new BufferedOutputStream(
				new FileOutputStream(fileSend));
		ioSend.write(dataSend.getBytes());
		ioSend.close();

		/* prepare file to receive the data */
		String fileNameIn = "__test_file_receive.txt";
		String fullPathIn = systemTempDir + "/" + fileNameIn;
		File fileReceive = new File(fullPathIn);
		fileReceive.createNewFile();

		/* run the network transfer */
		List<Callable<Object>> tasks = new ArrayList<>();
		tasks.add(getReceiverCallable(fullPathIn));
		tasks.add(getSenderCallable(fullPathOut, 2));
		ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
		List<Future<Object>> results = TaskExecutor.execute(executor, tasks);
		assertNull(results.get(0).get());
		assertNull(results.get(1).get());

		/* read the received data */
		byte[] buffer = new byte[dataSend.length()];
		InputStream ioReceive = new BufferedInputStream(
				new FileInputStream(fileReceive));
		int numberOfBytesRead = ioReceive.read(buffer);
		assertEquals(dataSend.length(), numberOfBytesRead);
		String dataReceive = new String(buffer, StandardCharsets.UTF_8);
		logger.debug("Received string: " + dataReceive + " (send string: "
				+ dataSend + ")");
		assertEquals(dataSend, dataReceive);

		/* clean the resources */
		fileSend.delete();
		ioReceive.close();
		fileReceive.delete();
	}

	@Test
	/** Test transfer of big data via network. */
	public void testBigDataTransfer() throws IOException, InterruptedException,
			RunShellException, ExecutionException {
		String pipeOut = Pipe.INSTANCE.createAndGetFullName("data_out");
		String pipeIn = Pipe.INSTANCE.createAndGetFullName("data_in");

		/* 2 GB of data: 2 ^ 32 */
		long totalBytes = 2 * 1024 * 1024 * 1024;
		logger.debug("total bytes: " + totalBytes);
		long startTimeMigration = System.currentTimeMillis();
		List<Callable<Object>> tasks = new ArrayList<>();
		tasks.add(getReceiverCallable(pipeIn));
		tasks.add(getSenderCallable(pipeOut, 2));
		tasks.add(getWriteBytesCallable(pipeOut, totalBytes));
		tasks.add(getReadBytesCallable(pipeIn));
		ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
		List<Future<Object>> results = TaskExecutor.execute(executor, tasks);
		assertNull(results.get(0).get());
		assertNull(results.get(1).get());
		assertNull(results.get(2).get());
		assertEquals(totalBytes, results.get(3).get());
		long endTimeMigration = System.currentTimeMillis();
		long durationMsec = endTimeMigration - startTimeMigration;
		double speed = (totalBytes / 1024) / (durationMsec / 1000.0);
		logger.debug("speed: " + speed + " MB/sec");
	}

}
