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

	private String systemTempDir;

	@Before
	private void setUp() {
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

}
