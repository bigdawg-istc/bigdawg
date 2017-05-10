/**
 * Task to run the main thread of BigDAWG for tests.
 */
package istc.bigdawg.system;

import static istc.bigdawg.Main.main;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import istc.bigdawg.utils.StackTrace;

/**
 * Run the main task in separate threads for testing.
 * 
 * @author Adam Dziedzic
 * 
 */
public class MainTask implements Runnable {

	/**
	 * log
	 */
	private static Logger log = Logger.getLogger(MainTask.class.getName());

	/* Executor to run the main thread in. */
	private ExecutorService executor = null;

	/**
	 * Run the service for migrator - this network task accepts remote request
	 * for data migration.
	 */
	public MainTask() {
		/*
		 * Creates a thread pool that creates new threads as needed, but will
		 * reuse previously constructed threads when they are available.
		 */
		executor = Executors.newCachedThreadPool();
		executor.submit(this);
	}

	/**
	 * Close the main task.
	 */
	public void close() {
		if (executor != null) {
			if (!executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
		executor = null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			main(new String[0]);
		} catch (IOException e) {
			String msg = "There might be another instance of BigDAWG already "
					+ "running on the machine. Check the problem: "
					+ e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			System.exit(1);
		}
	}

}