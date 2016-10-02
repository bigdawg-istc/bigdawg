/**
 * 
 */
package istc.bigdawg.migration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import istc.bigdawg.network.NetworkIn;

/**
 * @author Adam Dziedzic
 * 
 *         Mar 9, 2016 7:20:55 PM
 */
public class MigratorTask {

	private ExecutorService executor = null;
	
	/* How many threads should we use to run the task. */
	int numberOfThreads = 1;

	/**
	 * Run the service for migrator - this network task accepts remote request
	 * for data migration.
	 */
	public MigratorTask() {
		executor = Executors.newFixedThreadPool(numberOfThreads);
		executor.submit(new NetworkIn());
	}

	/**
	 * Close the migrator task.
	 */
	public void close() {
		if (executor != null) {
			if (!executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
		executor = null;
	}

}
