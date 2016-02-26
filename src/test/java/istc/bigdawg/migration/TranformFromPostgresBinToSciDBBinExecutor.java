/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Test;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 1:59:12 PM
 */
public class TranformFromPostgresBinToSciDBBinExecutor {

	@Test
	public void testTransformation()
			throws InterruptedException, ExecutionException {
		ExecutorService executor = null;
		try {
			TransformFromPostgresBinToSciDBBinExecutor transformExecutor = new TransformFromPostgresBinToSciDBBinExecutor(
					"src/main/cmigrator/data/fromPostgresIntDoubleString.bin",
					"src/main/cmigrator/data/toSciDBIntDoubleString.bin,",
					"int32_t,int32_t:null,double,double:null,string,string");
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			int minNumberOfThreads = 1;
			executor = Executors.newFixedThreadPool(minNumberOfThreads);
			executor.submit(transformTask);
			long exitValue = transformTask.get();
			System.out.println(
					"Exit value postgres2scidb exitValue: " + exitValue);
			assertEquals(0, exitValue);
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}

}
