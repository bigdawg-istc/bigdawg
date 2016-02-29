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
public class TranformBinExecutor {

	@Test
	public void testTransformationFromPostgresToSciDB() throws InterruptedException, ExecutionException {
		ExecutorService executor = null;
		try {
			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					"src/main/cmigrator/data/test_int_double_string_from_postgres.bin",
					"src/main/cmigrator/data/test_int_double_string_to_scidb.bin",
					"int32,int32 null,double,double null,string,string null",
					TransformBinExecutor.TYPE.FromPostgresToSciDB);
			FutureTask<Long> transformTask = new FutureTask<Long>(transformExecutor);
			int minNumberOfThreads = 1;
			executor = Executors.newFixedThreadPool(minNumberOfThreads);
			executor.submit(transformTask);
			long exitValue = transformTask.get();
			System.out.println("Exit value postgres2scidb exitValue: " + exitValue);
			assertEquals(0, exitValue);
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}

	@Test
	public void testTransformationFromSciDBToPostgres() throws InterruptedException, ExecutionException {
		ExecutorService executor = null;
		try {
			// cross checks
			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					"src/main/cmigrator/data/test_int_double_string_from_scidb.bin",
					"src/main/cmigrator/data/test_int_double_string_to_postgres.bin",
					"int32,int32 null,double,double null,string,string null",
					TransformBinExecutor.TYPE.FromSciDBToPostgres);
			FutureTask<Long> transformTask = new FutureTask<Long>(transformExecutor);
			int minNumberOfThreads = 1;
			executor = Executors.newFixedThreadPool(minNumberOfThreads);
			executor.submit(transformTask);
			long exitValue = transformTask.get();
			System.out.println("Exit value scidb2postgres exitValue: " + exitValue);
			assertEquals(0, exitValue);
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}
}
