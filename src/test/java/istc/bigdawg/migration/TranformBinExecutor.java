/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 1:59:12 PM
 */
public class TranformBinExecutor {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(TransformBinExecutor.class);

	@Before
	/**
	 * Prepare the test data in an array in SciDB.
	 * 
	 * @throws SQLException
	 * @throws IOException
	 */
	public void beforeTests() throws SQLException, IOException {
		LoggerSetup.setLogging();
	}

	@Test
	public void testTransformationFromPostgresToSciDB()
			throws InterruptedException, ExecutionException {
		ExecutorService executor = null;
		logger.debug(
				"Simple test for binary migraion from PostgreSQL to SciDB.");
		try {
			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					"src/main/cmigrator/data/test_int_double_string_from_postgres.bin",
					"src/main/cmigrator/data/test_int_double_string_to_scidb.bin",
					"int32,int32 null,double,double null,string,string null",
					TransformBinExecutor.TYPE.FromPostgresToSciDB);
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

	@Test
	public void testTransformationFromSciDBToPostgres()
			throws InterruptedException, ExecutionException {
		ExecutorService executor = null;
		try {
			// cross checks
			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					"src/main/cmigrator/data/test_int_double_string_from_scidb.bin",
					"src/main/cmigrator/data/test_int_double_string_to_postgres.bin",
					"int32,int32 null,double,double null,string,string null",
					TransformBinExecutor.TYPE.FromSciDBToPostgres);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			int minNumberOfThreads = 1;
			executor = Executors.newFixedThreadPool(minNumberOfThreads);
			executor.submit(transformTask);
			long exitValue = transformTask.get();
			System.out.println(
					"Exit value scidb2postgres exitValue: " + exitValue);
			assertEquals(0, exitValue);
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}
}
