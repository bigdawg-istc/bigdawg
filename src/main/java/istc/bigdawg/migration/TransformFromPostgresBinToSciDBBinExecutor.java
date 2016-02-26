/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 11:14:07 AM
 */
public class TransformFromPostgresBinToSciDBBinExecutor
		implements Callable<Long> {

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToPostgres.class);

	/* input: the path to the file with binary data from PostgreSQL */
	private final String postgresBinFilePath;
	/* output: the path to the file with binary data for SciDB */
	private final String scidbBinFilePath;
	/*
	 * the format/types of the attributes, for example:
	 * types=int32_t,int32_t:null,double,double:null,string,string
	 */
	private final String format;

	/**
	 * @param postgresBinFilePath
	 * @param scidbBinFilePath
	 * @param format
	 */
	public TransformFromPostgresBinToSciDBBinExecutor(
			String postgresBinFilePath, String scidbBinFilePath,
			String format) {
		this.postgresBinFilePath = postgresBinFilePath;
		this.scidbBinFilePath = scidbBinFilePath;
		this.format = format;
	}

	/**
	 * Transform data from PostgreSQL binary format to SciDB binary format.
	 * 
	 * @return 0 if process was executed correctly, -1 if something went wrong
	 */
	public Long call() {
		try {
			return (long) RunShell.runShellReturnExitValue(new ProcessBuilder(
					"src/main/cmigrator/postgres2scidb", "-i",
					postgresBinFilePath, "-o", scidbBinFilePath, "-f", format));
		} catch (IOException | InterruptedException ex) {
			ex.printStackTrace();
			log.error(
					"The binary transformation from PostgreSQL to SciDB failed: "
							+ ex.getMessage() + " "
							+ StackTrace.getFullStackTrace(ex),
					ex);
			return -1L;
		}
	}

	/**
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		ExecutorService executor = null;
		try {
		TransformFromPostgresBinToSciDBBinExecutor transformExecutor = new TransformFromPostgresBinToSciDBBinExecutor(
				"src/main/cmigrator/data/fromPostgresIntDoubleString.bin",
				"src/main/cmigrator/data/toSciDBIntDoubleString.bin,",
				"int32_t,int32_t:null,double,double:null,string,string");
		FutureTask<Long> transformTask = new FutureTask<Long>(
				transformExecutor);
		int minNumberOfThreads = 1;
		executor = Executors
				.newFixedThreadPool(minNumberOfThreads);
		executor.submit(transformTask);
		long exitValue = transformTask.get();
		System.out.println("Exit value postgres2scidb exitValue: " + exitValue);
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}

}
