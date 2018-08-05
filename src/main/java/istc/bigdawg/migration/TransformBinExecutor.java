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

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 26, 2016 11:14:07 AM
 */
public class TransformBinExecutor implements Callable<Long> {

	/* log */
	private static Logger log = Logger.getLogger(TransformBinExecutor.class);

	/* input: the path to the file with input binary data */
	private final String inputBinPath;
	/* output: the path to the file with output binary data */
	private final String outputBinPath;
	/*
	 * the format/types of the attributes, for example:
	 * types=int32_t,int32_t:null,double,double:null,string,string
	 */
	private final String binFormat;

	/** Path to the migrator. */
	static private String path;

	static {
		path = BigDawgConfigProperties.INSTANCE.getCmigratorDir();
	}

	/**
	 * the type of migration: specify between which databases it should be
	 * executed
	 */
	private final TYPE type;

	public enum TYPE {
		/**
		 * the type is the name of the c++ program which executes a given
		 * migration type
		 */
		FromPostgresToSciDB("postgres2scidb"),
		FromRESTToSciDB("rest2scidb"),
		FromSciDBToPostgres("scidb2postgres");

		/** this represents the full path to the c++ migrator */
		private final String type;

		private TYPE(String type) {
			this.type = type;
			log.debug("type: " + this.type);
		}

		public String toString() {
			return type;
		}

	}

	/**
	 * @param inputBinPath
	 * @param outputBinPath
	 * @param binFormat
	 *            the format/types of the attributes, for example:
	 *            types=int32_t,int32_t:null,double,double:null,string,string
	 */
	public TransformBinExecutor(String inputBinPath, String outputBinPath,
			String binFormat, TYPE type) {
		this.inputBinPath = inputBinPath;
		this.outputBinPath = outputBinPath;
		this.binFormat = binFormat;
		this.type = type;
	}

	/**
	 * Transform data from PostgreSQL binary format to SciDB binary format.
	 * 
	 * @return 0 if process was executed correctly, -1 if something went wrong
	 */
	public Long call() {
		try {
			/*
			 * The attributes from the format have to enclosed in a quotation
			 * marks to be read as a single argument.
			 */
			log.debug("transformation command: " + path
					+ "src/main/data-migrator-exe" + " -t " + type.toString()
					+ " -i " + inputBinPath + " -o " + outputBinPath + " -f "
					+ binFormat);
			return (long) RunShell.runShellReturnExitValue(new ProcessBuilder(
					path + "src/main/data-migrator-exe", "-t", type.toString(),
					"-i", inputBinPath, "-o", outputBinPath, "-f", binFormat));
		} catch (IOException | InterruptedException ex) {
			ex.printStackTrace();
			log.error("The binary transformation " + type.name() + " failed: "
					+ ex.getMessage() + " " + StackTrace.getFullStackTrace(ex),
					ex);
			return -1L;
		}
	}

	/**
	 * @param args
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void main(String[] args)
			throws InterruptedException, ExecutionException {
		LoggerSetup.setLogging();
		ExecutorService executor = null;
		try {
			// TransformFromPostgresBinToSciDBBinExecutor transformExecutor =
			// new TransformFromPostgresBinToSciDBBinExecutor(
			// "src/main/cmigrator/data/fromPostgresIntDoubleString.bin",
			// "src/main/cmigrator/data/toSciDBIntDoubleString.bin",
			// "int32_t,int32_t null,double,double null,string,string null");
			executor = Executors.newSingleThreadExecutor();
			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					"/home/adam/data/region_postgres.bin",
					"/home/adam/data/region_scidb.bin",
					"int64 null,string,string", TYPE.FromPostgresToSciDB);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);
			long exitValue = transformTask.get();
			System.out.println(
					"Exit value postgres2scidb exitValue: " + exitValue);
		} finally {
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
	}

}
