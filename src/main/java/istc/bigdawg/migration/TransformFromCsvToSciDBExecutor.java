/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.utils.RunShell;

/**
 * Transform a file in CSV format into SciDB (array like) format.
 * 
 * @author Adam Dziedzic
 */
public class TransformFromCsvToSciDBExecutor implements Callable<Object> {

	/* log */
	private static Logger log = Logger
			.getLogger(TransformFromCsvToSciDBExecutor.class);

	/*
	 * format of data in the csv file, it specifies number of columns and their
	 * types: for example: NNNS denotes that 3 first columns in CSV file are
	 * number and the last one is a string
	 */
	private String typesPattern;
	/* path to the csv file exported from PostgreSQL */
	private String csvFilePath;
	/*
	 * delimiter in the csv file (the separator between fields in the csv file)
	 */
	private String delimiter;
	/*
	 * path to the file in SciDB format, this is the destination of the output
	 * of this executor
	 */
	private String scidbFilePath;
	/*
	 * path to the binary file for SciDB that converts a file in CSV format to a
	 * file in SciDB format
	 */
	private String scidbBinPath;

	/**
	 * 
	 * @param typesPattern
	 *            format of data in the csv file, it specifies number of columns
	 *            and their types: for example: NNNS denotes that 3 first
	 *            columns in CSV file are number and the last one is a string
	 * @param csvFilePath
	 *            path to the csv file exported from PostgreSQL
	 * @param delimiter
	 *            delimiter in the csv file (the separator between fields in the
	 *            csv file)
	 * @param scidbFilePath
	 *            path to the binary file for SciDB that converts a file in CSV
	 *            format to a file in SciDB format
	 * @param scidbBinPath
	 *            path to the binary file for SciDB that converts a file in CSV
	 *            format to a file in SciDB format
	 */
	public TransformFromCsvToSciDBExecutor(String typesPattern,
			String csvFilePath, String delimiter, String scidbFilePath,
			String scidbBinPath) {
		this.typesPattern = typesPattern;
		this.csvFilePath = csvFilePath;
		this.delimiter = delimiter;
		this.scidbFilePath = scidbFilePath;
		this.scidbBinPath = scidbBinPath;
	}

	/*
	 * 
	 * @throws MigrationException thrown when conversion from csv to scidb
	 * format fails
	 */
	public Long call() throws MigrationException {
		ProcessBuilder csv2scidb = new ProcessBuilder(
				scidbBinPath + "csv2scidb", "-i", csvFilePath, "-o",
				scidbFilePath, "-p", typesPattern, "-d",
				"\"" + delimiter + "\"");
		log.debug("Command - csv to scidb transformation for loading:"
				+ csv2scidb.command());
		try {
			Long returnValue = (long) RunShell
					.runShellReturnExitValue(csv2scidb);
			if (returnValue != 0) {
				String message = "Conversion from csv to scidb format failed! "
						+ this.toString();
				log.error(message);
				throw new MigrationException(message);
			}
			return returnValue;
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
			String message = "Conversion from csv to scidb format failed! "
					+ e.getMessage() + " " + this.toString();
			log.error(message);
			throw new MigrationException(message);
		}

	}

	@Override
	public String toString() {
		return "TransformFromCsvToSciDBExecutor [typesPattern=" + typesPattern
				+ ", csvFilePath=" + csvFilePath + ", delimiter=" + delimiter
				+ ", scidbFilePath=" + scidbFilePath + ", scidbBinPath="
				+ scidbBinPath + "]";
	}

	/**
	 * @param args
	 * @throws MigrationException
	 * @throws IOException
	 */
	public static void main(String[] args)
			throws MigrationException, IOException {
		LoggerSetup.setLogging();
		SciDBConnectionInfo connectionInfo = new SciDBConnectionInfo();
		String scidbBinPath = connectionInfo.getBinPath();
		String typesPattern = "NSS";
		String csvFilePath = "src/test/resources/region.csv";
		String scidbFilePath = "src/test/resources/region_test.scidb";
		String delimiter = "|";
		TransformFromCsvToSciDBExecutor executor = new TransformFromCsvToSciDBExecutor(
				typesPattern, csvFilePath, delimiter, scidbFilePath,
				scidbBinPath);
		long result = executor.call();
		System.out.println("result of the execution: " + result);
	}

}
