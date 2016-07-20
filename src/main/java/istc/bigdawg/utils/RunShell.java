/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.AccumuloShellScriptException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * @author Adam Dziedzic
 * 
 */
public class RunShell {

	private static Logger log = Logger.getLogger(RunShell.class);

	/**
	 * Runs a shell script and returns the stream of data returned by the called
	 * program.
	 * 
	 * @param procBuilder
	 * @return the stream of data returned by the process
	 * @throws RunShellException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static InputStream runShell(ProcessBuilder procBuilder)
			throws RunShellException, InterruptedException, IOException {
		
		System.out.printf("procBuilder: %s\n", procBuilder.command());
		
		Process prop = procBuilder.start();
		int exitVal = prop.waitFor();
		if (exitVal != 0) {
			throw new RunShellException("Process returned value: " + exitVal);
		}
		return prop.getInputStream();
	}

	/**
	 * Run a shell script (program from a shell command line) and return the
	 * value returned by the process.
	 * 
	 * @param procBuilder
	 * @return the value (int) returned by the process
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static int runShellReturnExitValue(ProcessBuilder procBuilder) throws IOException, InterruptedException {
		Process prop = procBuilder.start();
		prop.waitFor();
		return prop.exitValue();
	}

	/* makes a FIFO special file with name pathname */
	public static int mkfifo(String name) throws IOException, InterruptedException {
		return runShellReturnExitValue(new ProcessBuilder("mkfifo", name));
	}

	public static InputStream runAccumuloScript(String filePath, String database, String table, String query)
			throws IOException, InterruptedException, AccumuloShellScriptException {
		try {
			return runShell(new ProcessBuilder(filePath, database, table, query));
		} catch (RunShellException e) {
			e.printStackTrace();
			String msg = "Problem with the shell script: " + filePath + " database: " + database + " table: " + table
					+ " query: " + query + " " + e.getMessage();
			log.error(msg);
			throw new AccumuloShellScriptException(msg);
		}
	}
	
	public static InputStream runNewAccumuloScript(String filePath, List<String> databaseTableAndQuery)
			throws IOException, InterruptedException, AccumuloShellScriptException {
		try {
			int size = databaseTableAndQuery.size();
			if (size == 4)
				return runShell(new ProcessBuilder(filePath, databaseTableAndQuery.get(0), databaseTableAndQuery.get(1), databaseTableAndQuery.get(2), databaseTableAndQuery.get(3)));
			else if (size == 3)
				return runShell(new ProcessBuilder(filePath, databaseTableAndQuery.get(0), databaseTableAndQuery.get(1), databaseTableAndQuery.get(2)));
			else 
				throw new AccumuloShellScriptException(String.format("Invalid input size: %s; parametres: %s\n", size, databaseTableAndQuery));
		} catch (RunShellException e) {
			e.printStackTrace();
			String msg = "Problem with the new shell script runner: " + filePath + " input entries: " + databaseTableAndQuery + " " + e.getMessage();
			log.error(msg);
			throw new AccumuloShellScriptException(msg);
		}
	}

	public static InputStream runSciDBAFLquery(String host, String port, String binPath, String query)
			throws IOException, InterruptedException, SciDBException {
		String msg = "host: " + host + " query in runSciDB: " + query + " SciDB bin path: " + binPath;
		log.info(msg);
		/*
		 * there were problems with the ' so it was supposed to be rplaced with
		 * ^^ in the input
		 */
		query = query.replace("^^", "'");
		try {
			return runShell(new ProcessBuilder(binPath + "iquery", "--host", host, "--port", port, "--afl --query",
					query, "-o", "tsv+"));
		} catch (RunShellException e) {
			e.printStackTrace();
			msg = "Problem iquery and parameters: " + msg + " " + e.getMessage();
			log.error(msg);
			throw new SciDBException(msg);
		}
	}

	/**
	 * Execute AQL command in SciDB without returning any data.
	 * 
	 * @param host
	 * @param port
	 * @param command
	 * @return
	 * @throws SciDBException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static InputStream executeAQLcommandSciDB(String host, String port, String binPath, String command)
			throws SciDBException, IOException, InterruptedException {
		String msg = "command to be executed in SciDB: " + command.replace("'", "") + "; on host: " + host + " port: "
				+ port + " SciDB bin path: " + binPath;
		log.info(msg);
		try {
			return runShell(new ProcessBuilder(binPath + "iquery", "--host", host, "--port", port, "--no-fetch",
					"--query", command));
		} catch (RunShellException e) {
			e.printStackTrace();
			msg = "Error for: " + msg + " " + e.getMessage();
			log.error(msg);
			throw new SciDBException(msg);
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		LoggerSetup.setLogging();
		System.out.println("Present Project Directory : " + System.getProperty("user.dir"));
		try {
			// InputStream
			// inStream=run(System.getProperty("user.dir")+"/scripts/test_script/echo_script.sh");
			// InputStream
			// inStream=run(System.getProperty("user.dir")+"/scripts/test_script/vijay_query.sh");
			System.out.println(BigDawgConfigProperties.INSTANCE.getAccumuloShellScript());
			InputStream inStream = runAccumuloScript(BigDawgConfigProperties.INSTANCE.getAccumuloShellScript(),
					"classdb01", "note_events_Tedge",
					"Tedge('16965_recordTime_2697-08-04-00:00:00.0_recordNum_1_recordType_DISCHARGE_SUMMARY.txt,',:)");
			// InputStream
			// inStream=run("/home/adam/Chicago/bigdawgmiddle/scripts/test_script/vijay_query.sh");
			String result = IOUtils.toString(inStream, Constants.ENCODING);
			System.out.println(result);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (AccumuloShellScriptException e) {
			e.printStackTrace();
		}
	}

}
