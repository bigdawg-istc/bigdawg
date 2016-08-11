/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.AccumuloShellScriptException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.myria.MyriaHandler;
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
		
//		// DEBUG START
//		System.out.printf("procBuilder: %s\n", procBuilder.command());
//		Process prop0 = (new ProcessBuilder("ls", "/home/gridsan/groups/istcdata/tmp/")).start();
//		if (prop0.waitFor() != 0)
//			System.out.printf("----======= NON ZERO EXIT CODE ========----\n");
//		else 
//			System.out.printf("----======= OK ========----\nInputStream: %s\n", IOUtils.toString(prop0.getInputStream(), Constants.ENCODING));
//		// DEBUG END
		
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
	
//	public static InputStream runMyriaCommand(List<String> databaseTableAndQuery)
	public static InputStream runMyriaCommand(String curlParameters, String name, boolean isQuery, boolean isDownload)
			throws IOException, InterruptedException, JSONException, BigDawgException {
		
		InputStream scriptResultInStream;
		try {
			System.out.printf("\nbeginning commands: %s;\n\n", Arrays.asList(curlParameters.split("@@@")));
			scriptResultInStream = runShell(new ProcessBuilder(curlParameters.split("@@@")));
		} catch (RunShellException e) {
			e.printStackTrace();
			String msg = "Problem with the new shell script runner: curl " + curlParameters;
			log.error(msg);
			throw new BigDawgException(msg);
		}
		
		if (isQuery ^ isDownload) return scriptResultInStream;
		else if (! isQuery && ! isDownload) throw new BigDawgException("Myria command not query and not download: "+name+"; "+curlParameters);
		
		// else we're talking about a tracked query
		
		String myriaQueryHost = BigDawgConfigProperties.INSTANCE.getMyriaHost();
		String myriaQueryPort = BigDawgConfigProperties.INSTANCE.getMyriaPort();
//		String myriaDownloadPort = BigDawgConfigProperties.INSTANCE.getMyriaDownloadPort();
		
		String scriptResult = IOUtils.toString(scriptResultInStream, Constants.ENCODING);
		
		System.out.printf("\ncommands: %s; \nscriptResult from server: %s\n\n", Arrays.asList(curlParameters.split("@@@")), scriptResult);
		
		JSONObject obj = new JSONObject (scriptResult);
		String status = obj.getString("status");
		String queryID = obj.getString("queryId");
		
		int sleepInterval = 250;
		while (!status.equalsIgnoreCase("SUCCESS") && !status.equalsIgnoreCase("ERROR")) {
			Thread.sleep(sleepInterval); 
			
			try {
				System.out.printf("status: %s; queryId: %s; sleepInterval: %s\n", status, queryID, sleepInterval);
				scriptResultInStream = runShell(new ProcessBuilder(String.format(MyriaHandler.myriaInquieryString, myriaQueryHost, myriaQueryPort, queryID).split("@@@")));
			} catch (RunShellException e) {
				e.printStackTrace();
				String msg = "Problem with constant inquery: " + String.format(MyriaHandler.myriaInquieryString, myriaQueryHost, myriaQueryPort, queryID);
				log.error(msg);
				throw new BigDawgException(msg);
			}
			
			scriptResult = IOUtils.toString(scriptResultInStream, Constants.ENCODING);
			
			obj = new JSONObject (scriptResult);
			status = obj.getString("status");
			
			if (sleepInterval < 4000) sleepInterval *= 2;
		}
		
		System.out.printf("EXITED status: %s; queryId: %s; sleepInterval: %s\n", status, queryID, sleepInterval);
		
		if (status.equalsIgnoreCase("ERROR")) {
			System.out.printf("Error exit; scriptResult: %s\n", scriptResult);
			return scriptResultInStream;
		}
		
		
		System.out.printf("NO ERROR status: %s; queryId: %s\n", status, queryID);
		
		// download the query
		try {
			System.out.printf("Downloading... queryId: %s; query: %s\n", queryID, String.format(MyriaHandler.myriaDataRetrievalString, myriaQueryHost, myriaQueryPort, name));
			return runShell(new ProcessBuilder(String.format(MyriaHandler.myriaDataRetrievalString, myriaQueryHost, myriaQueryPort, name).split("@@@")));
		} catch (RunShellException e) {
			e.printStackTrace();
			String msg = "Problem with downloading the data: " + String.format(MyriaHandler.myriaDataRetrievalString, myriaQueryHost, myriaQueryPort, name);
			log.error(msg);
			throw new BigDawgException(msg);
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
