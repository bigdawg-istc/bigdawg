/**
 * 
 */
package istc.bigdawg.utils;

import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.exceptions.ShellScriptException;
import istc.bigdawg.properties.BigDawgConfigProperties;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

/**
 * @author adam
 *
 */
public class RunShell {
	
	public static InputStream run(String filePath, String database, String table, String query) throws IOException, InterruptedException, ShellScriptException {
		Process prop = new ProcessBuilder(filePath,database,table,query).start();
		prop.waitFor();
		int exitVal=prop.exitValue();
		if (exitVal != 0) {
			throw new ShellScriptException("Problem with the shell script: "+filePath+". Process returned value: "+exitVal);
		}
		return prop.getInputStream();
	}
	
    public static InputStream runSciDB(String host, String query) throws IOException, InterruptedException, SciDBException {
	query=query.replace("^^","'");
	Process prop = new ProcessBuilder("iquery","--host",host,"-aq",query).start();
		prop.waitFor();
		int exitVal=prop.exitValue();
		if (exitVal != 0) {
			throw new SciDBException("Problem iquery and parameters: "+host+" "+query+". Process returned value: "+exitVal);
		}
		return prop.getInputStream();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Present Project Directory : "+ System.getProperty("user.dir"));
		try {
			//InputStream inStream=run(System.getProperty("user.dir")+"/scripts/test_script/echo_script.sh");
			//InputStream inStream=run(System.getProperty("user.dir")+"/scripts/test_script/vijay_query.sh");
			System.out.println(BigDawgConfigProperties.INSTANCE.getAccumuloShellScript());
			InputStream inStream=run(BigDawgConfigProperties.INSTANCE.getAccumuloShellScript(),"classdb01","note_events_Tedge","Tedge('16965_recordTime_2697-08-04-00:00:00.0_recordNum_1_recordType_DISCHARGE_SUMMARY.txt,',:)");
			//InputStream inStream=run("/home/adam/Chicago/bigdawgmiddle/scripts/test_script/vijay_query.sh");
			String result = IOUtils.toString(inStream, Constants.ENCODING); 
			System.out.println(result);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch(ShellScriptException e) {
			e.printStackTrace();
		}
	}

}
