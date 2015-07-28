/**
 * 
 */
package istc.bigdawg.utils;

import istc.bigdawg.exceptions.ShellScriptException;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

/**
 * @author adam
 *
 */
public class RunShellScript {
	
	public static InputStream run(String filePath) throws IOException, InterruptedException, ShellScriptException {
		Process prop = new ProcessBuilder(filePath).start();
		prop.waitFor();
		int exitVal=prop.exitValue();
		if (exitVal != 0) {
			throw new ShellScriptException("Problem with the shell script: "+filePath+". Process returned value: "+exitVal);
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
			InputStream inStream=run(System.getProperty("user.dir")+"/scripts/test_script/vijay_query.sh");
			String result = IOUtils.toString(inStream, "UTF-8"); 
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
