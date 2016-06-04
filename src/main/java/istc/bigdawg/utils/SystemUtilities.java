/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author Adam Dziedzic
 * 
 *         Jan 13, 2016 10:58:09 AM
 */
public class SystemUtilities {

	/* log */
	private static Logger log = Logger.getLogger(SystemUtilities.class);

	/**
	 * 
	 * @return operating system temporary directory / folder
	 */
	public static String getSystemTempDir() {
		String property = "java.io.tmpdir";
		String tempDir = System.getProperty(property);
		return tempDir;
	}

	/*
	 * @return name of this machine
	 */
	@SuppressWarnings("unused")
	private String getHostName() {
		Map<String, String> env = System.getenv();
		if (env.containsKey("COMPUTERNAME")) {
			return env.get("COMPUTERNAME");
		}
		else if (env.containsKey("HOSTNAME")) {
			return env.get("HOSTNAME");
		}
		else {
			return "Unknown Computer";
		}
	}

	/**
	 * 
	 * @return true if file was deleted, false if the file was not deleted
	 *         because it did not exist
	 * @throws IOException
	 */
	public static boolean deleteFileIfExists(String fileName)
			throws IOException {
		try {
			return Files
					.deleteIfExists(FileSystems.getDefault().getPath(fileName));
		} catch (IOException e) {
			e.printStackTrace();
			log.error(e.getMessage() + " Could nor remove file: " + fileName
					+ StackTrace.getFullStackTrace(e));
			throw e;
		}
	}

}
