/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;

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

	/**
	 * 
	 * @return true if file was deleted
	 */
	public static boolean deleteFileIfExists(String fileName) {
		try {
			Files.deleteIfExists(FileSystems.getDefault().getPath(fileName));
		} catch (IOException e) {
			e.printStackTrace();
			log.error(e.getMessage() + " Could nor remove file: " + fileName + StackTrace.getFullStackTrace(e));
			return false;
		}
		return true;
	}

}
