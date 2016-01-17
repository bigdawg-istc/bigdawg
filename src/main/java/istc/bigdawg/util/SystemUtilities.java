/**
 * 
 */
package istc.bigdawg.util;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;

/**
 * @author Adam Dziedzic
 * 
 *         Jan 13, 2016 10:58:09 AM
 */
public class SystemUtilities {

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
			return false;
		}
		return true;
	}

}
