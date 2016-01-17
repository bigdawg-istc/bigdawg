/**
 * 
 */
package istc.bigdawg.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Adam Dziedzic
 * 
 *         Jan 17, 2016 1:38:15 PM
 */
public class StackTrace {

	public static String getFullStackTrace(Exception ex) {
		StringWriter sWriter = new StringWriter();
		PrintWriter pWriter = new PrintWriter(sWriter);
		ex.printStackTrace(pWriter);
		return sWriter.toString();
	}

}
