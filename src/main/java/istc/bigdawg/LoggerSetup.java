/**
 * 
 */
package istc.bigdawg;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class LoggerSetup {
	private static Logger logger;

	public static void setLogging() throws IOException {
		logger = Logger.getLogger(LoggerSetup.class);
		String log4JPropertyFile = "bigdawg-log4j.properties";
		Properties prop = new Properties();
		InputStream inputStream = LoggerSetup.class.getClassLoader().getResourceAsStream(log4JPropertyFile);
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException(
					"log4j property file '" + log4JPropertyFile + "' not found in the classpath");
		}
		PropertyConfigurator.configure(prop);
		logger.info("Starting application. Logging was configured!");
	}

}
