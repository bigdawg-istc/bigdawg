/**
 * 
 */
package istc.bigdawg;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class LoggerSetup {
	private static Logger logger;

	public static void setLogging() {
		logger = Logger.getLogger(LoggerSetup.class);
		/* turn off the excessive logging from ZooKeeper client! */
		Logger.getLogger("org.apache.zookeeper.ClientCnxn").setLevel(Level.INFO);
		String log4JPropertyFile = "bigdawg-log4j.properties";
		Properties prop = new Properties();
		ClassLoader classLoader = LoggerSetup.class.getClassLoader();
		String testStr = classLoader.toString();
		InputStream inputStream = LoggerSetup.class.getClassLoader()
				.getResourceAsStream(log4JPropertyFile);
		if (inputStream != null) {
			try {
				prop.load(inputStream);
			} catch (IOException e) {
				System.err.println(
						"Wrong properties file for log4j - check your settings for logging.");
				System.exit(1);
			}
		} else {
			System.err.println("log4j property file '" + log4JPropertyFile
					+ "' not found in the classpath");
			System.exit(1);
		}
		PropertyConfigurator.configure(prop);
		logger.info("Logging was configured!");
	}

}
