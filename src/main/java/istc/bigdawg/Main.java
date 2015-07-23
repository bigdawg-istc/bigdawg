package istc.bigdawg;

import istc.bigdawg.stream.MemStreamDAO;
import istc.bigdawg.stream.StreamDAO;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import org.apache.accumulo.core.client.impl.thrift.ThriftTest.AsyncProcessor.throwsError;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Main class.
 * 
 */
public class Main {

	// Base URI the Grizzly HTTP server will listen on
	public static String BASE_URI;

	public static StreamDAO streamDAO = null;

	/**
	 * Starts Grizzly HTTP server exposing JAX-RS resources defined in this
	 * application.
	 * 
	 * @return Grizzly HTTP server.
	 * @throws IOException
	 */
	public static HttpServer startServer() throws IOException {
		// create a resource config that scans for JAX-RS resources and
		// providers
		// in istc.bigdawg package
		final ResourceConfig rc = new ResourceConfig().packages("istc.bigdawg");

		// create and start a new instance of grizzly http server
		// exposing the Jersey application at BASE_URI
		streamDAO = getStreamDAO();
		Properties prop = new Properties();
		String propFileName = "config.properties";
		InputStream inputStream = Main.class.getClassLoader()
				.getResourceAsStream(propFileName);
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propFileName
					+ "' not found in the classpath");
		}
		String grizzlyIpaddress = prop.getProperty("main.grizzly.ipaddress");
		String grizzlyPort = prop.getProperty("main.grizzly.port");
		BASE_URI = "http://" + grizzlyIpaddress + ":" + grizzlyPort
				+ "/bigdawg/";
		System.out.println("base uri: " + BASE_URI);
		return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI),
				rc);
	}

	public static StreamDAO getStreamDAO() {
		if (streamDAO == null) {
			streamDAO = new MemStreamDAO();
		}
		return streamDAO;
	}

	private static void setLogging() throws IOException {
		Logger logger = Logger.getLogger(Main.class);
		String log4JPropertyFile = "bigdawg-log4j.properties";
		Properties prop = new Properties();
		InputStream inputStream = Main.class.getClassLoader()
				.getResourceAsStream(log4JPropertyFile);
		if (inputStream != null) {
			prop.load(inputStream);
		} else {
			throw new FileNotFoundException("log4j property file '"
					+ log4JPropertyFile + "' not found in the classpath");
		}
		PropertyConfigurator.configure(prop);
		logger.info("Starting application. Logging was configured!");
	}

	/**
	 * Main method.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		setLogging();
		final HttpServer server = startServer();
		System.out.println(String.format(
				"Jersey app started with WADL available at "
						+ "%sapplication.wadl\nHit enter to stop it...",
				BASE_URI));
		System.in.read();
		server.shutdownNow();
	}
}
