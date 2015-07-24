package istc.bigdawg;

import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.stream.MemStreamDAO;
import istc.bigdawg.stream.Stream;
import istc.bigdawg.stream.StreamDAO;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

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

	public static StreamDAO streamDAO ;
	public static final String BASE_URI;
	static {
		BASE_URI = BigDawgConfigProperties.INSTANCE.getBaseURI();
	}

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
		streamDAO = MemStreamDAO.INSTANCE;
		
		System.out.println("base uri: "+BASE_URI);
		return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI),
				rc);
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
		// show current classpath
		ClassLoader cl = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) cl).getURLs();
		System.out.println("Class-paths:");
		for (URL url : urls) {
			System.out.println(url.getFile());
		}
		System.out.println("The end of class-paths.");

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
