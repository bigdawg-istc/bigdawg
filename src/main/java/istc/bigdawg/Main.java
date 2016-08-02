package istc.bigdawg;

import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.migration.MigratorTask;
import istc.bigdawg.monitoring.MonitoringTask;
import istc.bigdawg.properties.BigDawgConfigProperties;

/**
 * Main class.
 * 
 */
public class Main {

	public static String BASE_URI;
	private static Logger logger;

	static {
		BASE_URI = BigDawgConfigProperties.INSTANCE.getBaseURI();
	}

	/**
	 * see: {@link #startServer(String)}
	 * 
	 * @return Grizzly HTTP server.
	 * @throws IOException
	 */
	public static HttpServer startServer() throws IOException {
		return startServer(null);
	}

	/**
	 * Starts Grizzly HTTP server exposing JAX-RS resources defined in this
	 * application.
	 * 
	 * @param ipAddress
	 *            The IP address on the Grizzly server should wait on requests.
	 * @return Grizzly HTTP server.
	 * @throws IOException
	 */
	public static HttpServer startServer(String ipAddress) throws IOException {
		// exposing the Jersey application at BASE_URI
		if (ipAddress != null) {
			BASE_URI = BigDawgConfigProperties.INSTANCE.getBaseURI(ipAddress);
		}
		logger.info("base uri: " + BASE_URI);

		// create a resource config that scans for JAX-RS resources and
		// providers in istc.bigdawg package
		final ResourceConfig rc = new ResourceConfig().packages("istc.bigdawg");

		// create and start a new instance of grizzly http server
		return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI),
				rc);
	}

	/**
	 * Main method.
	 * 
	 * There is an optional one argument (args[0]) which can store the ip
	 * address on which the grizzly server should wait for requests.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// show current classpath
		// ClassLoader cl = ClassLoader.getSystemClassLoader();
		// URL[] urls = ((URLClassLoader) cl).getURLs();
		// System.out.println("Class-paths:");
		// for (URL url : urls) {
		// System.out.println(url.getFile());
		// }
		// System.out.println("The end of class-paths.");
		LoggerSetup.setLogging();
		logger = Logger.getLogger(Main.class);
		logger.info("Starting application ...");
		CatalogInstance.INSTANCE.getCatalog();
		String ipAddress = null;
		logger.debug("args length: " + args.length);
		if (args.length > 0) {
			logger.debug("args 0: " + args[0]);
			ipAddress = args[0];
		}
		final HttpServer server = startServer(ipAddress);
		// ZooKeeperUtils.registerNodeInZooKeeper();
		MonitoringTask relationalTask = new MonitoringTask();
		relationalTask.run();
		MigratorTask migratorTask = new MigratorTask();
		Thread migratorT = new Thread(migratorTask);
		migratorT.start();
		logger.info("Server started");
		System.out.println(String.format(
				"Jersey app started with WADL available at "
						+ "%sapplication.wadl\nHit enter to stop it...",
				BASE_URI));
		System.in.read();
		CatalogInstance.INSTANCE.closeCatalog();
		migratorTask.close();
		// ZooKeeperUtils.unregisterNodeInZooKeeper();
		server.shutdownNow();
	}
}
