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

import istc.bigdawg.zookeeper.ZooKeeperUtils;

/**
 * Main class.
 * 
 */
public class Main {

	public static final String BASE_URI;
	private static Logger logger;
	
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
		// providers in istc.bigdawg package
		final ResourceConfig rc = new ResourceConfig().packages("istc.bigdawg");

		// create and start a new instance of grizzly http server
		// exposing the Jersey application at BASE_URI
		System.out.println("base uri: " + BASE_URI);
		return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI),
				rc);
	}

	/**
	 * Main method.
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
		final HttpServer server = startServer();
		//ZooKeeperUtils.registerNodeInZooKeeper();
		MonitoringTask relationalTask = new MonitoringTask();
		relationalTask.run();
		MigratorTask migratorTask = new MigratorTask();
		logger.info("Server started");
		System.out.println(String.format(
				"Jersey app started with WADL available at "
						+ "%sapplication.wadl\nHit enter to stop it...",
				BASE_URI));
		System.in.read();
		CatalogInstance.INSTANCE.closeCatalog();
		migratorTask.close();
		//ZooKeeperUtils.unregisterNodeInZooKeeper();
		server.shutdownNow();
	}
}
