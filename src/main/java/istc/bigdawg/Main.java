package istc.bigdawg;

import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.migration.MigratorTask;
import istc.bigdawg.monitoring.MonitoringTask;
import istc.bigdawg.network.NetworkUtils;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.zookeeper.NodeInfo;
import istc.bigdawg.zookeeper.ZooKeeperConnection;

/**
 * Main class.
 * 
 */
public class Main {

	public static final String BASE_URI;
	private static Logger logger;

	/*
	 * the object that represents an active connection of this BigDAWG node to
	 * ZooKeeper
	 */
	private static ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection();

	/**
	 * The timeout for this client when maintaining the connection with
	 * ZooKeeper.
	 */
	private static final int sessionTimeout = 2000;

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
	 * Register the node as BigDAWG active node in Zookeeper.
	 * 
	 * 
	 */
	public static void registerNodeInZooKeeper() {

		/*
		 * Set of zookeeper host addresses and their port numbers. These are
		 * physical nodes, not znodes.
		 */
		String zooKeeperNodes = BigDawgConfigProperties.INSTANCE
				.getZooKeepers();

		/* each node in BigDAWG is defined by a separate ipAddress and port. */
		String ipAddress = BigDawgConfigProperties.INSTANCE
				.getGrizzlyIpAddress();
		String port = BigDawgConfigProperties.INSTANCE.getGrizzlyPort();

		try {
			ZooKeeper zooKeeper = zooKeeperConnection.connect(zooKeeperNodes,
					sessionTimeout);
			NodeInfo nodeInfo = new NodeInfo();
			byte[] znodeData = NetworkUtils.serialize(nodeInfo);
			/* create the znode to denote the active BigDAWG node */
			zooKeeper.create("/BigDAWG/nodes/" + ipAddress + ":" + port,
					znodeData, ZooDefs.Ids.READ_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
		} catch (Exception ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error(LogUtils.replace(stackTrace));
		}

	}

	/**
	 * In a normal case, the znode should be removed at the end of the life of
	 * the node.
	 */
	public static void unregisterNodeInZooKeeper() {
		try {
			zooKeeperConnection.close();
		} catch (InterruptedException ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error(LogUtils.replace(stackTrace));
		}
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
		registerNodeInZooKeeper();
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
		unregisterNodeInZooKeeper();
		server.shutdownNow();
	}
}
