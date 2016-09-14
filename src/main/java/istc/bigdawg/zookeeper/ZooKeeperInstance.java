/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Singleton of the ZooKeeper service. Each node should hold only one
 *         session with ZooKeeper and this session maintains an ephemeral znode
 *         in ZooKeeper which denotes that this BigDAWG node is alive and can
 *         exchange message with the other BigDAWG nodes.
 *
 */
public enum ZooKeeperInstance {

	INSTANCE;

	/**
	 * The timeout for this client when maintaining the connection with
	 * ZooKeeper.
	 */
	private static final int sessionTimeout = 2000;

	private Logger logger = org.apache.log4j.Logger
			.getLogger(ZooKeeperInstance.class.getName());

	/** The instance of ZooKeeper session. */
	private ZooKeeper zooKeeper;

	private ZooKeeperInstance() {
		/*
		 * Set of zookeeper host addresses and their port numbers. These are
		 * physical nodes, not znodes.
		 */
		String zooKeeperNodes = BigDawgConfigProperties.INSTANCE
				.getZooKeepers();
		try {
			zooKeeper = ZooKeeperHandler.connect(zooKeeperNodes,
					sessionTimeout);
		} catch (IOException | InterruptedException e) {
			String stackTrace = StackTrace.getFullStackTrace(e);
			logger.error(
					"Could not establish connection to ZooKeeper ensemble. "
							+ LogUtils.replace(stackTrace));
			System.exit(1);
		}
	}

	/**
	 * 
	 * @return The instance of the connection to ZooKeeper.
	 */
	public ZooKeeper getZooKeeper() {
		return zooKeeper;
	}

	/**
	 * Close the connection to ZooKeeper.
	 */
	public void close() {
		try {
			zooKeeper.close();
		} catch (InterruptedException e) {
			String stackTrace = StackTrace.getFullStackTrace(e);
			logger.error("Could not close connection to ZooKeeper. "
					+ LogUtils.replace(stackTrace));
		}
	}
}
