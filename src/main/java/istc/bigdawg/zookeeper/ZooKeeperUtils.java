/**
 * 
 */
package istc.bigdawg.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkUtils;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.SystemUtilities;
import istc.bigdawg.zookeeper.znode.NodeInfo;

/**
 * @author Adam Dziedzic
 * 
 *         Utilities for handling specific BigDAWG needs in ZooKeeper.
 */
public class ZooKeeperUtils {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(ZooKeeperUtils.class);

	/** The starting path for BigDAWG in ZooKeeper. */
	public static final String BigDAWGPath = "/BigDAWG";

	/**
	 * The trace for active nodes in ZooKeeper. ZooKeeper periodically exchanges
	 * heart beat messages with the nodes so we can monitor all the time which
	 * nodes are alive in BigDAWG.
	 */
	public static final String nodes = "/nodes";

	/**
	 * At a given point in time, a given node can participate only in a single
	 * data migration task. The other nodes have to wait for the lock.
	 * 
	 * Use sequential nodes for this purpose.
	 */
	public static final String migrationLocks = "/migrationLocks";

	/**
	 * Source of the truth about tables - where the table is stored.
	 */
	public static final String tables = "/tables";

	private static final String znodePath;

	static {
		/* each node in BigDAWG is defined by a separate ipAddress and port. */
		String ipAddress = BigDawgConfigProperties.INSTANCE
				.getGrizzlyIpAddress();
		String port = BigDawgConfigProperties.INSTANCE.getGrizzlyPort();
		znodePath = ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.nodes + "/"
				+ ipAddress + ":" + port;
	}

	/**
	 * 
	 * @param ipAddress
	 * @param port
	 * @return full znode path for the given ipAddress and port
	 */
	public static String getZnodePath(String ipAddress, String port) {
		return ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.nodes + "/"
				+ ipAddress + ":" + port;
	}

	/**
	 * see: {@link #getZnodePath(String, String)}; we set default address which
	 * was given to BigDAWG (grizzly port)
	 * 
	 * @param ipAddress
	 * @return full znode path for the given ipAddress and port
	 */
	public static String getZnodePath(String ipAddress) {
		return getZnodePath(ipAddress,
				BigDawgConfigProperties.INSTANCE.getGrizzlyPort());
	}

	/**
	 * Handle zookeeper operations.
	 */
	public static ZooKeeperHandler zooHandler;

	/**
	 * Create the root znodes in ZooKeeper for BigDAWG.
	 * 
	 * @param zooHandler
	 * @throws NetworkException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void createBigDAWGZnodes(ZooKeeperHandler zooHandler)
			throws NetworkException, KeeperException, InterruptedException {
		zooHandler.createZnodeIfNotExists(ZooKeeperUtils.BigDAWGPath,
				NetworkUtils.serialize(new NodeInfo("BigDAWG")),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		/** create sub-nodes for BigDAWG */
		zooHandler.createZnodeIfNotExists(
				ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.nodes,
				NetworkUtils.serialize(new NodeInfo("nodes")),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zooHandler.createZnodeIfNotExists(
				ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.migrationLocks,
				NetworkUtils.serialize(new NodeInfo("migrationLocks")),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zooHandler.createZnodeIfNotExists(
				ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.tables,
				NetworkUtils.serialize(new NodeInfo("tables")),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	/**
	 * Register the node as BigDAWG active node in Zookeeper.
	 */
	public static void registerNodeInZooKeeper() {

		try {
			ZooKeeperHandler zooHandler = new ZooKeeperHandler(
					ZooKeeperInstance.INSTANCE.getZooKeeper());
			ZooKeeperUtils.createBigDAWGZnodes(zooHandler);
			NodeInfo nodeInfo = new NodeInfo(
					"By computer with name: " + SystemUtilities.getHostName());
			byte[] znodeData = NetworkUtils.serialize(nodeInfo);
			/* create the znode to denote the active BigDAWG node */
			zooHandler.createZnode(znodePath, znodeData,
					ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (Exception ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error("Initialization of BigDAWG in ZooKeeper failed. "
					+ LogUtils.replace(stackTrace));
			System.exit(1);
		}
	}

	/**
	 * In a normal case, the znode should be removed at the end of the life of
	 * the node.
	 * 
	 * @throws KeeperException
	 */
	public static void unregisterNodeInZooKeeper() {
		try {
			/*
			 * Try to clean/remove the znodes that were created for this BigDAWG
			 * nodes.
			 */
			zooHandler.deleteZnode(znodePath);
		} catch (KeeperException | InterruptedException ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error(LogUtils.replace(stackTrace));
		} finally {
			ZooKeeperInstance.INSTANCE.close();
		}
	}

}
