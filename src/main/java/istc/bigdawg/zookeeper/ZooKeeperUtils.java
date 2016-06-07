/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.IpAddressPort;
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

	/** Znode Path for this machine. */
	private static final String znodeBigDAWGNodesPath;

	/**
	 * Handle zookeeper operations.
	 */
	public static ZooKeeperHandler zooHandler;

	static {
		/* each node in BigDAWG is defined by a separate ipAddress and port. */
		String ipAddress = BigDawgConfigProperties.INSTANCE
				.getGrizzlyIpAddress();
		String port = BigDawgConfigProperties.INSTANCE.getGrizzlyPort();
		znodeBigDAWGNodesPath = ZooKeeperUtils.BigDAWGPath
				+ ZooKeeperUtils.nodes + "/" + ipAddress + ":" + port;
		zooHandler = new ZooKeeperHandler(
				ZooKeeperInstance.INSTANCE.getZooKeeper());
	}

	/**
	 * 
	 * @param ipAddress
	 * @param port
	 * @return full znode path for the given ipAddress and port
	 */
	public static String getZnodePathNodes(String ipAddress, String port) {
		return ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.nodes + "/"
				+ ipAddress + ":" + port;
	}

	/**
	 * Get path for a lock.
	 * 
	 * @param ipAddress
	 * @param port
	 * @return full znode path for the given ipAddress and port
	 */
	public static String getZnodePathMigrationLocks(String ipAddress,
			String port) {
		return ZooKeeperUtils.BigDAWGPath + ZooKeeperUtils.migrationLocks + "/"
				+ ipAddress + ":" + port;
	}

	/**
	 * see: {@link #getZnodePathNodes(String, String)}; we set default address
	 * which was given to BigDAWG (grizzly port)
	 * 
	 * @param ipAddress
	 * @return full znode path for the given ipAddress and port
	 */
	public static String getZnodePathNodes(String ipAddress) {
		return getZnodePathNodes(ipAddress,
				BigDawgConfigProperties.INSTANCE.getGrizzlyPort());
	}

	/**
	 * see: {@link #getZnodePathNodes(String, String)}; we set default address
	 * which was given to BigDAWG (grizzly port)
	 * 
	 * @param ipAddress
	 * @return full znode path for the given ipAddress and port
	 */
	public static String getZnodePathMigrationLocks(String ipAddress) {
		return getZnodePathMigrationLocks(ipAddress,
				BigDawgConfigProperties.INSTANCE.getGrizzlyPort());
	}

	/**
	 * Create the root znodes in ZooKeeper for BigDAWG.
	 * 
	 * @param zooHandler
	 * @throws NetworkException
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void createBigDAWGZnodes()
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
			createBigDAWGZnodes();
			NodeInfo nodeInfo = new NodeInfo(
					"By computer with name: " + SystemUtilities.getHostName());
			byte[] znodeData = NetworkUtils.serialize(nodeInfo);
			/* create the znode to denote the active BigDAWG node */
			zooHandler.createZnode(znodeBigDAWGNodesPath, znodeData,
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
		ZooKeeperHandler zooHandler = new ZooKeeperHandler(
				ZooKeeperInstance.INSTANCE.getZooKeeper());
		try {
			/*
			 * Try to clean/remove the znodes that were created for this BigDAWG
			 * nodes.
			 */
			zooHandler.deleteZnode(znodeBigDAWGNodesPath);
		} catch (KeeperException | InterruptedException ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error(LogUtils.replace(stackTrace));
		} finally {
			ZooKeeperInstance.INSTANCE.close();
		}
	}

	/**
	 * Lock the nodes for migration. The locking is executed on the server from
	 * which the data is migrated.
	 * 
	 * @param hostnameFrom
	 * @param hostnameTo
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public static List<String> acquireLockMigrationNodes(
			List<IpAddressPort> ipPortPairs)
					throws KeeperException, InterruptedException {
		List<String> locks = new ArrayList<String>();
		if (ipPortPairs.isEmpty()) {
			return locks;
		}
		/* our locking mechanism locks addresses in lexicographical order */
		String message = "lock created by: "
				+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress();
		byte[] data = message.getBytes();
		for (IpAddressPort ipPort : ipPortPairs) {
			String znodePath = ZooKeeperUtils.getZnodePathMigrationLocks(
					ipPort.getIpAddress(), ipPort.getPort());
			locks.add(zooHandler.getLock(znodePath, data));
		}
		return locks;
	}

	/**
	 * Release the lock for the migration process.
	 * 
	 * @param locks
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void releaseLockMigrationNodes(List<String> locks)
			throws KeeperException, InterruptedException {
		for (String lock : locks) {
			zooHandler.releaseLock(lock);
		}
	}

}
