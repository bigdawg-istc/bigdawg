/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

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

	/** Znode Path for this machine - indicates active node. */
	private static final String znodeBigDAWGNodesPath;

	/**
	 * Znode Path for this machine - denotes a node for which a lock can be
	 * acquired.
	 */
	private static final String znodeBigDAWGMigrationLocksPath;

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
		znodeBigDAWGMigrationLocksPath = ZooKeeperUtils.BigDAWGPath
				+ ZooKeeperUtils.migrationLocks + "/" + ipAddress + ":" + port;
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
			String info = "The znode was created by node: "
					+ BigDawgConfigProperties.INSTANCE.getGrizzlyIpAddress();
			byte[] znodeData = info.getBytes();
			/* delete any ephemeral znode which was not garbage collected */
			zooHandler.deleteZnode(znodeBigDAWGNodesPath);
			/* create the znode to denote the active BigDAWG node */
			zooHandler.createZnode(znodeBigDAWGNodesPath, znodeData,
					ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL);
			/*
			 * create the znode to denote the active BigDAWG node for migration
			 * locks
			 */
			zooHandler.deleteZnodeWithChildren(znodeBigDAWGMigrationLocksPath);
			zooHandler.createZnode(znodeBigDAWGMigrationLocksPath, znodeData,
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
			 * node.
			 */
			zooHandler.deleteZnode(znodeBigDAWGNodesPath);
			List<String> children = zooHandler
					.getChildren(znodeBigDAWGMigrationLocksPath);
			for (String child : children) {
				zooHandler.deleteZnode(
						znodeBigDAWGMigrationLocksPath + "/" + child);
			}
			zooHandler.deleteZnode(znodeBigDAWGMigrationLocksPath);
		} catch (KeeperException | InterruptedException ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error(
					"Could not clean znodes in ZooKeeper for this machine: "
							+ BigDawgConfigProperties.INSTANCE
									.getGrizzlyIpAddress()
							+ "Exception info: " + ex.getMessage()
							+ LogUtils.replace(stackTrace));
		} finally {
			ZooKeeperInstance.INSTANCE.close();
		}
	}

	/**
	 * Acquire locks in ZooKeeper for data migration.
	 * 
	 * @param ipAddresses
	 *            list of BigDAWG nodes to be locked for the migration.
	 * @param data
	 *            data to be put in the locks
	 * @return Names of znodes that represents locks in ZooKeeper (empty list
	 *         represents local execution).
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static List<String> acquireMigrationLocks(List<String> ipAddresses,
			byte[] data) throws KeeperException, InterruptedException {
		/* IP address on this very machine where the code is running. */
		String thisIpAddress = BigDawgConfigProperties.INSTANCE
				.getGrizzlyIpAddress();
		if (thisIpAddress.equals("localhost")
				|| thisIpAddress.equals("127.0.0.1")) {
			/* this BigDAWG instance is running only locally. */
			return new ArrayList<>();
		}
		String port = BigDawgConfigProperties.INSTANCE.getGrizzlyPort();
		/*
		 * The addresses to acquire the locks for have to be unique and sorted
		 * (so we use the tree set directly without the set interface). If the
		 * migration is executed only within one node (the source and target
		 * databases are on the same node) then only the lock should be acquired
		 * for this single machine.
		 */
		TreeSet<IpAddressPort> ipAddressPortPairs = new TreeSet<>();
		for (String ipAddress : ipAddresses) {
			ipAddressPortPairs.add(new IpAddressPort(ipAddress, port));
		}
		return ZooKeeperUtils.acquireMigrationLocks(ipAddressPortPairs, data);
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
	private static List<String> acquireMigrationLocks(
			TreeSet<IpAddressPort> ipPortPairs, byte[] data)
					throws KeeperException, InterruptedException {
		assert ipPortPairs != null;
		List<String> locks = new ArrayList<String>();
		if (ipPortPairs.isEmpty()) {
			return locks;
		}
		/* our locking mechanism locks addresses in lexicographical order */
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
	 * @param zooKeeperLocks
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void releaseMigrationLocks(List<String> zooKeeperLocks)
			throws KeeperException, InterruptedException {
		if (zooKeeperLocks == null) {
			return;
		}
		for (String lock : zooKeeperLocks) {
			zooHandler.releaseLock(lock);
		}
	}

}
