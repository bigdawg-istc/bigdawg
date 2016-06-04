/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Handle interaction with ZooKeeper.
 */
public class ZooKeeperHandler {
	/* log */
	private static Logger log = Logger.getLogger(ZooKeeperHandler.class);

	/** instance for ZooKeeper class */
	private ZooKeeper zk;

	/**
	 * Initialize the ZooKeeper handler with a ZooKeeper instance.
	 */
	public ZooKeeperHandler(ZooKeeper zk) {
		this.zk = zk;
	}

	/**
	 * Create a znode in ZooKeeper ensemble, with default ACL - completely open
	 * and in persistent mode.
	 */
	public void createZnode(String path, byte[] data)
			throws KeeperException, InterruptedException {
		this.createZnode(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
	}

	/** Create znode with all parameters provided. */
	public void createZnode(String path, byte[] data, List<ACL> acl,
			CreateMode createMode)
					throws KeeperException, InterruptedException {
		zk.create(path, data, acl, createMode);
	}

	/**
	 * Create a znode in ZooKeeper ensemble if it does not already exists.
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void createZnodeIfNotExists(String path, byte[] data, List<ACL> acl,
			CreateMode createMode)
					throws KeeperException, InterruptedException {
		/* check if the znode exists */
		Stat stat = this.znodeExists(path);
		if (stat != null) {
			log.info("The znode with path: '" + path + "' already exists.");
		} else {
			log.info("There is no znode with the path: '" + path + "'");
			this.createZnode(path, data, acl, createMode);
		}
	}

	/**
	 * Return the stat of the node of the given path. Return null if no such a
	 * node exists.
	 */
	public Stat znodeExists(String path)
			throws KeeperException, InterruptedException {
		return zk.exists(path, true);
	}

	/** Delete the znode with the given path. */
	public void deleteZnode(String path)
			throws KeeperException, InterruptedException {
		zk.delete(path, zk.exists(path, true).getVersion());
	}

	/** Get the data from the znode path. */
	public byte[] getZnodeData(String path)
			throws KeeperException, InterruptedException {
		Stat stat = new Stat();
		return zk.getData(path, false, stat);
	}

	/**
	 * Put new data into ZooKeeper in a znode.
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void setZnodeData(String path, byte[] data)
			throws KeeperException, InterruptedException {
		zk.setData(path, data, zk.exists(path, false).getVersion());
	}

	/** Return the list of the children of the node of the given path. */
	public List<String> getChildren(String path)
			throws KeeperException, InterruptedException {
		return zk.getChildren(path, false);
	}

	/**
	 * Show how to use the ZooKeeper interface.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		LoggerSetup.setLogging();
		Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);

		log.info("Test of ZooKeeper");

		/* znode test path */
		String path = "/TestPath";

		/* data in byte array */
		byte[] data = "zookeeper test".getBytes(StandardCharsets.UTF_8);

		try {

			/* open the connection to ZooKeeper */
			ZooKeeperConnection conn = new ZooKeeperConnection();
			int sessionTimeout = 5000;
			ZooKeeper zk = conn.connect("localhost", sessionTimeout);
			ZooKeeperHandler zkHandler = new ZooKeeperHandler(zk);

			/* check if the znode exists */
			Stat stat = zkHandler.znodeExists(path);
			if (stat != null) {
				log.info("The znode with path: '" + path + "' already exists.");

				byte[] existingData = zkHandler.getZnodeData(path);
				log.info("The data in the node is: '"
						+ new String(existingData, StandardCharsets.UTF_8)
						+ "'");

				zkHandler.deleteZnode(path);
			} else {
				log.info("There is no znode with the path: '" + path + "'");
			}

			/* create the data in the specified path in ZooKeeper ensemble */

			zkHandler.createZnode(path, data);

			log.info("New path: '" + path
					+ "' was created in ZooKeeper with data: '"
					+ new String(data, StandardCharsets.UTF_8) + "'");

			byte[] returnedDataNewZnode = zkHandler.getZnodeData(path);
			log.info("returned data from the new znode: '"
					+ new String(returnedDataNewZnode, StandardCharsets.UTF_8)
					+ "'");

			String newData = "adam-new-data";
			log.info("Set new data: '" + newData + "'");
			zkHandler.setZnodeData(path,
					newData.getBytes(StandardCharsets.UTF_8));
			log.info("get newly set data");
			byte[] newlySetData = zkHandler.getZnodeData(path);
			log.info("returned newly set data: '"
					+ new String(newlySetData, StandardCharsets.UTF_8) + "'");

			log.info("Create children");
			zkHandler.createZnode(path + "/Child1",
					"child1".getBytes(StandardCharsets.UTF_8));
			zkHandler.createZnode(path + "/Child2",
					"child2".getBytes(StandardCharsets.UTF_8));

			List<String> children = zkHandler.getChildren(path);
			log.info("Returned children: ");
			for (String child : children) {
				log.info(child);
			}

			/* close the connection to ZooKeeper */
			conn.close();
		} catch (Exception ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			log.error(LogUtils.replace(stackTrace));
			System.exit(1);
		}
	}

}
