/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
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
 *         Handle interactions with ZooKeeper.
 */
public class ZooKeeperHandler {
	/* log */
	private static Logger logger = Logger.getLogger(ZooKeeperHandler.class);

	/** The prefix of the message returned from the ZooKeeper watcher */
	public static final String watchMessagePrefix = "The following event was triggered in ZooKeeper: ";

	/** Information that the lock was acquired but after some waiting. */
	public static final String lockAcquiredAfterWaiting = "The lock was acquired after wait. ";

	/** Information that the lock was acquired immediately. */
	public static final String lockAcquiredImmediately = "The lock was acquired immediately. ";

	/** Instance for ZooKeeper class. */
	private ZooKeeper zk;

	/**
	 * Initialize the ZooKeeper handler with a ZooKeeper instance.
	 */
	public ZooKeeperHandler(ZooKeeper zk) {
		this.zk = zk;
	}

	/**
	 * connection to zookeeper ensemble
	 * 
	 * @param hostPort
	 *            connection string containing a comma separated list of
	 *            host:port pairs
	 * 
	 */
	public static ZooKeeper connect(String hostPort, int sessionTimeout)
			throws IOException, InterruptedException {
		/**
		 * Count down latch is a synchronization mechanism; the process that
		 * calls await on the latch wait until the latch is decremented to zero.
		 * 
		 * It waits until the connection with a ZooKeeper physical node was
		 * established.
		 */
		CountDownLatch connectedSignal = new CountDownLatch(1);
		ZooKeeper zoo = new ZooKeeper(hostPort, sessionTimeout, new Watcher() {
			public void process(WatchedEvent watchedEvent) {
				/**
				 * KeeperState.SyncConnected - the client is in the connected
				 * state - it is connected to a server in the ensemble
				 */
				if (watchedEvent.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});
		connectedSignal.await();
		return zoo;
	}

	/**
	 * Create a znode in ZooKeeper ensemble, with default ACL (Access Control
	 * List) - completely open and in persistent mode.
	 */
	public String createZnode(String path, byte[] data)
			throws KeeperException, InterruptedException {
		return this.createZnode(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
	}

	/** Create znode with all parameters provided. */
	public String createZnode(String path, byte[] data, List<ACL> acl,
			CreateMode createMode)
					throws KeeperException, InterruptedException {
		return zk.create(path, data, acl, createMode);
	}

	/**
	 * Wait/watch for an event from ZooKeeper for a given znode.
	 * 
	 * @param znodePath
	 *            Path to a znode in ZooKeeper ensemble.
	 * @param eventType
	 *            The type of event on which we await.
	 * @return signal (string) that the event was triggered
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String watchEvent(String znodePath,
			Watcher.Event.EventType eventType) {
		logger.debug("Watch event of type: " + eventType + " for znode path: "
				+ znodePath);
		/**
		 * Count down latch is a synchronization mechanism.
		 * 
		 * Calling await on the latch wait until the latch is decremented to
		 * zero.
		 * 
		 * It waits until the event was reported by ZooKeeper.
		 */
		CountDownLatch signal = new CountDownLatch(1);
		try {
			if (zk.exists(znodePath, true) == null) {
				return "The znode: " + znodePath + " does not exist.";
			}
			zk.exists(znodePath, new Watcher() {
				public void process(WatchedEvent watchedEvent) {
					if (watchedEvent.getType() == eventType) {
						logger.debug("The event of type: " + eventType
								+ " was triggered for znode path: "
								+ znodePath);
						signal.countDown();
					}
				}
			});
		} catch (KeeperException | InterruptedException e) {
			String message = e.getMessage() + " Problem with ZooKeeper, path: "
					+ znodePath;
			logger.error(message + " Stack trace: "
					+ StackTrace.getFullStackTrace(e));
			return message;
		} catch (IllegalArgumentException e) {
			String message = e.getMessage() + "Invalid path was specified: "
					+ znodePath;
			logger.error(message + " Stack trace: "
					+ StackTrace.getFullStackTrace(e));
			return message;
		}
		try {
			signal.await();
		} catch (InterruptedException e) {
			String message = "The watchEvent from ZooKeeper thread was interrupted while waiting. ";
			logger.warn(message + "Stack trace: "
					+ StackTrace.getFullStackTrace(e));
			return message;
		}
		return watchMessagePrefix + eventType + " path: " + znodePath;
	}

	/**
	 * Callable for a ZooKeeper watch. This is callable for an object because it
	 * will be part of collections of objects which can return different types
	 * of objects.
	 * 
	 * @param zooHandler
	 *            handler to ZooKeeper
	 * @param znodePath
	 *            path to a znode
	 * @param eventType
	 *            the type of event to watch on a znode
	 * @return if event happens that it returns an object (with information
	 *         about the event)
	 */
	public Callable<Object> callableWatch(String znodePath,
			EventType eventType) {
		logger.debug("Set watch for znode: " + znodePath + " watch for event: "
				+ eventType);
		return () -> {
			return watchEvent(znodePath, eventType);
		};
	}

	/**
	 * Create a znode in ZooKeeper ensemble if it does not already exists.
	 * 
	 * @return
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public String createZnodeIfNotExists(String path, byte[] data,
			List<ACL> acl, CreateMode createMode)
					throws KeeperException, InterruptedException {
		/* check if the znode exists */
		Stat stat = this.znodeExists(path);
		if (stat != null) {
			logger.info("The znode with path: '" + path + "' already exists.");
			return path;
		} else {
			logger.info("There is no znode with the path: '" + path + "'");
			return this.createZnode(path, data, acl, createMode);
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
		Stat stat = zk.exists(path, true);
		if (stat != null) {
			zk.delete(path, stat.getVersion());
		}
	}

	/**
	 * Delete the znode specified by the path with all of its children.
	 * 
	 * @param path
	 *            the path to a znode in ZooKeeper
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void deleteZnodeWithChildren(String path)
			throws KeeperException, InterruptedException {
		Stat stat = zk.exists(path, true);
		if (stat != null) {
			List<String> children = getChildren(path);
			for (String child : children) {
				String fullPath = path + "/" + child;
				deleteZnodeWithChildren(fullPath);
			}
			zk.delete(path, stat.getVersion());
		}
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

	/** Return the sorted list of the children of the node of the given path. */
	public List<String> getSortedChildren(String path)
			throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(path, false);
		children.sort(String::compareTo);
		return children;
	}

	/**
	 * Acquire lock in ZooKeeper. check paper on ZooKeeper - ZooKeeper: Wait-free
	 * coordination for Internet-scale systems - page 6 - simple locks without
	 * Herd Effects. It blocks until the lock can be acquired.
	 * 
	 * @param znodePath
	 *            path to be locked
	 * @param data
	 *            data to be put in the lock
	 * @return the full path to the znode which represents the lock
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public String acquireLock(String znodePath, byte[] data,
			ZnodePathMessage znodePathMessage)
					throws KeeperException, InterruptedException {
		logger.debug("get lock for: " + znodePath + ", data: "
				+ new String(data, StandardCharsets.UTF_8));
		String createdZnode = createZnode(znodePath + "/lock-", data,
				ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		znodePathMessage.setZnodePath(createdZnode);
		/*
		 * When the process was waiting for the lock and another processes
		 * released the lock (the znode being watched by the client goes away,
		 * the client must check if it now holds the lock. The previous lock
		 * request may have been abandoned and there is a znode with a lower
		 * sequence number still waiting for or holding the lock.
		 */
		boolean lockAcquired = false;
		/* indicates if the process was waiting for the lock */
		boolean wasWaiting = false;
		while (!lockAcquired) {
			List<String> children = getChildren(znodePath);
			children.sort(String::compareTo);
			/* If this machine is not the first one to acquire the lock. */
			/*
			 * The collection of children contains only the final name of znode
			 * without a full path!
			 */
			if (!createdZnode.equals(znodePath + "/" + children.get(0))) {
				wasWaiting = true;
				for (int i = 1; i < children.size(); ++i) {
					if (createdZnode
							.equals(znodePath + "/" + children.get(i))) {
						String previousZnodeLock = children.get(i - 1);
						logger.debug("Waiting to acquire the following lock: "
								+ createdZnode);
						String watchEventMessage = watchEvent(
								znodePath + "/" + previousZnodeLock,
								EventType.NodeDeleted);
						znodePathMessage.setMessage(
								lockAcquiredAfterWaiting + watchEventMessage);
						break;
					}
				}
			} else {
				if (!wasWaiting) {
					znodePathMessage.setMessage(lockAcquiredImmediately);
				}
				lockAcquired = true;
			}
		}
		return createdZnode;
	}

	/**
	 * {@link #acquireLock(String, byte[], ZnodePathMessage)}
	 */
	public String getLock(String znodePath, byte[] data)
			throws KeeperException, InterruptedException {
		return this.acquireLock(znodePath, data, new ZnodePathMessage());
	}

	/**
	 * Release the lock in ZooKeeper.
	 * 
	 * @param lock
	 *            the full path to the znode in ZooKeeper that represents the
	 *            lock
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void releaseLock(String lock)
			throws KeeperException, InterruptedException {
		deleteZnode(lock);
	}

	/**
	 * Show how to use the ZooKeeper interface.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		LoggerSetup.setLogging();
		Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);

		logger.info("Test of ZooKeeper");

		/* znode test path */
		String path = "/TestPath";

		/* data in byte array */
		byte[] data = "zookeeper test".getBytes(StandardCharsets.UTF_8);

		try {
			/* open the connection to ZooKeeper */
			int sessionTimeout = 5000;
			ZooKeeper zk = ZooKeeperHandler.connect("localhost",
					sessionTimeout);
			ZooKeeperHandler zkHandler = new ZooKeeperHandler(zk);

			/* check if the znode exists */
			Stat stat = zkHandler.znodeExists(path);
			if (stat != null) {
				logger.info(
						"The znode with path: '" + path + "' already exists.");

				byte[] existingData = zkHandler.getZnodeData(path);
				logger.info("The data in the node is: '"
						+ new String(existingData, StandardCharsets.UTF_8)
						+ "'");
				List<String> children = zk.getChildren(path, false);
				for (String child : children) {
					zkHandler.deleteZnode(path + "/" + child);
				}
				zkHandler.deleteZnode(path);
			} else {
				logger.info("There is no znode with the path: '" + path + "'");
			}

			/* create the data in the specified path in ZooKeeper ensemble */

			zkHandler.createZnode(path, data);

			logger.info("New path: '" + path
					+ "' was created in ZooKeeper with data: '"
					+ new String(data, StandardCharsets.UTF_8) + "'");

			byte[] returnedDataNewZnode = zkHandler.getZnodeData(path);
			logger.info("returned data from the new znode: '"
					+ new String(returnedDataNewZnode, StandardCharsets.UTF_8)
					+ "'");

			String newData = "adam-new-data";
			logger.info("Set new data: '" + newData + "'");
			zkHandler.setZnodeData(path,
					newData.getBytes(StandardCharsets.UTF_8));
			logger.info("get newly set data");
			byte[] newlySetData = zkHandler.getZnodeData(path);
			logger.info("returned newly set data: '"
					+ new String(newlySetData, StandardCharsets.UTF_8) + "'");

			logger.info("Create children");
			zkHandler.createZnode(path + "/Child1",
					"child1".getBytes(StandardCharsets.UTF_8));
			zkHandler.createZnode(path + "/Child2",
					"child2".getBytes(StandardCharsets.UTF_8));

			List<String> children = zkHandler.getChildren(path);
			logger.info("Returned children: ");
			for (String child : children) {
				logger.info(child);
			}
			/* close the connection/session to ZooKeeper */
			zk.close();
		} catch (Exception ex) {
			String stackTrace = StackTrace.getFullStackTrace(ex);
			logger.error(LogUtils.replace(stackTrace));
			System.exit(1);
		}
	}

}
