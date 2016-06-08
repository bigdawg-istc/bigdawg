/**
 * 
 */
package istc.bigdawg.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkUtils;
import istc.bigdawg.zookeeper.znode.NodeInfo;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class ZooKeeperHandlerTest {

	/* log */
	private static Logger logger = Logger.getLogger(ZooKeeperHandler.class);

	/**
	 * Path to the test znode in ZooKeeper.
	 */
	private final static String znodePath = "/_test_zookeeper_watch_event_";

	private ZooKeeperHandler zooHandler;
	private ZooKeeper zk;

	@Before
	public void setUp() throws NetworkException, KeeperException,
			InterruptedException, IOException {
		LoggerSetup.setLogging();
		/* open the connection to ZooKeeper */
		int sessionTimeout = 5000;
		zk = ZooKeeperHandler.connect("localhost", sessionTimeout);
		zooHandler = new ZooKeeperHandler(zk);
		zooHandler.createZnodeIfNotExists(znodePath,
				NetworkUtils.serialize(new NodeInfo("test")),
				ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	@After
	public void close() throws InterruptedException {
		zk.close();
	}

	@Test
	public void testWatchEvent()
			throws NetworkException, KeeperException, InterruptedException,
			IOException, ExecutionException, TimeoutException {
		EventType eventType = EventType.NodeDeleted;
		String znodePathReturned = zooHandler.createZnodeIfNotExists(znodePath,
				"testWatchEvent".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
		assertEquals(znodePath, znodePathReturned);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		Future<Object> futureResultFromCallable = executor
				.submit(zooHandler.callableWatch(znodePath, eventType));
		/* wait until the watch was established */
		TimeUnit.SECONDS.sleep(3);
		assertFalse(futureResultFromCallable.isDone());
		assertFalse(futureResultFromCallable.isCancelled());
		zooHandler.deleteZnode(znodePath);
		String result = (String) futureResultFromCallable.get(10,
				TimeUnit.SECONDS);
		executor.shutdownNow();
		System.out.println("Result from the watcher: " + result);
		assertEquals(ZooKeeperHandler.watchMessagePrefix + eventType + " path: "
				+ znodePath, result);
		zooHandler.deleteZnode(znodePath);
	}

	@Test
	public void testAcquireLock()
 throws KeeperException, InterruptedException,
			ExecutionException, BigDawgException {
		logger.debug("Test get lock");
		String lockPath = "/_test_locks___";
		zooHandler.createZnodeIfNotExists(lockPath, "test locks".getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		/* clean the znode */
		for (String lock : zooHandler.getChildren(lockPath)) {
			logger.debug("There were some locks left from the previous test.");
			zooHandler.deleteZnode(lock);
		}
		ZnodePathMessage znodePathMessage = new ZnodePathMessage();
		String firstLock = zooHandler.acquireLock(lockPath, "test".getBytes(),
				znodePathMessage);
		logger.debug("frist lock:" + firstLock);
		assertEquals(firstLock,
				lockPath + "/" + zooHandler.getChildren(lockPath).get(0));
		assertEquals(firstLock, znodePathMessage.getZnodePath());
		assertEquals(ZooKeeperHandler.lockAcquiredImmediately,
				znodePathMessage.getMessage());
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Callable<String> task = () -> {
			String secondLock = null;
			try {
				ZnodePathMessage znodePathMessageSecondLock = new ZnodePathMessage();
				secondLock = zooHandler.acquireLock(lockPath, "test".getBytes(),
						znodePathMessageSecondLock);
				logger.debug("second lock: " + secondLock);
				assertEquals(secondLock,
						znodePathMessageSecondLock.getZnodePath());
				logger.debug("expected: "
						+ ZooKeeperHandler.lockAcquiredAfterWaiting
						+ ZooKeeperHandler.watchMessagePrefix
						+ EventType.NodeDeleted + " path: " + firstLock);
				logger.debug(
						"obtained: " + znodePathMessageSecondLock.getMessage());
				assertEquals(
						ZooKeeperHandler.lockAcquiredAfterWaiting
								+ ZooKeeperHandler.watchMessagePrefix
								+ EventType.NodeDeleted + " path: " + firstLock,
						znodePathMessageSecondLock.getMessage());
				return secondLock;
			} catch (Exception e) {
				e.printStackTrace();
			}
			return secondLock;
		};
		Future<String> futureLock = executor.submit(task);
		assertFalse(futureLock.isDone());
		/* wait until the second node (lock) is created */
		while (zooHandler.getChildren(lockPath).size() < 2) {
			TimeUnit.SECONDS.sleep(2);
		}
		List<String> children = zooHandler.getChildren(lockPath);
		children.sort(String::compareTo);
		assertEquals(firstLock, lockPath + "/" + children.get(0));
		zooHandler.releaseLock(firstLock);
		String secondLock = futureLock.get();
		assertEquals(secondLock,
				lockPath + "/" + zooHandler.getChildren(lockPath).get(0));
		zooHandler.releaseLock(secondLock);
		assertEquals(0, zooHandler.getChildren(lockPath).size());
		zooHandler.deleteZnode(lockPath);
	}

}
