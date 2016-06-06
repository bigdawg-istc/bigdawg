/**
 * 
 */
package istc.bigdawg.zookeeper;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import istc.bigdawg.exceptions.NetworkException;
import istc.bigdawg.network.NetworkUtils;
import istc.bigdawg.zookeeper.znode.NodeInfo;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class ZooKeeperHandlerTest {

	/**
	 * Path to the test znode in ZooKeeper.
	 */
	private final static String znodePath = "/_test_zookeeper_watch_event_";

	@Test
	public void testWatchEvent()
			throws NetworkException, KeeperException, InterruptedException,
			IOException, ExecutionException, TimeoutException {
		/* open the connection to ZooKeeper */
		int sessionTimeout = 5000;
		ZooKeeper zk = ZooKeeperHandler.connect("localhost", sessionTimeout);
		ZooKeeperHandler zooHandler = new ZooKeeperHandler(zk);
		zooHandler.createZnodeIfNotExists(znodePath,
				NetworkUtils.serialize(new NodeInfo("test")),
				ZooDefs.Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT);
		EventType eventType = EventType.NodeDeleted;

		ExecutorService executor = Executors.newFixedThreadPool(1);
		Future<Object> futureResultFromCallable = executor
				.submit(zooHandler.callableWatch(znodePath, eventType));
		zooHandler.deleteZnode(znodePath);
		String result = (String) futureResultFromCallable.get(10,
				TimeUnit.SECONDS);
		System.out.println("Result from the watcher: " + result);
		assertEquals(ZooKeeperHandler.watchMessagePrefix + eventType + " path: "
				+ znodePath, result);
		zk.close();
	}

}
