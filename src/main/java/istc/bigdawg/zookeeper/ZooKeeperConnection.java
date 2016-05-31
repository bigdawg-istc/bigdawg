/**
 * 
 */
package istc.bigdawg.zookeeper;

/* import java classes */
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author Adam Dziedzic
 * 
 *         For connection to ZooKeeper ensemble.
 */
public class ZooKeeperConnection {

	/** Declare ZooKeeper instance to access ZooKeeper ensemble */
	private ZooKeeper zoo;

	/**
	 * count down latch is a synchronization mechanism and process that calls
	 * await on the latch wait until the latch is decremented to zero
	 * 
	 * It waits until the connection with a ZooKeeper physical node was established.
	 */
	private final CountDownLatch connectedSignal = new CountDownLatch(1);

	/**
	 * connection to zookeeper ensemble
	 * 
	 * @param hostPort
	 *            connection string containing a comma separated list of
	 *            host:port pairs
	 */
	public ZooKeeper connect(String hostPort, int timeout)
			throws IOException, InterruptedException {
		zoo = new ZooKeeper(hostPort, timeout, new Watcher() {
			public void process(WatchedEvent we) {
				/**
				 * KeeperState.SyncConnected - the client is in the connected
				 * state - it is connected to a server in the ensemble
				 */
				if (we.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});
		connectedSignal.await();
		return zoo;
	}

	/** disconnect from the ZooKeeper server */
	public void close() throws InterruptedException {
		zoo.close();
	}

}
