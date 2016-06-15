/**
 * 
 */
package istc.bigdawg.zookeeper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;

import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 *         Execution is carried out on more than one node. If one of the nodes
 *         in the execution fails, then the execution is stopped and an
 *         exception is returned.
 *
 */
public class FollowRemoteNodes {

	/*
	 * log
	 */
	private static Logger logger = Logger.getLogger(FollowRemoteNodes.class);

	/**
	 * Carry out the execution on this (local) node and remote nodes. If one of
	 * the remote nodes fails, then the execution should not proceed and an
	 * exception is returned.
	 * 
	 * @param ipAddresses
	 *            which remote nodes should be followed
	 * @param callable
	 *            the task to be executed on this and other nodes
	 */
	public static Object execute(List<String> ipAddresses,
			Callable<Object> callable) {
		logger.debug(
				"Execution of data migration and following the remote nodes: "
						+ ipAddresses.toString());
		ZooKeeperHandler zooHandler = new ZooKeeperHandler(
				ZooKeeperInstance.INSTANCE.getZooKeeper());
		List<Callable<Object>> callables = new ArrayList<>();
		for (String ipAddress : ipAddresses) {
			callables.add(zooHandler.callableWatch(
					ZooKeeperUtils.getZnodePathNodes(ipAddress),
					EventType.NodeDeleted));
		}
		callables.add(callable);
		logger.debug("The number of threads in the executor: "
				+ "number of nodes to follow + the migration execution.");
		ExecutorService executor = Executors
				.newFixedThreadPool(ipAddresses.size() + 1);
		try {
			Object result = executor.invokeAny(callables);
			executor.shutdownNow();
			return result;
		} catch (InterruptedException | ExecutionException e) {
			String stackTrace = StackTrace.getFullStackTrace(e);
			logger.error("The executor service for threads failed. "
					+ LogUtils.replace(stackTrace));
			return e;
		}
	}

}
