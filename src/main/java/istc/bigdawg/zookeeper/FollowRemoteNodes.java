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

import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * Execution is carried out on more than one node. If one of the nodes in the
 * execution fails, then the execution is stopped and an exception is returned.
 * 
 * @author Adam Dziedzic
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
	public static Object execute(List<String> ipAddresses, Callable<Object> callable) {
		logger.debug("Execution of data migration and following the remote nodes: "
				+ ipAddresses.toString());
		List<Callable<Object>> allCallables = new ArrayList<>();
		allCallables.add(callable);
		// ZooKeeperHandler zooHandler = new ZooKeeperHandler(
		// ZooKeeperInstance.INSTANCE.getZooKeeper());
		// for (String ipAddress : ipAddresses) {
		// allCallables.add(zooHandler.callableWatch(
		// ZooKeeperUtils.getZnodePathNodes(ipAddress),
		// EventType.NodeDeleted));
		// }
		logger.debug("The number of threads in the executor: "
				+ "number of nodes to follow + the migration execution.");
		ExecutorService executor = Executors
				.newFixedThreadPool(allCallables.size());
		try {
			Object result = executor.invokeAny(allCallables);
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
