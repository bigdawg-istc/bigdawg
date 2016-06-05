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
	private static Logger logger = Logger
			.getLogger(FollowRemoteNodes.class);

	/**
	 * Carry out the execution on this (local) node and remote nodes. If one of
	 * the remote nodes fails, then the execution should not proceed and an
	 * exception is returned.
	 * 
	 * @param nodePortPairs
	 *            which remote nodes should be followed
	 * @param callable
	 *            the task to be executed on this and other nodes
	 */
	public static Object execute(List<String> nodePortPairs,
			Callable<Object> callable) {
		ZooKeeperHandler zooHandler = new ZooKeeperHandler(
				ZooKeeperInstance.INSTANCE.getZooKeeper());
		List<Callable<Object>> callables = new ArrayList<>();
		callables.add(callable);
		for (String nodePort : nodePortPairs) {
			callables.add(zooHandler.callableWatch(ZooKeeperUtils.BigDAWGPath
					+ ZooKeeperUtils.nodes + "/" + nodePort,
					EventType.NodeDeleted));
		}
		ExecutorService executor = Executors.newWorkStealingPool();
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
