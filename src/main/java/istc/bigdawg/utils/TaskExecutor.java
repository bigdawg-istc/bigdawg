/**
 * 
 */
package istc.bigdawg.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

/**
 * @author Adam Dziedzic
 * 
 *         Executes a collection of tasks and its main purpose is to detect if
 *         there is any exception thrown in one of the tasks, if so then the
 *         whole execution is stopped and the exception caught is thrown further
 *         on.
 */
public class TaskExecutor {

	/* log */
	private static Logger log = Logger.getLogger(TaskExecutor.class);

	/**
	 * Execute the list of tasks using the given executor and when one of the
	 * tasks returns an exception then throw it.
	 * 
	 * @param executorExternal
	 *            Executor to be used for execution of the tasks.
	 * @param tasks
	 *            The collection of Callables to be executed.
	 * @throws Exception
	 * @returns Collection of futures that are done, the future objects contain
	 *          the results and their order corresponds to the order of the
	 *          given tasks (Callables).
	 */
	public static List<Future<Object>> execute(ExecutorService executorExternal,
			List<Callable<Object>> tasks) throws Exception {
		List<Future<Object>> results = new ArrayList<>();
		CompletionService<Object> service = new ExecutorCompletionService<Object>(
				executorExternal);
		for (Callable<Object> task : tasks) {
			results.add(service.submit(task));
		}
		/*
		 * call the service as many times as there are objects in the tasks
		 * collection
		 */
		for (@SuppressWarnings("unused")
		Callable<Object> task : tasks) {
			try {
				/* log.debug(task.toString()); */
				/*
				 * The take method - takes the first executed task and returns
				 * it or waits if there is no finished tasks (it blocks).
				 */
				Future<Object> resultCompletedTask = service.take();
				if (resultCompletedTask != null
						&& resultCompletedTask.isDone()) {
					Object result = resultCompletedTask.get();
					if (result instanceof Exception) {
						Exception ex = (Exception) result;
						throw ex;
					}
					log.debug("result of execution ("
							+ resultCompletedTask.toString() + "): " + result);
				}
			} catch (ExecutionException e) {
				log.error(e.getMessage() + " StackTrace: "
						+ StackTrace.getFullStackTrace(e));
				throw e;
			} catch (Exception e) {
				log.error("Exception in execution of one of the tasks. "
						+ e.getMessage() + " StackTrace: "
						+ StackTrace.getFullStackTrace(e));
				throw e;
			}
		}
		return results;
	}
}
