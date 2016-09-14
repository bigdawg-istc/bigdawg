/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.network.DataIn;
import istc.bigdawg.network.NetworkObject;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.TaskExecutor;

/**
 * Load data on a remote node.
 * 
 * The instance of the class can be sent via network to another node where it
 * will launch data loading process.
 * 
 * @author Adam Dziedzic
 */
public class LoadRemote implements NetworkObject {

	/**
	 * Determines if a de-serialized file is compatible with this class.
	 */
	private static final long serialVersionUID = 2582635762481125148L;

	/* log */
	private static Logger log = Logger.getLogger(LoadRemote.class);

	/**
	 * @see: {@link #LoadRemote(int, Export, Load)} @param port
	 */
	private int port;

	/**
	 * @see: {@link #LoadRemote(int, Export, Load)} @param loader
	 */
	private Load loader;

	/**
	 * @see: {@link #LoadRemote(int, Export, Load)} @param export
	 */
	private Export export;

	/** Pipes to be removed after the execution of the migration. */
	private List<String> pipes = new ArrayList<>();

	/** Executor used to run many tasks for the migration process. */
	private ExecutorService executorService;

	/**
	 * Initialize an object which represent a data loading request to be
	 * executed on a remote node. The data will be sent from a local node to a
	 * remote node to the port and loaded to a database via loader.
	 * 
	 * @param port
	 *            The number of port on which we should listen to get the data
	 *            transfer request. @see
	 *            {@link istc.bigdawg.network.DataIn#receive(int, String)}
	 * @param export
	 *            We transfer the export object to use it methods at the other
	 *            node and set some meta information for the loader.
	 * @param loader
	 *            The loader which will be used to load the data on the remote
	 *            node.
	 * 
	 * 
	 */
	public LoadRemote(int port, Export export, Load loader) {
		this.port = port;
		this.export = export;
		this.loader = loader;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.network.NetworkObject#execute()
	 */
	@Override
	public Object execute() throws MigrationException {
		try {
			long startTimeLoading = System.currentTimeMillis();
			MigrationInfo migrationInfo = loader.getMigrationInfo();
			String pipe = Pipe.INSTANCE
					.createAndGetFullName(this.getClass().getName() + "_from_"
							+ migrationInfo.getObjectFrom() + "_to_"
							+ migrationInfo.getObjectTo());
							/* pipe = "/tmp/adam"; */

			/* add the pipe to be removed when cleaning the resources */
			pipes.add(pipe);
			loader.setLoadFrom(pipe);
			loader.setHandlerFrom(export.getHandler());
			/* activate the tasks */
			List<Callable<Object>> tasks = new ArrayList<>();
			tasks.add(new DataIn(port, pipe));
			tasks.add(loader);
			executorService = Executors.newFixedThreadPool(tasks.size());
			long startTimeMigration = System.currentTimeMillis();
			List<Future<Object>> results = TaskExecutor.execute(executorService,
					tasks);
			Long countBytesNetwork = (Long) results.get(0).get();
			Long countLoadedElements = (Long) results.get(1).get();
			long endTimeLoading = System.currentTimeMillis();
			long durationMsec = endTimeLoading - startTimeLoading;
			String message = "Remote loading was executed correctly "
					+ "(bytes received from network: " + countBytesNetwork
					+ ").";
			log.info(message);
			LoadRemoteResult loadingResult = new LoadRemoteResult(
					countLoadedElements, startTimeMigration, endTimeLoading,
					durationMsec, countBytesNetwork, message);
			return loadingResult;
		} catch (Exception e) {
			String msg = e.getMessage() + " Remote loading failed.";
			log.error(msg + " " + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		} finally {
			cleanResources();
		}

	}

	/**
	 * Clean the resources that were allocated for the migration process.
	 */
	private void cleanResources() {
		for (String pipe : pipes) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(pipe);
			} catch (IOException e) {
				log.error("Problem when removing the pipe/file: " + pipe + " "
						+ e.getMessage() + StackTrace.getFullStackTrace(e), e);
			}
		}
		pipes.clear();
		if (executorService != null) {
			if (!executorService.isShutdown()) {
				executorService.shutdownNow();
			}
			executorService = null;
		}
	}

}
