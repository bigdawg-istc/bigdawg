/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.RunShellException;

/**
 * This it the pipe for the migrations. It creates a mkfifo in the local file
 * system.
 * 
 * The pipe can be used only by one thread.
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 2, 2016 1:54:17 PM
 */
public enum Pipe {
	INSTANCE;

	/* log */
	private static Logger log = Logger.getLogger(Pipe.class);

	/**
	 * global counter for the pipes - each new pipe has to have a different
	 * name, no two threads can use the same pipe at the same time
	 */
	private AtomicLong globalCounter;

	private Pipe() {
		globalCounter = new AtomicLong(0);
	}

	/**
	 * Create a pipe and get its full path.
	 * 
	 * @param pipeName
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws RunShellException
	 */
	synchronized public String createAndGetFullName(String pipeName)
			throws IOException, InterruptedException, RunShellException {
		String fullName = System.getProperty("user.dir") + "/src/main/resources/tmp/bigdawg_" + pipeName + "_"
				+ globalCounter.incrementAndGet();
		log.debug("full path name for the pipe: " + fullName);
		/* if a test/execution fails then a pipe can still exists */
		this.deletePipeIfExists(fullName);
		if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC) {
			RunShell.mkfifo(fullName);
			// RunShell.runShell(new ProcessBuilder("touch", fullName)); /* for
			// debug */
			RunShell.runShell(new ProcessBuilder("chmod", "a+rw", fullName));
		} else {
			throw new RuntimeException("The platforms (such as Windows, Solaris) are not supported.");
		}
		/*
		 * For windows: SystemUtils.IS_OS_WINDOWS_7 or
		 * SystemUtils.IS_OS_WINDOWS_8 the required files should be created by
		 * the load/export tools in the databases
		 */
		/*
		 * SciDB (or database instances installed in other users) has to have
		 * rights to read from/write to the pipe
		 */
		return fullName;
	}

	/**
	 * 
	 * @param pipeNameFull
	 * @return true if pipe was deleted, false if the pipe was not deleted
	 *         because it did not exist
	 * @throws IOException
	 */
	synchronized public boolean deletePipeIfExists(String pipeNameFull) throws IOException {
		return SystemUtilities.deleteFileIfExists(pipeNameFull);
	}

}
