/**
 * 
 */
package istc.bigdawg.utils;

import java.io.IOException;

import org.junit.Test;

import istc.bigdawg.exceptions.RunShellException;

/**
 * Simple test to check if a pipe can be created and destroyed.
 * 
 * @author Adam Dziedzic
 * 
 *
 */
public class PipeTest {

	@Test
	public void testPipes()
			throws IOException, InterruptedException, RunShellException {
		String pipeFullName = Pipe.INSTANCE.createAndGetFullName("test");
		System.out.println("created pipe, the full path is: " + pipeFullName);
		Pipe.INSTANCE.deletePipeIfExists(pipeFullName);
	}
}
