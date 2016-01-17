package istc.bigdawg.plan;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import istc.bigdawg.postgresql.PostgreSQLHandler;
import junit.framework.TestCase;

public class PermutationTest extends TestCase {

	
	private static Map<String, String> expectedOutputs;
//	private static PostgreSQLHandler psqlh;
	
	protected void setUp() throws Exception {
		expectedOutputs = new HashMap<>();
//		psqlh			= new PostgreSQLHandler();
		
		setupNoJoin();
		setupOneJoin();
		setupTwoJoins();
	}
	
	
	private void setupNoJoin() {
		expectedOutputs.put("no-join", "");
		// should have no effect
	}
	
	private void setupOneJoin() {
		expectedOutputs.put("one-join", "");
		// should have no effect
	}
	
	private void setupTwoJoins() {
		expectedOutputs.put("two-joins", "");
		// should give 3 permutations
	}
	
	@Test
	public void testNoJoin() throws Exception {
		testCaseForPermutation("no-join");
	}
	
	@Test
	public void testOneJoin() throws Exception {
		testCaseForPermutation("one-join");
	}
	
	@Test
	public void testTwoJoins() throws Exception {
		testCaseForPermutation("two-join");
	}
	
	
	private void testCaseForPermutation(String testName) throws Exception {
		
		String expectedOutput = expectedOutputs.get(testName);
		
		String serverOutput = "fail";
		
		assertEquals(expectedOutput, serverOutput);
	}

}
