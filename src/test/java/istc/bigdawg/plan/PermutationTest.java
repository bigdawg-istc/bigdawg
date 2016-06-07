package istc.bigdawg.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import junit.framework.TestCase;

public class PermutationTest extends TestCase {

	
	private static Map<String, Set<String>> expectedOutputs;
	private static Map<String, String> inputs;
	private static Map<String, Set<String>> constraints;
//	private static PostgreSQLHandler psqlh;
	
	protected void setUp() throws Exception {
		expectedOutputs = new HashMap<>();
		inputs			= new HashMap<>();
		constraints		= new HashMap<>();
		
		setupNoJoin();
		setupOneJoin();
		setupTwoJoins();
		setupThreeJoins();
		setupThreeJoinsWithConstraint();
	}
	
	
	private void setupNoJoin() {
		inputs.put("no-join", "1");
		Set<String> out = new HashSet<>();
		out.add("(1)");
		expectedOutputs.put("no-join", out);
		
		// should have no effect
	}
	
	private void setupOneJoin() {
		inputs.put("one-join", "1 2");
		Set<String> out = new HashSet<>();
		out.add("(1 2)");
		expectedOutputs.put("one-join", out);
		// should have no effect
	}
	
	private void setupTwoJoins() {
		inputs.put("two-join", "1 2 3");
		Set<String> out = new HashSet<>();
		out.add("((1 2) 3)");
		out.add("((1 3) 2)");
		out.add("((2 3) 1)");
		expectedOutputs.put("two-join", out);
		// should give 3 permutations
	}
	
	private void setupThreeJoins() {
		inputs.put("three-join", "1 2 3 4");
		Set<String> out = new HashSet<>();
		out.add("(((1 2) 3) 4)");
		out.add("(((1 2) 4) 3)");
		out.add("(((1 3) 2) 4)");
		out.add("(((1 3) 4) 2)");
		out.add("(((1 4) 2) 3)");
		out.add("(((1 4) 3) 2)");
		
		out.add("(((2 3) 1) 4)");
		out.add("(((2 3) 4) 1)");
		out.add("(((2 4) 1) 3)");
		out.add("(((2 4) 3) 1)");
		
		out.add("(((3 4) 1) 2)");
		out.add("(((3 4) 2) 1)");
		
		out.add("((1 2) (3 4))");
		out.add("((1 3) (2 4))");
		out.add("((1 4) (2 3))");
		expectedOutputs.put("three-join", out);
		// should give 15 permutations
	}
	
	private void setupThreeJoinsWithConstraint() {
		inputs.put("three-join-with-con", "1 2 3 4");
		Set<String> out = new HashSet<>();
		out.add("(((1 2) 3) 4)");
		out.add("(((1 2) 4) 3)");
		out.add("(((1 3) 2) 4)");
		out.add("(((1 3) 4) 2)");
		out.add("(((1 4) 2) 3)");
		out.add("(((1 4) 3) 2)");
		
		out.add("(((2 3) 1) 4)");
		out.add("(((2 3) 4) 1)");
		out.add("(((2 4) 1) 3)");
		out.add("(((2 4) 3) 1)");
		
		out.add("(((3 4) 1) 2)");
		out.add("(((3 4) 2) 1)");
		
		out.add("((1 2) (3 4))");
		out.add("((1 3) (2 4))");
		out.add("((1 4) (2 3))");
		expectedOutputs.put("three-join-with-con", out);
		
		
		Set<String> cons = new HashSet<>();
		cons.add("2");
		constraints.put("3", cons);
		
		cons = new HashSet<>();
		cons.add("3");
		constraints.put("2", cons);
		// should give 15 permutations
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
	
	@Test
	public void testThreeJoins() throws Exception {
		testCaseForPermutation("three-join");
	}
	
	@Test
	public void testThreeJoinsWithConstraint() throws Exception {
		testCaseForPermutation("three-join-with-con");
	}
	
	
	private void testCaseForPermutation(String testName) throws Exception {
		
		Set<String> expectedOutput = expectedOutputs.get(testName);
		Set<String> generatedOutput = getPermutationString(inputs.get(testName));
		
		assertEquals(expectedOutput, generatedOutput);
	}
	
	
	private Set<String> getPermutationString(String input) {
		
		Set<String> extraction = new HashSet<>();
		
		int len = input.split(" ").length;
		

		if (len == 1) {
			// the case of one
			// this case must NOT appear; throw an exception
			
			extraction.add(makeJoin((input.split(" "))[0], null));
			return extraction;
			
			
		} else if (len == 2) {
			// the case of two
			extraction.add(makeJoin((input.split(" "))[0], (input.split(" "))[1]));
			return extraction;
		} 
		
		ArrayList<String> singles = new ArrayList<String>(Arrays.asList(input.split(" ")));
		ArrayList<ArrayList<String>> permutations = new ArrayList<>();
		permutations.add(singles);
		
		ArrayList<String> newEntry;
		
		int j0;
		int j1;
		int k0;
		int k1;
		for (int i = 1; i < len ; i ++ ) {
			// i+1, start at 2, is the permutation tuples we're working on
			newEntry = new ArrayList<String>();
			
			for (j0 = i - 1; j0 >= 0 ; j0 --) {
				// j0 and j1 are the two to be joined
				j1 = i - j0 - 1; 
				
				if (j0 < j1) break; // we always want the larger, j0, in the front
				if (j0 == j1) {
					// iterate only when j0's sub position is smaller, and they contain distinct members
					
					ArrayList<String> j0list = permutations.get(j0);
					int j0listSize = j0list.size();
					
					for (k0 = 0; k0 < j0listSize; k0 ++ ) {
						String k0str = permutations.get(j0).get(k0);
						String k1str;
						for (k1 = k0 + 1; k1 < j0listSize; k1 ++) {
							k1str = permutations.get(j0).get(k1);
							
							if (isDisj(k0str, k1str)) {
								
								// TODO this is where you check conditions
								// well, currently we do not prune search space
								
								newEntry.add(makeJoin(k0str, k1str));
							}
							// else do nothing
						}
					}
				} else {
					// iterate through all
					ArrayList<String> j0list = permutations.get(j0);
					ArrayList<String> j1list = permutations.get(j1);
					int j0listSize = j0list.size();
					int j1listSize = j1list.size();
					
					for (k0 = 0; k0 < j0listSize; k0 ++ ) {
						String k0str = permutations.get(j0).get(k0);
						String k1str;
						for (k1 = 0; k1 < j1listSize; k1 ++) {

							k1str = permutations.get(j1).get(k1);
							
							if (isDisj(k0str, k1str)) {
								
								// TODO this is where you check conditions
								// well, currently we do not prune search space
								
								newEntry.add(makeJoin(k0str, k1str));
							}
						}
					}
				}
			}
			
			permutations.add(newEntry);
			
		}
		
		extraction.addAll(permutations.get(permutations.size()-1));
		
		
		return extraction;
	}

	private String makeJoin(String input1, String input2) {
		if (input2 == null) return "("+input1+")"; 
		return "("+input1+" "+input2+")";
	}
	
	private boolean isDisj(String s1, String s2) {
		Set<String> set1 = new HashSet<String>(Arrays.asList(s1.replaceAll("[\\(\\)]", "").split(" ")));
		Set<String> set2 = new HashSet<String>(Arrays.asList(s2.replaceAll("[\\(\\)]", "").split(" ")));
		return (!(set1.removeAll(set2)));
	}
	
	private Set<String> getObjects(String str) {
		
		return new HashSet<String>(Arrays.asList(str.replaceAll("[\\(\\)]", "").split(" ")));
	}
}
