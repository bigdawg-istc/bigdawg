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
//	private static PostgreSQLHandler psqlh;
	
	protected void setUp() throws Exception {
		expectedOutputs = new HashMap<>();
		inputs			= new HashMap<>();
		
		setupNoJoin();
		setupOneJoin();
		setupTwoJoins();
		setupThreeJoins();
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
	
	
	private void testCaseForPermutation(String testName) throws Exception {
		
		Set<String> expectedOutput = expectedOutputs.get(testName);
		Set<String> generatedOutput = getPermutationString(inputs.get(testName));
		
		
		assertEquals(expectedOutput, generatedOutput);
	}
	
	
	private Set<String> getPermutationString(String input) {
		
		Set<String> extraction = new HashSet<>();
		
		int len = input.split(" ").length;
		
		if (len <= 2) {
			extraction.add(pize(input));
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
				j1 = i - j0 - 1; ///// TODO if i + 1 = 3, j0 + 1 starts at 2, j1 + 1 = 1
				
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
								newEntry.add(pize(k0str+" "+k1str));
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
								newEntry.add(pize(k0str+" "+k1str));
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

	private String pize(String input) {
		return "("+input+")";
	}
	
	private boolean isDisj(String s1, String s2) {
		Set<String> set1 = new HashSet<String>(Arrays.asList(s1.replaceAll("[\\(\\)]", "").split(" ")));
		Set<String> set2 = new HashSet<String>(Arrays.asList(s2.replaceAll("[\\(\\)]", "").split(" ")));
		return (!(set1.removeAll(set2)));
	}
}
