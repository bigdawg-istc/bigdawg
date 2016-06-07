package istc.bigdawg.parsers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.experimental.dag.DirectedAcyclicGraph.CycleFoundException;
import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.cast.Cast;
import istc.bigdawg.packages.CrossIslandQueryPlan;

public class UserQueryParser {
			
	static final String extractTitle = "BIGDAWGTAG_";
	
	/**
	 * Unrolling island calls and data casts.
	 * This implementation assumes that the user will explicitly make island call and cast calls.
	 * Examples: 
	 * - bdarray(project(array1))
	 * - bdcast(array1, array, table)
	 * 
	 * @param str
	 * @return ArrayList of Strings, each an island call or a cast call.
	 */
	@Deprecated
	public static Map<String, String> getUnwrappedQueriesByIslands(String str) {
		
		// NEW
		DirectedAcyclicGraph<Pair<String, String>, DefaultEdge> dag = new DirectedAcyclicGraph<Pair<String, String>, DefaultEdge>(DefaultEdge.class);
		// NEW END
		
		Pattern mark					= Pattern.compile("(?i)(bdrel\\(|bdarray\\(|bdtext\\(|bdgraph\\(|"
											+ "bdstream\\(|bdcast\\(|"
//											+ "(, *(relational|graph|stream|text|array)\\))|\\(|\\))");
											+ "\\(|\\))");
		Matcher matcher					= mark.matcher(str);
	    
	    Map<String, String> extraction 	= new HashMap<>();
	    Stack<String> stkwrite			= new Stack<String>();
	    Stack<Integer> stkparen			= new Stack<Integer>();
	    
	    // nodes in the last level
	    List<Pair<String, String>> lp 	= new ArrayList<>();
	    int lastLevel = 0; 
	    
	    int extractionCounter 			= 0;
	    int parenLevel					= 0;
	    int lastStop					= 0;						// location of the place where it stopped last iteration
	    
	    stkwrite.push("");
	    stkparen.push(parenLevel);				// records level of parenthesis
	    
	    while (matcher.find()) {
	    	if ((str.charAt(matcher.start()) | 0x20) == 'b') {
	    		// update the prior and add a new one
	    		stkwrite.push(stkwrite.pop() + str.substring(lastStop, matcher.start()));
	    		lastStop = matcher.end(); 
	    		stkwrite.push(str.substring(matcher.start(), lastStop));
	    		
	    		// add parse level
	    		parenLevel += 1;
	    		stkparen.push(parenLevel);
	    	} else if (str.charAt(matcher.start()) == '(') {
	    		parenLevel += 1;
	    	} else {
	    		if (parenLevel != stkparen.peek()) {
	    			parenLevel -= 1;
	    		} else {
		    		// finish and extract this entry, add new variable for the prior
	    			
	    			String name = null;
	    			
		    		if (parenLevel == 1)
		    			name = CrossIslandQueryPlan.getOutputToken();
		    		else 
		    			name = extractTitle + extractionCounter;
		    		
		    		// NEW
		    		Pair<String, String> thisPair = new ImmutablePair<>(name, stkwrite.peek() + str.substring(lastStop, matcher.end()));
		    		
		    		// if lastLevel is this level + 1, then connect everything and clear
		    		// otherwise if lastLevel is the same, then add this one
		    		// otherwise complain
		    		dag.addVertex(thisPair);
		    		if (lastLevel <= parenLevel) {
		    			lp.add(thisPair);
		    		} else if (lastLevel == parenLevel + 1) {
		    			
		    			for (Pair<String, String> p : lp) 
		    				try {
								dag.addDagEdge(p, thisPair);
							} catch (CycleFoundException e) {
								e.printStackTrace();
							}
		    			lp.clear();
		    			lp.add(thisPair);
		    		} else // lastLevel > parenLevel, not supposed to happen 
		    			System.out.printf("Complain: %s, %s, %s;\n", thisPair, parenLevel, lastLevel);
		    		
		    		lastLevel = parenLevel;
		    		// NEW END
		    		
		    		
	    			extraction.put(name, stkwrite.pop() + str.substring(lastStop, matcher.end()));
		    		stkwrite.push(stkwrite.pop() + extractTitle + extractionCounter);
		    		
		    		extractionCounter += 1;
		    		lastStop = matcher.end(); 
		    		
		    		// subtract one par level
		    		parenLevel -= 1;
		    		stkparen.pop();
	    		}
	    	};
	    }
	    
	    for (DefaultEdge e : dag.edgeSet()) {
	    	System.out.printf("%s -> %s\n", dag.getEdgeSource(e), dag.getEdgeTarget(e)); 
	    }
		
		return extraction;
	}
	
	/**
	 * Takes the unwrapped queries and turn them into signatures and casts.
	 * Currently, it's a TSV string of operators, a TSV string of of objects, and a TSV string  
	 * @param qs, or queries
	 * @return ArrayList of Signatures and casts
	 * @throws Exception
	 */
	@Deprecated
	public static Map<String, Object> getSignaturesAndCasts (Map<String, String> qs) throws Exception {
		
		Map<String, Object> output	= new HashMap<String, Object>();
		Pattern islandStart 		= Pattern.compile("^(bdrel\\(|bdarray\\(|bdtext\\(|bdgraph\\(|bdstream\\(|bdcast\\()");
		Pattern dawgtag 			= Pattern.compile("((\\);)|(\\)))$");
		
		Matcher mStart;
		Matcher mEnd;
		
		String query;
		String islandString;
		
		for (String tagString : qs.keySet()) {
			
			String q = qs.get(tagString);
			
			mStart = islandStart.matcher(q);
			mEnd   = dawgtag.matcher(q);
			
			if (mStart.find() && mEnd.find()) {
				islandString = q.substring(mStart.start(), mStart.end());
				query = q.substring(mStart.end(), mEnd.start());
				
				switch (islandString.toLowerCase()) {
					case "bdrel(":
//						output.put(tagString, new Signature(query, Scope.valueOf("RELATIONAL"), tagString));
						System.out.print  ("[Unsupported]\tRELATIONAL\t");
						break;
					case "bdarray(":
//						output.put(tagString, new Signature(query, Scope.valueOf("ARRAY"), tagString));
						System.out.print  ("[Unsupported]\tARRAY\t");
						break;
					case "bdtext(":
						System.out.print  ("[Unsupported]\tTEXT\t");
						System.out.print  ("\"" + query + "\"\t");
						System.out.println(tagString);
						break;
					case "bdgraph(":
						System.out.print  ("[Unsupported]\tGRAPH\t");
						System.out.print  ("\"" + query + "\"\t");
						System.out.println(tagString);
						break;
					case "bdstream(":
						System.out.print  ("[Unsupported]\tSTREAM\t");
						System.out.print  ("\"" + query + "\"\t");
						System.out.println(tagString);
						break;
					case "bdcast(":
						String[] temp = query.replace(" ", "").split(",");
						output.put(tagString, new Cast(temp[0], temp[1], temp[2], tagString));
						break;
					default:
						throw new Exception("Invalid Signature island input: "+islandString);
				}
			}
		}	
		return output; 
	}
}