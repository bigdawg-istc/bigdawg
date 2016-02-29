package istc.bigdawg.parsers;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.cast.Cast;

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
	public static LinkedHashMap<String, String> getUnwrappedQueriesByIslands(String str) {
		
		
		
		Pattern mark					= Pattern.compile("(?i)(bdrel\\(|bdarray\\(|bdtext\\(|bdgraph\\(|"
											+ "bdstream\\(|bdcast\\(|"
											+ "(, ?(relational|graph|stream|text|array)\\))|\\(|\\))");
		Matcher matcher					= mark.matcher(str);
	    
	    LinkedHashMap<String, String> extraction 	= new LinkedHashMap<>();
	    Stack<String> stkwrite			= new Stack<String>();
	    Stack<Integer> stkparen			= new Stack<Integer>();
	    
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
		    		if (parenLevel == 1)
		    			extraction.put("A_OUTPUT", stkwrite.pop() + str.substring(lastStop, matcher.end()));// + ";");
		    		else 
		    			extraction.put(extractTitle + extractionCounter, stkwrite.pop() + str.substring(lastStop, matcher.end()));
		    		stkwrite.push(stkwrite.pop() + extractTitle + extractionCounter);
		    		
		    		extractionCounter += 1;
		    		lastStop = matcher.end(); 
		    		
		    		// subtract one par level
		    		parenLevel -= 1;
		    		stkparen.pop();
	    		}
	    	};
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