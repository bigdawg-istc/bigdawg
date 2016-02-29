package istc.bigdawg.packages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.utils.IslandsAndCast;
import istc.bigdawg.utils.IslandsAndCast.Scope;

public class CrossIslandQueryPlan {
	private Map<String, CrossIslandQueryNode> members;
	private Map<String, Operator> rootsForSchemas;
	private CrossIslandQueryNode root;
	private static Pattern start = Pattern.compile("^((bdrel\\()|(bdarray\\()|(bdgraph\\()|(bdtext\\()|(bdstream\\()|(bdcast\\())");
	private static Pattern end	 = Pattern.compile("\\);?$");
	private static int maxSerial = 0;
	private int serial;
	
	public CrossIslandQueryPlan() {
		members = new HashMap<>();
		maxSerial++;
		this.serial = maxSerial;
	};
	
	public CrossIslandQueryPlan(LinkedHashMap<String, String> queries) throws Exception {
		members = new LinkedHashMap<>();
		rootsForSchemas = new HashMap<>();
		addNodes(queries);
		maxSerial++;
		this.serial = maxSerial;
	};
	
	
	
	public void addNodes(Map<String, String> queries) throws Exception {

		List<String> l = new ArrayList<>(queries.keySet());
		Collections.sort(l, Collections.reverseOrder());
		
		for (String n : l) {
			
			// IDENTIFY ISLAND AND STRIP
			String rawQueryString = queries.get(n);
			Matcher islandMatcher = start.matcher(rawQueryString);
			Matcher queryEndMatcher = end.matcher(rawQueryString);
			
			CrossIslandQueryNode newNode;
			Scope scope;
			
			if (islandMatcher.find() && queryEndMatcher.find()) {
				
				// creating the children tables for this islands
				
				scope = IslandsAndCast.convertScope(rawQueryString.substring(islandMatcher.start(), islandMatcher.end()));
				newNode = new CrossIslandQueryNode(
						scope
						, rawQueryString.substring(islandMatcher.end(), queryEndMatcher.start())
						, n
						, rootsForSchemas);
				
				rootsForSchemas.put(n, newNode.getRemainder(0));
				
			} else 
				throw new Exception("Matcher cannot find token");
			
			
			
			if (n.equals("A_OUTPUT")) {
				root = newNode;
			}
			
			members.put(n, newNode);
		}
		
	}
	
	public CrossIslandQueryNode getRoot() {
		return root;
	};
	
	public CrossIslandQueryNode getMember(String tag) {
		return members.get(tag);
	}
	
	public Set<String> getMemberKeySet() {
		return members.keySet();
	}
	
	public Map<String, Operator> getRootsForSchema() {
		return rootsForSchemas;
	}
	
	public List<CrossIslandQueryNode> getMemberChildren (String memberTag) {
		
		ArrayList<CrossIslandQueryNode> extraction = new ArrayList<>();
		
		for (String child : members.get(memberTag).getChildren()) {
			extraction.add(members.get(child));
		};
		
		return extraction;
	}
	
	public int getSerial() {
		return serial;
	}
}
