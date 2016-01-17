package istc.bigdawg.packages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.utils.IslandsAndCast;

public class CrossIslandQueryPlan {
	private Map<String, CrossIslandQueryNode> members;
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
	
	public CrossIslandQueryPlan(Map<String, String> queries) throws Exception {
		members = new HashMap<>();
		addNodes(queries);
		maxSerial++;
		this.serial = maxSerial;
	};
	
	
	
	public void addNodes(Map<String, String> queries) throws Exception {

		for (String n : queries.keySet()) {
			
			// IDENTIFY ISLAND AND STRIP
			String rawQueryString = queries.get(n);
			Matcher islandMatcher = start.matcher(rawQueryString);
			Matcher queryEndMatcher = end.matcher(rawQueryString);
			
			CrossIslandQueryNode newNode;
			
			if (islandMatcher.find() && queryEndMatcher.find()) {
				
				
				newNode = new CrossIslandQueryNode(
						IslandsAndCast.convertScope(rawQueryString.substring(islandMatcher.start(), islandMatcher.end()))
						, rawQueryString.substring(islandMatcher.end(), queryEndMatcher.start())
						, n);
				
				
			} else 
				throw new Exception("Matcher cannot find token");
			
			
			if (n.equals("OUTPUT")) {
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
