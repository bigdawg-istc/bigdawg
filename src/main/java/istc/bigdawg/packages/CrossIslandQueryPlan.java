package istc.bigdawg.packages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.utils.IslandsAndCast;

public class CrossIslandQueryPlan extends DirectedAcyclicGraph<CrossIslandQueryNode, DefaultEdge> 
	implements Iterable<CrossIslandQueryNode> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3609729432970736589L;
	private Map<String, Operator> terminalOperatorsForSchemas;
	private CrossIslandQueryNode terminalNode;
	private static final String outputToken = "A_OUTPUT";
	private static Pattern start = Pattern.compile("^((bdrel\\()|(bdarray\\()|(bdgraph\\()|(bdtext\\()|(bdstream\\()|(bdcast\\())");
	private static Pattern end	 = Pattern.compile("\\);?$");
	private static Pattern bigdawgtagpattern = Pattern.compile("(BIGDAWGTAG_[0-9]+)|("+outputToken+")");
	private static int maxSerial = 0;
	private int serial;
	
	
	
	public CrossIslandQueryPlan() {
		super(DefaultEdge.class);
//		members = new HashMap<>();
		maxSerial++;
		this.serial = maxSerial;
	};
	
	public CrossIslandQueryPlan(Map<String, String> queries) throws Exception {
		this();
		terminalOperatorsForSchemas = new HashMap<>();
		addNodes(queries);
	};
	
	
	
	public void addNodes(Map<String, String> queries) throws Exception {

		// create a mapping between sources and destinations
		
		List<String> l = new ArrayList<>(queries.keySet());
		Collections.sort(l, Collections.reverseOrder());
		
		System.out.printf("--> queries: %s;\n", queries);
		
		Map<String, List<String>> dependencies = new HashMap<>();
		Map<String, CrossIslandQueryNode> members = new HashMap<>();
		
		for (String n : l) {
			
			// IDENTIFY ISLAND AND STRIP
			String rawQueryString = queries.get(n);
			Matcher islandMatcher = start.matcher(rawQueryString);
			Matcher queryEndMatcher = end.matcher(rawQueryString);
			
			CrossIslandQueryNode newNode;
			
			if (islandMatcher.find() && queryEndMatcher.find()) {
				
				// creating the children tables for this islands
				newNode = new CrossIslandQueryNode(
						IslandsAndCast.convertScope(rawQueryString.substring(islandMatcher.start(), islandMatcher.end()))
						, rawQueryString.substring(islandMatcher.end(), queryEndMatcher.start())
						, n
						, terminalOperatorsForSchemas);
				
				this.addVertex(newNode);
				terminalOperatorsForSchemas.put(n, newNode.getRemainder(0));
				
				// adding the dependencies for edge matching
				Matcher m = bigdawgtagpattern.matcher(rawQueryString.substring(islandMatcher.end(), queryEndMatcher.start()));
				dependencies.put(n, new ArrayList<>());
				while(m.find()) 
					dependencies.get(n).add(rawQueryString.substring(m.start(),m.end()));
				
			} else 
				throw new Exception("Matcher cannot find token");
			
			if (n.equals(getOutputToken())) {
				terminalNode = newNode;
			}
			
			members.put(n, newNode);
		}
		
		// edges
		for (String n : dependencies.keySet()) {
			for (String n2: dependencies.get(n)) {
				this.addEdge(members.get(n2), members.get(n));
			}
		}
	}
	
	public CrossIslandQueryNode getTerminalNode() {
		return terminalNode;
	};
	
	
//	public CrossIslandQueryNode getMember(String tag) {
//		return members.get(tag);
//	}
//	
//	public Set<String> getMemberKeySet() {
//		return members.keySet();
//	}
	
	public Map<String, Operator> getTerminalsForSchemas() {
		return terminalOperatorsForSchemas;
	}
	
//	public List<CrossIslandQueryNode> getMemberChildren (String memberTag) {
//		
//		ArrayList<CrossIslandQueryNode> extraction = new ArrayList<>();
//		
////		for (String child : members.get(memberTag).getChildren()) {
////			extraction.add(members.get(child));
////		};
//		
//		return extraction;
//	}
	
	public int getSerial() {
		return serial;
	}
	
	public static String getOutputToken () {
		return new String (outputToken);
	}
}
