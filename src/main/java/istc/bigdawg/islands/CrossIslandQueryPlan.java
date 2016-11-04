package istc.bigdawg.islands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.operators.Merge;

public class CrossIslandQueryPlan extends DirectedAcyclicGraph<CrossIslandPlanNode, DefaultEdge> 
	implements Iterable<CrossIslandPlanNode> {

	private static final long serialVersionUID = -3609729432970736589L;
	private Stack<Map<String, String>> transitionSchemas;
	private Set<CrossIslandPlanNode> entryNode;
	private CrossIslandPlanNode terminalNode;
	private static final String outputToken  = "BIGDAWG_OUTPUT";
	private static final String extractTitle = "BIGDAWGTAG_";
	private static final String castTitle = "BIGDAWGCAST_";
	private static int maxSerial = 0;
	private int serial;
	
	public CrossIslandQueryPlan() {
		super(DefaultEdge.class);
		maxSerial++;
		this.serial = maxSerial;
	}
	
	// NEW METHOD
	public CrossIslandQueryPlan(String userinput, Set<Integer> objectsToDelete) throws Exception {
		this();
//		terminalOperatorsForSchemas = new HashMap<>();
		transitionSchemas = new Stack<>();
		entryNode = new HashSet<>();
		addNodes(userinput, objectsToDelete);
		checkAndProcessUnionTerminalOperators();
	}
	
	public void addNodes(String userinput, Set<Integer> objectsToDelete) throws Exception {

		Pattern mark								= IslandsAndCast.QueryParsingPattern;
		Matcher matcher								= mark.matcher(userinput);
		
	    Stack<String> stkwrite						= new Stack<>();
	    Stack<Integer> stkparen						= new Stack<>();
	    Stack<Scope> lastScopes						= new Stack<>();
	    
	    // nodes in the last level
	    Stack<List<CrossIslandPlanNode>> nodeStack	= new Stack<>();
	    int lastLevel								= 0; 
	    Scope thisScope								= null;
	    Scope innerScope							= null;
	    
	    int extractionCounter 						= 0;
	    int parenLevel								= 0;
	    int lastStop								= 0;	// location of the place where it stopped last iteration
	    
	    stkwrite.push("");
	    stkparen.push(parenLevel);						// records level of parenthesis
	    
	    while (matcher.find()) {
	    	if ((userinput.charAt(matcher.start()) | 0x20) == 'b') {
	    		// update the prior and add a new one
	    		stkwrite.push(stkwrite.pop() + userinput.substring(lastStop, matcher.start()));
	    		lastStop = matcher.end(); 
	    		stkwrite.push(userinput.substring(matcher.start(), lastStop));
	    		
	    		lastScopes.push(IslandsAndCast.convertFunctionScope(userinput.substring(matcher.start(), matcher.end())));
	    		innerScope = null;
//	    		System.out.printf("Last scope: %s\n", lastScopes.peek());
	    		
	    		// add parse level
	    		parenLevel += 1;
	    		stkparen.push(parenLevel);
	    		transitionSchemas.push(new HashMap<>());
	    		nodeStack.push(new ArrayList<>());
	    	} else if (userinput.charAt(matcher.start()) == '(') {
	    		parenLevel += 1;
	    	} else {
	    		if (parenLevel != stkparen.peek()) {
	    			parenLevel -= 1;
	    		} else {
		    		// Pop current scope, because it's no longer useful.
	    			thisScope = lastScopes.pop();
	    			
	    			// finish and extract this entry, add new variable for the prior
	    			String name = null;
	    			
		    		if (parenLevel == 1)
		    			name = CrossIslandQueryPlan.getOutputToken();
		    		else if (!thisScope.equals(Scope.CAST)) {
		    			extractionCounter += 1;
		    			name = extractTitle + extractionCounter;
		    		} else 
		    			name = castTitle + parenLevel;
		    			
		    		
		    		// NEW
		    		Scope outterScope = lastScopes.isEmpty() ? null : lastScopes.peek();
//		    		System.out.printf("This Scope: %s; Outter Scope: %s\n", thisScope, outterScope);
		    		CrossIslandPlanNode newNode = createVertex(name, stkwrite.pop() + userinput.substring(lastStop, matcher.end()), thisScope, innerScope, outterScope, objectsToDelete);
		    		
		    		innerScope = thisScope;
		    		
		    		// if lastLevel is this level + 1, then connect everything and clear
		    		// otherwise if lastLevel is the same, then add this one
		    		// otherwise complain
		    		this.addVertex(newNode);
		    		if (nodeStack.peek().isEmpty()) 
		    			entryNode.add(newNode);
		    		if (name.equals(getOutputToken()))
		    			terminalNode = newNode;
		    		
		    		List<CrossIslandPlanNode> temp = nodeStack.pop();
		    		if (lastLevel <= parenLevel) {
		    			if (!nodeStack.isEmpty()) 
		    				nodeStack.peek().add(newNode);
		    		} else if (lastLevel > parenLevel ) {//+ 1) {
		    			for (CrossIslandPlanNode p : temp) {
	    					// create new edge
							this.addDagEdge(p, newNode);
		    			}
		    			if (!nodeStack.isEmpty()) 
		    				nodeStack.peek().add(newNode);
		    		} 
		    		
		    		lastLevel = parenLevel;
		    		// NEW END
		    		stkwrite.push(stkwrite.pop() + newNode.getName()); //extractTitle + extractionCounter);
		    		
//		    		if (! (newNode instanceof CrossIslandCastNode))
//	    			extractionCounter += 1;
		    		lastStop = matcher.end(); 
		    		
		    		// subtract one par level
		    		parenLevel -= 1;
		    		stkparen.pop();
		    		
	    		}
	    	}
	    }
		
	}
	
	private void checkAndProcessUnionTerminalOperators() {
		// TODO check the last operator and process it
		if (terminalNode instanceof CrossIslandQueryNode 
				&& ((CrossIslandQueryNode)terminalNode).getRemainderLoc() == null
				&& ((CrossIslandQueryNode)terminalNode).getRemainder(0) instanceof Merge) {
//			CrossIslandQueryNode node = (CrossIslandQueryNode) terminalNode;
			
		}
	}
	
	private CrossIslandPlanNode createVertex(String name, String rawQueryString, Scope thisScope, Scope innerScope, Scope outterScope, Set<Integer> catalogSOD) throws Exception{
		
		// IDENTIFY ISLAND AND STRIP
		Matcher islandMatcher	= IslandsAndCast.ScopeStartPattern.matcher(rawQueryString);
		Matcher queryEndMatcher = IslandsAndCast.ScopeEndPattern.matcher(rawQueryString);
		
		CrossIslandPlanNode newNode;
		
		if (islandMatcher.find() && queryEndMatcher.find()) {
			
			String islandQuery = rawQueryString.substring(islandMatcher.end(), queryEndMatcher.start());

			// check scope and direct traffic
			if (thisScope.equals(Scope.CAST)) {

				Matcher castSchemaMatcher = IslandsAndCast.CastSchemaPattern.matcher(islandQuery);
				if (castSchemaMatcher.find()) {
					
					Matcher castNameMatcher = IslandsAndCast.CastNamePattern.matcher(islandQuery);
					if (!castNameMatcher.find()) throw new Exception("Cannot find name for cast result: "+ islandQuery);
					
					// dummy scopes; source need to be changed below or when Edges happen
					newNode = new CrossIslandCastNode(innerScope, outterScope, 
							islandQuery.substring(castSchemaMatcher.start(), castSchemaMatcher.end()), 
							islandQuery.substring(castNameMatcher.start(), castNameMatcher.end()));
				} else 
					throw new Exception("Invalid Schema for CAST: "+ islandQuery);
				
				Matcher castSourceScopeMatcher = IslandsAndCast.CastScopePattern.matcher(islandQuery);
				// if not found then we rely on edge to make it happen
				if (castSourceScopeMatcher.find()) {
					((CrossIslandCastNode)newNode).setDestinationScope(IslandsAndCast.convertDestinationScope(islandQuery.substring(castSourceScopeMatcher.start(), castSourceScopeMatcher.end())));
				} 
				
				transitionSchemas.pop();
				transitionSchemas.peek().put(newNode.getName(), TheObjectThatResolvesAllDifferencesAmongTheIslands.getCreationQueryForCast(outterScope, newNode.getName(), islandQuery.substring(castSchemaMatcher.start(), castSchemaMatcher.end())));
				
				
				// add catalog entires
				int dbid = TheObjectThatResolvesAllDifferencesAmongTheIslands.getSchemaEngineDBID(((CrossIslandCastNode)newNode).getDestinationScope());
				catalogSOD.add(CatalogModifier.addObject(newNode.getName(), "TEMPORARY", dbid, dbid));
				
			} else if (TheObjectThatResolvesAllDifferencesAmongTheIslands.isOperatorBasedIsland(thisScope)) {
				newNode = new CrossIslandQueryNode(thisScope, islandQuery, name, transitionSchemas.pop());
			} else {
				newNode = new CrossIslandNonOperatorNode(thisScope, islandQuery, name);
			}
			
		} else 
			throw new Exception("Matcher cannot find token");
		
		return newNode;
	}; 
	
	// NEW METHOD END
	
	
	public CrossIslandPlanNode getTerminalNode() {
		return terminalNode;
	}
	
	public Stack<Map<String, String>> getTransitionSchemas() {
		return transitionSchemas;
	}
	
	public int getSerial() {
		return serial;
	}
	
	public Set<CrossIslandPlanNode> getEntryNodes() {
		return entryNode;
	} 
	
	public static String getOutputToken () {
		return new String (outputToken);
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Set<CrossIslandPlanNode> nodeList = new HashSet<>();
		nodeList.addAll(this.getEntryNodes());
		while (!nodeList.isEmpty()) {
			Set<CrossIslandPlanNode> nextGen = new HashSet<>();
			for (CrossIslandPlanNode n : nodeList) {
				for (DefaultEdge e : this.edgesOf(n)) {
					if (this.getEdgeTarget(e) == n)  continue;
					sb.append('(').append(this.getEdgeSource(e)).append(" -> ").append(this.getEdgeTarget(e)).append(")\n");
					nextGen.add(this.getEdgeTarget(e));
				}
			}
			nodeList = nextGen; 
		}
		return sb.toString();
	}
}
