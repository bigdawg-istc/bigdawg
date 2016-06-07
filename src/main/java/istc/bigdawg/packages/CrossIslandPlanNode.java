package istc.bigdawg.packages;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.utils.IslandsAndCast.Scope;

public class CrossIslandPlanNode {

	protected Scope sourceScope;
	protected String queryString;
	protected String name;
	
	public CrossIslandPlanNode (Scope sourceScope, String islandQuery, String name) {
		setSourceScope(sourceScope);
		setQueryString(islandQuery);
		setName(name);
	}
	
	public Scope getSourceScope() {
		return this.sourceScope;
	}
	
	public void setSourceScope(Scope newScope) {
		this.sourceScope = newScope;
	}
	
	public String getQueryString() {
		return this.queryString;
	}
	
	public void setQueryString(String query) {
		this.queryString = query;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String newName) {
		this.name = newName;
	}
	
	
	public CrossIslandPlanNode getTargetVertex(CrossIslandQueryPlan ciqp) throws Exception {
		List<CrossIslandPlanNode> target = getSourceOrTarget(ciqp, false);
		if (target.isEmpty()) return null;
		else return target.get(0);
	}
	
	protected List<CrossIslandPlanNode> getSourceOrTarget(CrossIslandQueryPlan ciqp, boolean isSource) throws Exception {
		
		if (!ciqp.containsVertex(this)) throw new Exception("CrossIslandQueryPlan does not contain cast node: "+this.getQueryString());
		
		List<CrossIslandPlanNode> output = new ArrayList<>();
		Set<DefaultEdge> edges = ciqp.edgesOf(this);
		for (DefaultEdge e : edges) {
			
			CrossIslandPlanNode node;
			if (isSource) node = ciqp.getEdgeSource(e);
			else node = ciqp.getEdgeTarget(e);
			
			if (node != this) output.add(node); // just test the pointers
		} 
		
		return output;
	}
}
