package istc.bigdawg.signature;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import convenience.RTED;
import istc.bigdawg.packages.QueryContainerForCommonDatabase;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import istc.bigdawg.utils.IslandsAndCast.Scope;

public class Signature {
	
	private Scope island;
	private String sig1;
	private List<String> sig2;
	private List<String> sig3;
	private String query;
	private List<String> sig4k;
//	private String identifier; 
	
//	private static Pattern possibleObjectsPattern	= Pattern.compile("[_@a-zA-Z0-9]+");
//	private static Pattern tagPattern				= Pattern.compile("BIGDAWGTAG_[0-9_]+");
	
	/**
	 * Construct a signature of three parts: sig1 tells about the structure, sig2 are all object references, sig3 are all constants
	 * @param cc
	 * @param query
	 * @param island
	 * @throws Exception
	 */
	public Signature(String query, Scope island, Operator root, Map<String, QueryContainerForCommonDatabase> container) throws Exception {
		
		if (island.equals(Scope.RELATIONAL)){
			setSig2(RelationalSignatureBuilder.sig2(query));
			setSig3(RelationalSignatureBuilder.sig3(query));
		} else if (island.equals(Scope.ARRAY)) {
			setSig2(ArraySignatureBuilder.sig2(query));
			setSig3(ArraySignatureBuilder.sig3(query));
		} else {
			throw new Exception("Invalid Signature island input: "+island);
		}
		
		
		List<String> cs = new ArrayList<>();
		for (String s : container.keySet()) 
			cs.add(container.get(s).generateTreeExpression());
		setSig4k(cs);
		setSig1(root.getTreeRepresentation(true));
		
		this.setQuery(query);
		this.setIsland(island);
//		this.setIdentifier(identifier);
	}
	
	
	public static double getTreeEditDistance(String s1, String s2) {
		return RTED.computeDistance(s1, s2);
	}
	

	public void print() {
		System.out.println("Type       : Signature");
//		System.out.println("Identifier : "+identifier);
		System.out.println("Island     : "+island);
		System.out.println("Signature 1: "+sig1);
		System.out.println("Signature 2: "+sig2);
		System.out.println("Signature 3: "+sig3);
		System.out.println("Query      : "+query);
	}
	
	public String getSig1() {
		return sig1;
	}

	public void setSig1(String sig1) {
		this.sig1 = sig1;
	}

	public List<String> getSig2() {
		return sig2;
	}

	public void setSig2(List<String> sig2) {
		this.sig2 = sig2;
	}

	public List<String> getSig3() {
		return sig3;
	}

	public void setSig3(List<String> sig3) {
		this.sig3 = sig3;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public Scope getIsland() {
		return island;
	}
	
	private void setIsland(Scope island) {
		this.island = island;
	}

	public List<String> getSig4k() {
		return sig4k;
	}

	public void setSig4k(List<String> sig4k) {
		this.sig4k = sig4k;
	}
	
	
	
}