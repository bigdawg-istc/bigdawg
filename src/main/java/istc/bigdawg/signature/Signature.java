package istc.bigdawg.signature;

import istc.bigdawg.catalog.Catalog;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;

public class Signature {
	
	private String island;
	private String sig1;
	private String sig2;
	private String sig3;
	private String query;
	private String identifier; 
	
	/**
	 * Construct a signature of three parts: sig1 tells about the structure, sig2 are all object references, sig3 are all constants
	 * @param cc
	 * @param query
	 * @param island
	 * @throws Exception
	 */
	public Signature(Catalog cc, String query, String island, String identifier) throws Exception {
		
		switch (island.toLowerCase()) {
			case "relational":
				setSig1(RelationalSignatureBuilder.sig1(query));
				setSig2(RelationalSignatureBuilder.sig2(cc, query));
				setSig3(RelationalSignatureBuilder.sig3(query));
				break;
			case "array":
				setSig1(ArraySignatureBuilder.sig1(query));
				setSig2(ArraySignatureBuilder.sig2(cc, query));
				setSig3(ArraySignatureBuilder.sig3(query));
				break;
			default:
				throw new Exception("Invalid Signature island input: "+island);
		}
		this.setQuery(query);
		this.setIsland(island);
		this.setIdentifier(identifier);
	}

	public void print() {
		System.out.println("Type       : Signature");
		System.out.println("Identifier : "+identifier);
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

	public String getSig2() {
		return sig2;
	}

	public void setSig2(String sig2) {
		this.sig2 = sig2;
	}

	public String getSig3() {
		return sig3;
	}

	public void setSig3(String sig3) {
		this.sig3 = sig3;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getIsland() {
		return island;
	}
	
	private void setIsland(String island) {
		this.island = island;
	}
	

	public String getIdentifier() {
		return identifier;
	}
	

	private void setIdentifier(String identifier) {
		this.identifier = identifier;
	}
	

}