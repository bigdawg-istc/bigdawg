package istc.bigdawg.utils;

public class IslandsAndCast {
	public enum Scope {
		RELATIONAL, ARRAY, GRAPH, TEXT, STREAM, CAST 
	}
	
	public static Scope convertScope (String prefix) throws Exception {
		switch (prefix) {
		case "bdrel":
		case "bdrel(":
			return Scope.RELATIONAL;
		case "bdarray":
		case "bdarray(":
			return Scope.ARRAY;
		case "bdgraph":
		case "bdgraph(":
			return Scope.GRAPH;
		case "bdtext":
		case "bdtext(":
			return Scope.TEXT;
		case "bdstream":
		case "bdstream(":
			return Scope.STREAM;
		case "bdcast":
		case "bdcast(":
			return Scope.CAST;
		default:
			throw new Exception("Unsupported island. Input token: "+ prefix);
		}
		
	}
}