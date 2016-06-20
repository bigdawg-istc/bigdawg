package istc.bigdawg.utils;

import java.util.regex.Pattern;

public class IslandsAndCast {
	public enum Scope {
		RELATIONAL, ARRAY, GRAPH, DOCUMENT, STREAM, CAST 
	}
	
	public static Pattern ScopeStartPattern	= Pattern.compile("^((bdrel\\()|(bdarray\\()|(bdgraph\\()|(bddoc\\()|(bdstream\\()|(bdcast\\())");
	public static Pattern ScopeEndPattern	= Pattern.compile("\\) *;? *$");
	public static Pattern CastScopePattern	= Pattern.compile("(?i)(relational|array|graph|document|stream)\\) *;? *$");
	public static Pattern CastSchemaPattern	= Pattern.compile("(?<=([_a-z0-9, ]+')).*(?=(' *, *(relational|array|graph|document|stream)))");
	public static Pattern CastNamePattern	= Pattern.compile("(?<=(, ))([_@0-9a-zA-Z]+)(?=, *')");
	
	public static Scope convertFunctionScope (String prefix) throws Exception {
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
		case "bddoc":
		case "bddoc(":
			return Scope.DOCUMENT;
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
	
	public static Scope convertDestinationScope (String prefix) throws Exception {
		switch (prefix.toLowerCase()) {
		case "relational":
			return Scope.RELATIONAL;
		case "array":
			return Scope.ARRAY;
		case "graph":
			return Scope.GRAPH;
		case "document":
			return Scope.DOCUMENT;
		case "stream":
			return Scope.STREAM;
		default:
			throw new Exception("Unsupported island. Input token: "+ prefix);
		}
		
	}
}