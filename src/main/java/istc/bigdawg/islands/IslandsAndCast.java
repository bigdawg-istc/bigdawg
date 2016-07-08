package istc.bigdawg.islands;

import java.util.regex.Pattern;

import istc.bigdawg.exceptions.UnsupportedIslandException;

public class IslandsAndCast {
	public enum Scope {
		RELATIONAL, ARRAY, KEYVALUE, TEXT, GRAPH, DOCUMENT, STREAM, CAST 
	}
	
	public static Pattern ScopeStartPattern	= Pattern.compile("^((bdrel\\()|(bdarray\\()|(bdkv\\()|(bdtext\\()|(bdgraph\\()|(bddoc\\()|(bdstream\\()|(bdcast\\())");
	public static Pattern ScopeEndPattern	= Pattern.compile("\\) *;? *$");
	public static Pattern CastScopePattern	= Pattern.compile("(?i)(relational|array|keyvalue|text|graph|document|stream)\\) *;? *$");
	public static Pattern CastSchemaPattern	= Pattern.compile("(?<=([_a-z0-9, ]+')).*(?=(' *, *(relational|array|keyvalue|text|graph|document|stream)))");
	public static Pattern CastNamePattern	= Pattern.compile("(?<=(, ))([_@0-9a-zA-Z]+)(?=, *')");
	
	public static Scope convertFunctionScope (String prefix) throws UnsupportedIslandException {
		switch (prefix) {
		case "bdrel(":
		case "bdrel":
			return Scope.RELATIONAL;
		case "bdarray(":
		case "bdarray":
			return Scope.ARRAY;
		case "bdkv(":
		case "bdkv":
			return Scope.KEYVALUE;
		case "bdtext(":
		case "bdtext":
			return Scope.TEXT;
		case "bdgraph(":
		case "bdgraph":
			return Scope.GRAPH;
		case "bddoc(":
		case "bddoc":
			return Scope.DOCUMENT;
		case "bdstream(":
		case "bdstream":
			return Scope.STREAM;
		case "bdcast(":
		case "bdcast":
			return Scope.CAST;
		default:
			throw new UnsupportedIslandException(prefix);
		}
		
	}
	
	public static Scope convertDestinationScope (String prefix) throws UnsupportedIslandException {
		switch (prefix.toLowerCase()) {
		case "relational":
			return Scope.RELATIONAL;
		case "array":
			return Scope.ARRAY;
		case "keyvalue":
			return Scope.KEYVALUE;
		case "text":
			return Scope.TEXT;
		case "graph":
			return Scope.GRAPH;
		case "document":
			return Scope.DOCUMENT;
		case "stream":
			return Scope.STREAM;
		case "cast":
			return Scope.CAST;
		default:
			throw new UnsupportedIslandException(prefix);
		}
		
	}
	
}