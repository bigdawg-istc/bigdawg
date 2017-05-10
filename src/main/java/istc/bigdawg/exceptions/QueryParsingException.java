package istc.bigdawg.exceptions;

public class QueryParsingException extends IslandException {

	private static final long serialVersionUID = 6840200200405864002L;

	public QueryParsingException(String msg) {
		super(msg);
	}
	
	public QueryParsingException(String msg, Exception e) {
		super(msg, e);
	}

}
