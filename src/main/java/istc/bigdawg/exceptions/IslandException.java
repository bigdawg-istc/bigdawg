package istc.bigdawg.exceptions;

public class IslandException extends BigDawgException {
	
	private static final long serialVersionUID = -1046110325521330033L;

	public IslandException(String msg) {
		super(msg);
	}
	
	public IslandException(String msg, Exception e) {
		super(msg, e);
	}

}
