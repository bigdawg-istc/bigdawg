package istc.bigdawg.exceptions;

public class ShimException extends BigDawgException {

	private static final long serialVersionUID = 3560208049207103675L;

	public ShimException(String msg) {
		super(msg);
	}

	public ShimException(String msg, Exception e) {
		super(msg, e);
	}
}
