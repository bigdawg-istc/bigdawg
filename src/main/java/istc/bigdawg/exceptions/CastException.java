package istc.bigdawg.exceptions;

public class CastException extends BigDawgException {
	

	private static final long serialVersionUID = -3526511443426266771L;

	public CastException(String msg) {
		super(msg);
	}
	
	public CastException(String msg, Exception e) {
		super(msg, e);
	}

}
