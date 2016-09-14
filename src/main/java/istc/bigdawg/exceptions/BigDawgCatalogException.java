package istc.bigdawg.exceptions;

public class BigDawgCatalogException extends BigDawgException {

	private static final long serialVersionUID = 8081583650337850302L;

	public BigDawgCatalogException(String msg) {
		super(msg);
	}

	public BigDawgCatalogException(Integer dbid) {
		super(dbid == -1 ? "None of the referenced object were listed in catalog; source of reference unknown" : "Connection Info Not Found: "+dbid);
	}
}
