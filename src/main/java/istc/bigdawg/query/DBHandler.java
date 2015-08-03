package istc.bigdawg.query;

import istc.bigdawg.BDConstants.Shim;

import javax.ws.rs.core.Response;

public interface DBHandler {
	Response executeQuery(String queryString);
	Shim getShim();
}
