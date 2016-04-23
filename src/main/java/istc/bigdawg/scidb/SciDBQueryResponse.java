package istc.bigdawg.scidb;

import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.QueryResponse;

/**
 * Created by ankush on 4/23/16.
 */
public class SciDBQueryResponse implements QueryResult {
    final String result;
    final ConnectionInfo conn;

    public SciDBQueryResponse(String result, ConnectionInfo conn) {
        this.result = result;
        this.conn = conn;
    }

    public ConnectionInfo getConnectionInfo() {
        return conn;
    }


    public String toPrettyString() {
        return result;
    }
}
