package istc.bigdawg.executor;

import istc.bigdawg.query.ConnectionInfo;

import java.util.List;

/**
 * Created by ankush on 4/21/16.
 */
public interface QueryResult {
    String toPrettyString();
    ConnectionInfo getConnectionInfo();
}
