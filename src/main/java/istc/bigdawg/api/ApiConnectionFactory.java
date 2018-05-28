package istc.bigdawg.api;

import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.query.ConnectionInfo;

import java.util.Map;

public class ApiConnectionFactory {
    public static ConnectionInfo get(String host, String port,
                                     String database, String user, String password, String connectionParameters) throws BigDawgCatalogException {

        Map<String, String> connectionParametersMap = AbstractApiConnectionInfo.parseConnectionParameters(connectionParameters);
        String connectionType = "REST";
        if (connectionParametersMap.containsKey("type")) {
            switch (connectionParametersMap.get("type").toUpperCase()) {
                case "REST":
                    break;
                case "GRAPH-QL":
                case "GRAPH_QL":
                case "GRAPHQL":
                    throw new BigDawgCatalogException("GraphQL connection types are not yet supported");
                default:
                    throw new BigDawgCatalogException("Unknown/Unsupported API connection type: " + connectionParametersMap.get("type"));
            }
        }
        switch(connectionType) {
            case "REST":
                return new RESTConnectionInfo(host, port, database, user, password, connectionParametersMap);
            default:
                throw new BigDawgCatalogException("Unknown/Unsupported API connection type(1): " + connectionType);
        }
    }
}
