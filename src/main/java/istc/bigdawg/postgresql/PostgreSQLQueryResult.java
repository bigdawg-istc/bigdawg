package istc.bigdawg.postgresql;

import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;

import java.util.List;

/**
 * Created by ankush on 4/22/16.
 */
public class PostgreSQLQueryResult implements QueryResult {
    private List<List<String>> rows;
    private List<String> types;
    private List<String> colNames;
    private ConnectionInfo connInfo;

    /**
	 * @return the rows
	 */
    public List<List<String>> getRows() {
        return rows;
    }

    /**
	 * @return the types
	 */
    public List<String> getTypes() {
        return types;
    }

    /**
	 * @return the colNames
	 */
    public List<String> getColNames() {
        return colNames;
    }

    /**
	 * @param rows
	 * @param types
	 * @param colNames
	 */
    public PostgreSQLQueryResult(List<List<String>> rows, List<String> types, List<String> colNames, ConnectionInfo connInfo) {
        super();
        this.rows = rows;
        this.types = types;
        this.colNames = colNames;
        this.connInfo = connInfo;
    }

    public String toPrettyString() {
        // print the result;
        StringBuffer out = new StringBuffer();

        for (String name : colNames) {
            out.append("\t" + name);
        }
        out.append("\n");

        int rowCounter = 1;
        for (List<String> row : rows) {
            out.append(rowCounter + ".");
            for (String s : row) {
                out.append("\t" + s);
            }
            out.append("\n");
            rowCounter += 1;
        }

        return out.toString();
    }

    public ConnectionInfo getConnectionInfo() {
        return this.connInfo;
    }

}
