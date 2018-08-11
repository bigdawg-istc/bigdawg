package istc.bigdawg.executor;


import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.utils.Tuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class RESTQueryResult implements QueryResult, Serializable {

    private List<Tuple.Tuple3<String, String, Boolean>> columns;
    private List<String> rows;
    private List<Map<String, Object>> rowsWithHeadings;
    transient private RESTConnectionInfo connectionInfo;

    /**
     *
     * @param columns (key: Column Name -> Value: column type)
     * @param rows list of rows
     */
    public RESTQueryResult(List<Tuple.Tuple3<String, String, Boolean>> columns,
                           List<String> rows,
                           List<Map<String, Object>> rowsWithHeadings,
                           RESTConnectionInfo connectionInfo) {
        this.columns = columns;
        this.rows = rows;
        this.rowsWithHeadings = rowsWithHeadings;
        this.connectionInfo = connectionInfo;
    }

    public List<Tuple.Tuple3<String, String, Boolean>> getColumns() {
        return columns;
    }

    public List<String> getRows() {
        return rows;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public List<Map<String, Object>> getRowsWithHeadings() {
        return rowsWithHeadings;
    }

    public String toPrettyString() {
        StringJoiner joinerRow = new StringJoiner("\n");
        for (String row: rows) {
            joinerRow.add(row);
        }
        return joinerRow.toString();
    }
}
