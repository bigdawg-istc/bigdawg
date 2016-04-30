package istc.bigdawg.executor;

import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.QueryResponse;
import istc.bigdawg.utils.JdbcUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by ankush on 4/23/16.
 */
public class JdbcQueryResult implements QueryResult {
    private final ResultSet results;
    private final ConnectionInfo connectionInfo;
    private final List<List<String>> rows;
    private final List<String> colNames;
    private final List<String> colTypes;

    public JdbcQueryResult(ResultSet resultSet, ConnectionInfo conn) throws SQLException {
        this.results = resultSet;
        this.rows = JdbcUtils.getRows(resultSet);
        this.colNames = JdbcUtils.getColumnNames(resultSet.getMetaData());
        this.colTypes = JdbcUtils.getColumnTypes(resultSet.getMetaData());
        this.connectionInfo = conn;
    }

    public ResultSet getResults() {
        return results;
    }

    @Override
    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public List<String> getColNames() {
        return colNames;
    }

    public List<String> getColTypes() {
        return colTypes;
    }

    public String toPrettyString() {
        return JdbcUtils.printResultSet(rows, colNames);
    }
}
