package istc.bigdawg.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;


import istc.bigdawg.exceptions.ApiException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.ConstructedQueryResult;
import istc.bigdawg.executor.IslandQueryResult;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextOperator;
import istc.bigdawg.islands.text.operators.TextScan;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.utils.LogUtils;

public class ApiExecutorEngine implements DBHandler, ExecutorEngine {

    private ConnectionInfo connectionInfo;

    public ApiExecutorEngine(ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    public void dropDataSetIfExists(String dataSetName) {
    }

    public Optional<QueryResult> execute(final String query) throws LocalQueryExecutionException {
        return null;
    }

    @Override
    public Response executeQuery(String queryString) {
        return null;
    }

    @Override
    public Shim getShim() {
        return null;
    }

    @Override
    public ObjectMetaData getObjectMetaData(String name) throws Exception {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean existsObject(String name) throws Exception {
        return false;
    }

    @Override
    public void close() throws Exception {
    }
}