package istc.bigdawg.islands.api;

import java.sql.SQLException;
import java.util.*;

import istc.bigdawg.api.ApiHandler;
import istc.bigdawg.islands.api.operators.ApiSeqScan;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import istc.bigdawg.accumulo.AccumuloExecutionEngine;
import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.exceptions.QueryParsingException;
import istc.bigdawg.islands.IntraIslandQuery;
import istc.bigdawg.islands.Island;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextScan;
import istc.bigdawg.properties.BigDawgConfigProperties;

public class ApiIsland implements Island {

    public static ApiIsland INSTANCE = new ApiIsland();

    protected ApiIsland() {

    }

    @Override
    public IntraIslandQuery getIntraIslandQuery(String islandQuery, String name, Map<String, String> transitionSchemas) throws IslandException {
        try {
            return new ApiIslandQuery(islandQuery, name, transitionSchemas);
        } catch (Exception e) {
            throw new IslandException(e.getMessage(), e);
        }
    };

    @Override
    public void setupForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas)
            throws IslandException {

        Set<String> createdTables = new HashSet<>();
        for (String key : outputTransitionSchemas.keySet())
            if (outputChildren.contains(key)) {
                try {
                    createdTables.add(key);
                    throw new IslandException("API island does not support creating tables");
                    //                    handler.createTable(key);
                } catch (Exception e) {
                    for (String s : createdTables) {
                    }
                    throw new IslandException(e.getMessage(), e);
                }
            }
    }

    @Override
    public void teardownForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas)
            throws IslandException {
    }

    @Override
    public String getCreateStatementForTransitionTable(String tableName, String attributes) {
        return String.format("CREATE ARRAY %s %s", tableName, attributes);
    }

    @Override
    public int addCatalogObjectEntryForTemporaryTable(String tableName) throws IslandException {
        try {
            return CatalogModifier.addObject(tableName, "TEMPORARY", 0, 0); // @TODO make the newLogDB and new PhyDB be the proper IDs
        } catch (SQLException | BigDawgCatalogException e) {
            throw new IslandException(e.getMessage(), e);
        }
    }

    @Override
    public Operator parseQueryAndExtractAllTableNames(String queryString, List<String> tables) throws IslandException {
        Operator root;
        try {
            root = (new ApiJSONQueryParser()).parse(queryString);
            if (!(root instanceof ApiSeqScan))
                throw new IslandException("API island does not support operator "+root.getClass().getName()+"; "
                        + "parseQueryAndExtractAllTableNames");

//            @TODO add execution tree?
        } catch (Exception e) {
            throw new QueryParsingException(e.toString(), e);
        }
        tables.add(((ApiSeqScan)root).getSourceTableName());
        return root;
    }

    @Override
    public Set<String> splitJoinPredicate(String predicates) throws IslandException {
        throw new IslandException("API island does not implement 'splitPredicates'");
    }

    @Override
    public Integer getDefaultCastReceptionDBID() {
        return 0;
    }

    @Override
    public List<String> getLiteralsAndConstantsSignature(String query) throws IslandException {

        // bdapi({ 'name' : 'twitter', 'endpoint' : 'tweets', 'query' : { 'q': '#mit' } })
        List<String> output = new ArrayList<>();
        try {
            JSONObject parsedObject = (JSONObject) (new JSONParser()).parse(query.replaceAll("[']", "\""));
            JSONObject queryObject = (JSONObject)ApiJSONQueryParser.getObjectByType(parsedObject.get("query"), JSONObject.class);
            if (queryObject != null) {
                for (Object key : queryObject.keySet()) {
                    String keyStr = (String) ApiJSONQueryParser.getObjectByType(key, String.class);
                    String value = (String) ApiJSONQueryParser.getObjectByType(queryObject.get(keyStr), String.class);
                    ApiJSONQueryParser.addNonNullStringToList(keyStr, output);
                    ApiJSONQueryParser.addNonNullStringToList(value, output);
                }
            }
            else if (parsedObject.containsKey("query-raw")) {
                String queryRaw = (String)ApiJSONQueryParser.getObjectByType(parsedObject.get("query-raw"), String.class);
                ApiJSONQueryParser.addNonNullStringToList(queryRaw, output);
            }
        } catch (ParseException e) {
            throw new QueryParsingException(e.getMessage(), e);
        }
        return output;
    }

    @Override
    public String wrapQueryInIslandIdentifier(String query) {
        return String.format("bdapi(%s);", query);
    }

    @Override
    public Join constructJoin(Operator o1, Operator o2, JoinType jt, List<String> joinPred, boolean isFilter)
            throws IslandException {
        throw new IslandException("API island does support Join Operator");
    }

}
