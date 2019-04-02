package istc.bigdawg.islands.text;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;
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

public class TextIsland implements Island {

	public static TextIsland INSTANCE = new TextIsland();
	
	private static final int accumuloSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getAccumuloSchemaServerDBID();
	private static AccumuloConnectionInfo textSchemaServerConnectionInfo = null;
	
	static {
		try {
			textSchemaServerConnectionInfo = 
				(AccumuloConnectionInfo)CatalogViewer.getConnectionInfo(accumuloSchemaServerDBID);
		} catch (SQLException | BigDawgCatalogException e) {
			e.printStackTrace();
		}
	}
	
	protected TextIsland() {
		
	}
	
	@Override 
	public IntraIslandQuery getIntraIslandQuery(String islandQuery, String name, Map<String, String> transitionSchemas) throws IslandException {
		try {
			return new TextIslandQuery(islandQuery, name, transitionSchemas);
		} catch (Exception e) {
			throw new IslandException(e.getMessage(), e);
		}
	};
	
	@Override
	public void setupForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas)
			throws IslandException {
		
		AccumuloExecutionEngine handler = null;
		Set<String> createdTables = new HashSet<>();
		try {
			handler = new AccumuloExecutionEngine(textSchemaServerConnectionInfo);
		} catch (AccumuloException | BigDawgException | AccumuloSecurityException e0) {
			throw new IslandException(e0.getMessage(), e0);
		}
		for (String key : outputTransitionSchemas.keySet()) 
			if (outputChildren.contains(key)) {
				try {
					createdTables.add(key);
					handler.createTable(key);
				} catch (Exception e) {
					for (String s : createdTables) {
						try {
							handler.dropDataSetIfExists(s);
						} catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e1) {
							throw new IslandException(e1.getMessage(), e1);
						}
					} 
					throw new IslandException(e.getMessage(), e);
				}
			}
	}

	@Override
	public void teardownForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas)
			throws IslandException {
		try {
			AccumuloExecutionEngine	handler = new AccumuloExecutionEngine(textSchemaServerConnectionInfo);
			for (String key : outputTransitionSchemas.keySet()) 
				if (outputChildren.contains(key)) {
					handler.dropDataSetIfExists(key);
				}
		} catch (BigDawgException | AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public String getCreateStatementForTransitionTable(String tableName, String attributes) {
		AccumuloExecutionEngine.addExecutionTree(AccumuloExecutionEngine.AccumuloCreateTableCommandPrefix+tableName, null);
		return tableName;
	}

	@Override
	public int addCatalogObjectEntryForTemporaryTable(String tableName) throws IslandException {
		try {
			return CatalogModifier.addObject(tableName, "TEMPORARY", accumuloSchemaServerDBID, accumuloSchemaServerDBID);
		} catch (SQLException | BigDawgCatalogException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public Operator parseQueryAndExtractAllTableNames(String queryString, List<String> tables) throws IslandException {
		Operator root;
		try {
			root = (new AccumuloJSONQueryParser()).parse(queryString);
			if (!(root instanceof TextScan))
				throw new IslandException("TEXT island does not support operator "+root.getClass().getName()+"; "
						+ "parseQueryAndExtractAllTableNames");
			
			AccumuloExecutionEngine.addExecutionTree(root.getSubTreeToken(), root);
		} catch (Exception e) {
			throw new QueryParsingException(e.toString(), e);
		}
		tables.add(((TextScan)root).getSourceTableName());
		return root;
	}

	@Override
	public Set<String> splitJoinPredicate(String predicates) throws IslandException {
		throw new IslandException("TEXT island does not implement 'splitPredicates'");
	}

	@Override
	public Integer getDefaultCastReceptionDBID() {
		return accumuloSchemaServerDBID;
	}

	@Override
	public List<String> getLiteralsAndConstantsSignature(String query) throws IslandException {
		
		// bdtext({ 'op' : 'scan', 'table' : 'note_events_TedgeT', 'range' : { 'start' : ['word|yesr','',''], 'end' : ['word|yet','','']} })
		List<String> output = new ArrayList<>();
		try {
			JSONObject parsedObject = (JSONObject) (new JSONParser()).parse(query.replaceAll("[']", "\""));
			JSONObject range = (JSONObject) AccumuloJSONQueryParser.getObjectByType(parsedObject.get("range"), JSONObject.class);
			if (range != null) {
				JSONArray startArray = (JSONArray)AccumuloJSONQueryParser.getObjectByType(range.get("start"), JSONArray.class);
				if (startArray != null) {
					AccumuloJSONQueryParser.addNonNullStringToList(startArray.get(0), output);
					AccumuloJSONQueryParser.addNonNullStringToList(startArray.get(1), output);
					AccumuloJSONQueryParser.addNonNullStringToList(startArray.get(2), output);
				}
				JSONArray endArray = (JSONArray)AccumuloJSONQueryParser.getObjectByType(range.get("end"), JSONArray.class);
				if (endArray != null) {
					AccumuloJSONQueryParser.addNonNullStringToList(endArray.get(0), output);
					AccumuloJSONQueryParser.addNonNullStringToList(endArray.get(1), output);
					AccumuloJSONQueryParser.addNonNullStringToList(endArray.get(2), output);
				}
			}
		} catch (ParseException e) {
			throw new QueryParsingException(e.getMessage(), e);
		}
		return output;
	}

	@Override
	public String wrapQueryInIslandIdentifier(String query) {
		return String.format("bdtext(%s);", query);
	}

	@Override
	public Join constructJoin(Operator o1, Operator o2, JoinType jt, List<String> joinPred, boolean isFilter)
			throws IslandException {
		throw new IslandException("TEXT island does support Join Operator");
	}
	
}
