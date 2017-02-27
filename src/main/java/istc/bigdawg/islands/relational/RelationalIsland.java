package istc.bigdawg.islands.relational;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.Island;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.operators.SQLIslandJoin;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import net.sf.jsqlparser.JSQLParserException;

public class RelationalIsland implements Island {

	private static final int psqlSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	private static PostgreSQLConnectionInfo relationalSchemaServerConnectionInfo = null;
	private static final Pattern relationalIslandPredicatePattern = Pattern.compile("(?<=\\()([^\\(^\\)]+)(?=\\))");
	
	static {
		try {
			relationalSchemaServerConnectionInfo = 
				(PostgreSQLConnectionInfo)CatalogViewer.getConnectionInfo(psqlSchemaServerDBID);
		} catch (SQLException | BigDawgCatalogException e) {
			e.printStackTrace();
		}
	}
	
	public RelationalIsland() {
		
	}
	
	@Override
	public void setupForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas) throws IslandException {

		PostgreSQLHandler handler = new PostgreSQLHandler(relationalSchemaServerConnectionInfo);
		Set<String> createdTables = new HashSet<>();
		
		for (String key : outputTransitionSchemas.keySet()) {
			if (outputChildren.contains(key)) {
				try {
					createdTables.add(key);
					handler.executeStatementPostgreSQL(outputTransitionSchemas.get(key));
				} catch (SQLException e) {
					try {
						for (String s : createdTables) {
							handler.dropDataSetIfExists(s);
						}
					} catch (SQLException e1) {
						throw new IslandException(e1.getMessage(), e1);
					}
					throw new IslandException(e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public void teardownForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas) throws IslandException {

		PostgreSQLHandler handler = new PostgreSQLHandler(relationalSchemaServerConnectionInfo);
		
		try {
			for (String key : outputTransitionSchemas.keySet()) 
				if (outputChildren.contains(key)) 
					handler.dropDataSetIfExists(key);
		} catch (SQLException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public String getCreateStatementForTransitionTable(String tableName, String attributes) {
		return String.format("CREATE TABLE %s %s", tableName, attributes);
	}
	
	@Override
	public int addCatalogObjectEntryForTemporaryTable(String tableName) throws IslandException {
		try {
			return CatalogModifier.addObject(tableName, "TEMPORARY", psqlSchemaServerDBID, psqlSchemaServerDBID);
		} catch (SQLException | BigDawgCatalogException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}
	
	@Override
	public Operator parseQueryAndExtractAllTableNames(String queryString, List<String> tables) throws IslandException {
		SQLQueryPlan relQueryPlan;
		Operator root;
		try {
			relQueryPlan = SQLPlanParser.extractDirectFromPostgreSQL(new PostgreSQLHandler(relationalSchemaServerConnectionInfo), queryString);
			root = relQueryPlan.getRootNode();
			tables.addAll(RelationalSignatureBuilder.sig2(queryString));
			return root;
		} catch (BigDawgCatalogException | SQLException | IOException | JSQLParserException e) {
			throw new IslandException(e.getMessage(), e);
		}
	};
	
	@Override
	public Set<String> splitJoinPredicate(String predicates) throws IslandException {
		Set<String> results = new HashSet<>();
		String joinDelim = "[=<>]+";
		
		Matcher m = relationalIslandPredicatePattern.matcher(predicates);
		while (m.find()){
			String current = m.group().replace(" ", "");
			String[] filters = current.split(joinDelim);
			Arrays.sort(filters);
			String result = String.join(joinDelim, filters);
			results.add(result);
		}
		return results;
	};
	
	@Override
	public Integer getDefaultCastReceptionDBID() {
		return psqlSchemaServerDBID;
	}
	
	@Override
	public List<String> getLiteralsAndConstantsSignature(String query) throws IslandException {
		try {
			return RelationalSignatureBuilder.sig3(query);
		} catch (IOException e) {
			throw new IslandException(e.getMessage(), e);
		}
	};
	
	@Override
	public String wrapQueryInIslandIdentifier(String query) {
		return String.format("bdrel(%s);", query);
	};

	@Override
	public Join constructJoin(Operator o1, Operator o2, JoinType jt, List<String> joinPred, boolean isFilter) throws IslandException {
		try {
			if (joinPred == null) return new SQLIslandJoin(o1, o2, jt, null, isFilter);
			return new SQLIslandJoin(o1, o2, jt, String.join(" AND ", joinPred), isFilter);
		} catch (JSQLParserException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	public String getCreateTableString(String tableName) throws SQLException {
		Connection con = PostgreSQLHandler.getConnection(relationalSchemaServerConnectionInfo);
		return PostgreSQLHandler.getCreateTable(con, tableName).replaceAll("\\scharacter[\\(]", " char(");
	}
}
