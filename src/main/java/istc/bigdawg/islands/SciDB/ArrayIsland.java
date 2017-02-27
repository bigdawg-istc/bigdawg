package istc.bigdawg.islands.SciDB;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import istc.bigdawg.catalog.CatalogModifier;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.IslandException;
import istc.bigdawg.islands.Island;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandJoin;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import net.sf.jsqlparser.JSQLParserException;

public class ArrayIsland implements Island {

	private static final int scidbSchemaServerDBID = BigDawgConfigProperties.INSTANCE.getSciDBSchemaServerDBID();
	private static SciDBConnectionInfo arraySchemaServerConnectionInfo = null;
	private static final Pattern arrayIslandPredicatePattern = Pattern.compile("(?<=\\()([^\\(^\\)]+)(?=\\))");
	
	static {
		try {
			arraySchemaServerConnectionInfo = 
				(SciDBConnectionInfo)CatalogViewer.getConnectionInfo(scidbSchemaServerDBID);
		} catch (SQLException | BigDawgCatalogException e) {
			e.printStackTrace();
		}
	}
	
	public ArrayIsland() {
		
	}
	
	@Override
	public void setupForQueryPlanning(Set<String> outputChildren, Map<String, String> outputTransitionSchemas)
			throws IslandException {

		SciDBHandler handler;
		Set<String> createdTables = new HashSet<>();
		
		for (String key : outputTransitionSchemas.keySet()) 
			if (outputChildren.contains(key)) {
				try {
					createdTables.add(key);
					handler = new SciDBHandler(arraySchemaServerConnectionInfo);
					((SciDBHandler)handler).executeStatementAQL(outputTransitionSchemas.get(key));
				} catch (SQLException e) {
					for (String s : createdTables) {
						try {
							SciDBHandler.dropArrayIfExists(arraySchemaServerConnectionInfo, s);
						} catch (SQLException e1) {
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
			for (String key : outputTransitionSchemas.keySet()) 
				if (outputChildren.contains(key)) 
					SciDBHandler.dropArrayIfExists(arraySchemaServerConnectionInfo, key);
		} catch (SQLException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public String getCreateStatementForTransitionTable(String tableName, String attributes) {
		return String.format("CREATE ARRAY %s %s", tableName, attributes);
	}

	@Override
	public int addCatalogObjectEntryForTemporaryTable(String tableName) throws IslandException {
		try {
			return CatalogModifier.addObject(tableName, "TEMPORARY", scidbSchemaServerDBID, scidbSchemaServerDBID);
		} catch (SQLException | BigDawgCatalogException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public Operator parseQueryAndExtractAllTableNames(String queryString, List<String> tables) throws IslandException {
		try {
			AFLQueryPlan arrayQueryPlan = AFLPlanParser.extractDirect(new SciDBHandler(arraySchemaServerConnectionInfo), queryString);
			Operator root = arrayQueryPlan.getRootNode();
			tables.addAll(ArraySignatureBuilder.sig2(queryString));
			return root;
		} catch (BigDawgCatalogException | SQLException | IOException | JSQLParserException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public Set<String> splitJoinPredicate(String predicates) throws IslandException {
		Set<String> results = new HashSet<>();
		String joinDelim = "[,=]";
		Matcher m = arrayIslandPredicatePattern.matcher(predicates);
		while (m.find()){
			String current = m.group().replace(" ", "");
			String[] filters = current.split(joinDelim);
			Arrays.sort(filters);
			String result = String.join(joinDelim, filters);
			results.add(result);
		}
		return results;
	}

	@Override
	public Integer getDefaultCastReceptionDBID() {
		return scidbSchemaServerDBID;
	}

	@Override
	public List<String> getLiteralsAndConstantsSignature(String query) throws IslandException {
		try {
			return ArraySignatureBuilder.sig3(query);
		} catch (IOException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

	@Override
	public String wrapQueryInIslandIdentifier(String query) {
		return String.format("bdarray(%s);", query);
	}

	@Override
	public Join constructJoin(Operator o1, Operator o2, JoinType jt, List<String> joinPred, boolean isFilter)
			throws IslandException {
		try {
			if (joinPred == null) return new SciDBIslandJoin().construct(o1, o2, jt, null, isFilter);
			return new SciDBIslandJoin().construct(o1, o2, jt, String.join(", ", joinPred.stream().map(s -> s.replaceAll("[<>=]+", ", ")).collect(Collectors.toSet())), isFilter);
		} catch (JSQLParserException e) {
			throw new IslandException(e.getMessage(), e);
		}
	}

}
