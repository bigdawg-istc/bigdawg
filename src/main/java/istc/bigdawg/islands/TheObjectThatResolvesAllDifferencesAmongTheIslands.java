package istc.bigdawg.islands;

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

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.UnsupportedIslandException;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.PostgreSQL.SQLPlanParser;
import istc.bigdawg.islands.PostgreSQL.SQLQueryGenerator;
import istc.bigdawg.islands.PostgreSQL.SQLQueryPlan;
import istc.bigdawg.islands.PostgreSQL.operators.PostgreSQLIslandJoin;
import istc.bigdawg.islands.SciDB.AFLPlanParser;
import istc.bigdawg.islands.SciDB.AFLQueryGenerator;
import istc.bigdawg.islands.SciDB.AFLQueryPlan;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandJoin;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.signature.builder.ArraySignatureBuilder;
import istc.bigdawg.signature.builder.RelationalSignatureBuilder;
import net.sf.jsqlparser.JSQLParserException;

/**
 * @author Jack
 * 
 * This class should contain only static objects and functions. 
 * It is intended to contain all functions that do different things when the island context is different.
 * Island writers should go through each of these functions to add supports for their islands
 *
 */
public class TheObjectThatResolvesAllDifferencesAmongTheIslands {
	
	public static final int  psqlSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	public static final int  scidbSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getSciDBSchemaServerDBID();
	
	private static final Pattern predicatePattern = Pattern.compile("(?<=\\()([^\\(^\\)]+)(?=\\))");
	
	/**
	 * For Executor.
	 * @param scope
	 * @return Instance of a query generator
	 * @throws UnsupportedIslandException
	 */
	public static OperatorVisitor getQueryGenerator(Scope scope) throws UnsupportedIslandException {
		if (scope.equals(Scope.RELATIONAL)) return new SQLQueryGenerator();
		else if (scope.equals(Scope.ARRAY)) return new AFLQueryGenerator();
		else throw new UnsupportedIslandException(scope, "getQueryGenerator");
	}
	
	/**
	 * For Catalog
	 * @param scope
	 * @return A string in the form of " AND scope_name = 'RELATIONAL' ", where RELATIONAL is replaced by the island reference in the Catalog 
	 * @throws UnsupportedIslandException
	 */
	public static String getCatalogIslandSelectionPredicate(Scope scope) throws UnsupportedIslandException {
		if (scope.equals(IslandsAndCast.Scope.RELATIONAL)) return " AND scope_name = \'RELATIONAL\' ";
		else if (scope.equals(IslandsAndCast.Scope.ARRAY)) return " AND scope_name = \'ARRAY\' ";
		else throw new UnsupportedIslandException(scope, "getCatalogIslandSelectionPredicate");
	}
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @param sourceScope
	 * @param children
	 * @param transitionSchemas
	 * @return
	 * @throws UnsupportedIslandException
	 * @throws BigDawgCatalogException
	 * @throws SQLException
	 */
	public static DBHandler createTableForPlanning(Scope sourceScope, Set<String> children, Map<String, String> transitionSchemas) throws UnsupportedIslandException, BigDawgCatalogException, SQLException {

		DBHandler dbSchemaHandler;
		if (sourceScope.equals(Scope.RELATIONAL)) {
			dbSchemaHandler = new PostgreSQLHandler((PostgreSQLConnectionInfo)CatalogViewer.getConnectionInfo(psqlSchemaHandlerDBID));
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) ((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL(transitionSchemas.get(key));
		} else if (sourceScope.equals(Scope.ARRAY)) {
			dbSchemaHandler = new SciDBHandler((SciDBConnectionInfo)CatalogViewer.getConnectionInfo(scidbSchemaHandlerDBID));
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((SciDBHandler)dbSchemaHandler).executeStatement(transitionSchemas.get(key));
					((SciDBHandler)dbSchemaHandler).commit();
				}
		} else throw new UnsupportedIslandException(sourceScope, "createTableForPlanning");
		
		return dbSchemaHandler;
	}
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @param sourceScope
	 * @param dbSchemaHandler
	 * @param children
	 * @param transitionSchemas
	 * @throws BigDawgCatalogException
	 * @throws SQLException
	 * @throws UnsupportedIslandException
	 */
	public static void removeTemporaryTableCreatedForPlanning(Scope sourceScope, DBHandler dbSchemaHandler, Set<String> children, Map<String, String> transitionSchemas) throws BigDawgCatalogException, SQLException, UnsupportedIslandException {
		if (sourceScope.equals(Scope.RELATIONAL)) {
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) 
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL("drop table "+key);
		} else if (sourceScope.equals(Scope.ARRAY)) {
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((SciDBHandler)dbSchemaHandler).executeStatementAFL("remove("+key+")");
					((SciDBHandler)dbSchemaHandler).commit();
				}
		} else throw new UnsupportedIslandException(sourceScope, "removeTemporaryTableCreatedForPlanning");
	}
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @param scope
	 * @return
	 * @throws UnsupportedIslandException
	 */
	public static Integer getSchemaEngineDBID(Scope scope) throws UnsupportedIslandException {
		if (scope.equals(Scope.RELATIONAL)) return psqlSchemaHandlerDBID;
		else if (scope.equals(Scope.ARRAY)) return scidbSchemaHandlerDBID;
		else throw new UnsupportedIslandException(scope, "getSchemaEngineDBID");
	}
	
	/**
	 * For query planning, CrossIslandQueryNode 
	 * @param scope
	 * @param o1, left operator
	 * @param o2, right operator
	 * @param jt, type of join
	 * @param joinPred, join predicate, could be null
	 * @param isFilter, whether it originally present as a joinFilter and not joinPredicate (distinction unclear)
	 * @return
	 * @throws JSQLParserException 
	 * @throws BigDawgException 
	 * @throws Exception
	 */
	public static Join constructJoin (Scope scope, Operator o1, Operator o2, JoinType jt, List<String> joinPred, boolean isFilter) throws JSQLParserException, BigDawgException {
		if (scope.equals(Scope.RELATIONAL)) {
			if (joinPred == null) return new PostgreSQLIslandJoin(o1, o2, jt, null, isFilter);
			return new PostgreSQLIslandJoin(o1, o2, jt, String.join(" AND ", joinPred), isFilter);
		} else if (scope.equals(Scope.ARRAY)) {
			if (joinPred == null) return new SciDBIslandJoin().construct(o1, o2, jt, null, isFilter);
			return new SciDBIslandJoin().construct(o1, o2, jt, String.join(", ", joinPred.stream().map(s -> s.replaceAll("[<>=]+", ", ")).collect(Collectors.toSet())), isFilter);
		} else 
			throw new UnsupportedIslandException(scope, "constructJoin");
	}
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @param scope
	 * @param dbSchemaHandler
	 * @param queryString
	 * @param objs
	 * @return
	 * @throws Exception
	 */
	public static Operator generateOperatorTreesAndAddDataSetObjectsSignature(Scope scope, DBHandler dbSchemaHandler, String queryString, List<String> objs) throws Exception {
		Operator root;
		if (scope.equals(Scope.RELATIONAL)) {
			SQLQueryPlan queryPlan = SQLPlanParser.extractDirect((PostgreSQLHandler)dbSchemaHandler, queryString);
			root = queryPlan.getRootNode();
			objs.addAll(RelationalSignatureBuilder.sig2(queryString));
		} else if (scope.equals(Scope.ARRAY)) {
			AFLQueryPlan queryPlan = AFLPlanParser.extractDirect((SciDBHandler)dbSchemaHandler, queryString);
			root = queryPlan.getRootNode();
			objs.addAll(ArraySignatureBuilder.sig2(queryString));
		} else 
			throw new UnsupportedIslandException(scope, "generateOperatorTrees");
		return root;
	} 
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @return
	 */
	public static Set<String> splitPredicates(Scope scope, String predicates) {
		Set<String> results = new HashSet<>();

		String joinDelim = "";
		if (scope.equals(Scope.RELATIONAL)){
			joinDelim = "[=<>]+";
		} else if (scope.equals(Scope.ARRAY)){
			// TODO ensure this is correct for SciDB
			joinDelim = "[,]";
		}

		Matcher m = predicatePattern.matcher(predicates);
		while (m.find()){
			String current = m.group().replace(" ", "");
			String[] filters = current.split(joinDelim);
			Arrays.sort(filters);
			String result = String.join(joinDelim, filters);
			results.add(result);
		}
		return results;
	}
	
	/**
	 * For monitoring, signature
	 * @param scope
	 * @param query
	 * @return
	 * @throws IOException
	 * @throws UnsupportedIslandException
	 */
	public static List<String> getLiteralsAndConstantsSignature(Scope scope, String query) throws IOException, UnsupportedIslandException {
		if (scope.equals(Scope.RELATIONAL)){
			return RelationalSignatureBuilder.sig3(query);
		} else if (scope.equals(Scope.ARRAY)) {
			return ArraySignatureBuilder.sig3(query);
		} else throw new UnsupportedIslandException(scope, "getDataSetObjectsSignature");
	}
	
	/**
	 * For monitoring, signature
	 * @param scope
	 * @param query
	 * @return
	 * @throws UnsupportedIslandException
	 */
	public static String getIslandStyleQuery(Scope scope, String query) throws UnsupportedIslandException {
		if (scope == Scope.RELATIONAL){
			return String.format("bdrel(%s);", query);
		} else if (scope == Scope.ARRAY){
			return String.format("bdarray(%s);", query);
		} else throw new UnsupportedIslandException(scope, "getIslandStyleQuery");
	}
	
	/**
	 * For CrossIslandCastNode
	 * @param scope
	 * @param name
	 * @param schemaCreationQuery
	 * @return
	 * @throws UnsupportedIslandException
	 */
	public static String getCreationQueryForCast(Scope scope, String name, String schemaCreationQuery) throws UnsupportedIslandException {
		if (scope.equals(Scope.ARRAY)) return String.format("CREATE ARRAY %s %s", name, schemaCreationQuery);
		else if (scope.equals(Scope.RELATIONAL)) return String.format("CREATE TABLE %s %s", name, schemaCreationQuery);
		else throw new UnsupportedIslandException(scope, "getCreationQuery");
	}
}
