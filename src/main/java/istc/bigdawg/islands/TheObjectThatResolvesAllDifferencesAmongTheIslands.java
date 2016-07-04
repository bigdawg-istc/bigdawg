package istc.bigdawg.islands;

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
import java.util.stream.Collectors;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.UnsupportedIslandException;
import istc.bigdawg.islands.IslandsAndCast.Scope;
import istc.bigdawg.islands.SciDB.AFLPlanParser;
import istc.bigdawg.islands.SciDB.AFLQueryGenerator;
import istc.bigdawg.islands.SciDB.AFLQueryPlan;
import istc.bigdawg.islands.SciDB.operators.SciDBIslandJoin;
import istc.bigdawg.islands.operators.Join;
import istc.bigdawg.islands.operators.Join.JoinType;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.relational.SQLPlanParser;
import istc.bigdawg.islands.relational.SQLQueryGenerator;
import istc.bigdawg.islands.relational.SQLQueryPlan;
import istc.bigdawg.islands.relational.operators.SQLIslandJoin;
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
 * NOTE: all relational island queries are assumed to be base in PostgreSQL. 
 *
 */
public class TheObjectThatResolvesAllDifferencesAmongTheIslands {
	
	public static final int  psqlSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getPostgresSchemaServerDBID();
	public static final int  scidbSchemaHandlerDBID = BigDawgConfigProperties.INSTANCE.getSciDBSchemaServerDBID();
	
	private static final Pattern predicatePattern = Pattern.compile("(?<=\\()([^\\(^\\)]+)(?=\\))");
	
	/**
	 * For CrossIslandQueryPlan
	 * Determines whether an island is implemented
	 * @param scope
	 * @return
	 */
	public static boolean isOperatorBasedIsland(Scope scope) throws UnsupportedIslandException {
		switch (scope) {
		case ARRAY:
		case RELATIONAL:
			return true;
		case DOCUMENT:
		case GRAPH:
		case KEYVALUE:
		case STREAM:
		case TEXT:
			return false;
		default:
			throw new UnsupportedIslandException(scope, "isOperatorBasedIsland");
		}
	}
	
	/**
	 * For Executor.
	 * @param scope
	 * @return Instance of a query generator
	 * @throws UnsupportedIslandException
	 */
	public static OperatorVisitor getQueryGenerator(Scope scope) throws UnsupportedIslandException {
		switch (scope) {
		case ARRAY:
			return new AFLQueryGenerator();
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return new SQLQueryGenerator();
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		throw new UnsupportedIslandException(scope, "getQueryGenerator");
	}
	
	/**
	 * For Catalog
	 * @param scope
	 * @return A string in the form of " AND scope_name = 'RELATIONAL' ", where RELATIONAL is replaced by the island reference in the Catalog 
	 * @throws UnsupportedIslandException
	 */
	public static String getCatalogIslandSelectionPredicate(Scope scope) throws UnsupportedIslandException {
		switch (scope) {
		case ARRAY:
			return " AND scope_name = \'ARRAY\' ";
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return " AND scope_name = \'RELATIONAL\' ";
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		throw new UnsupportedIslandException(scope, "getCatalogIslandSelectionPredicate");
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

		DBHandler dbSchemaHandler = null;
		
		switch (sourceScope) {
		case ARRAY:
			dbSchemaHandler = new SciDBHandler((SciDBConnectionInfo)CatalogViewer.getConnectionInfo(scidbSchemaHandlerDBID));
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((SciDBHandler)dbSchemaHandler).executeStatement(transitionSchemas.get(key));
					((SciDBHandler)dbSchemaHandler).commit();
				}
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			dbSchemaHandler = new PostgreSQLHandler((PostgreSQLConnectionInfo)CatalogViewer.getConnectionInfo(psqlSchemaHandlerDBID));
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) ((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL(transitionSchemas.get(key));
			break;
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		if (dbSchemaHandler == null) throw new UnsupportedIslandException(sourceScope, "createTableForPlanning");
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
		
		switch (sourceScope) {
		case ARRAY:
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) {
					((SciDBHandler)dbSchemaHandler).executeStatementAFL("remove("+key+")");
					((SciDBHandler)dbSchemaHandler).commit();
				}
			return;
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			for (String key : transitionSchemas.keySet()) 
				if (children.contains(key)) 
					((PostgreSQLHandler)dbSchemaHandler).executeStatementPostgreSQL("drop table "+key);
			return;
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		
		throw new UnsupportedIslandException(sourceScope, "removeTemporaryTableCreatedForPlanning");
	}
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @param scope
	 * @return
	 * @throws UnsupportedIslandException
	 */
	public static Integer getSchemaEngineDBID(Scope scope) throws UnsupportedIslandException {
		
		switch (scope) {
		case ARRAY:
			return scidbSchemaHandlerDBID;
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return psqlSchemaHandlerDBID;
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		
		throw new UnsupportedIslandException(scope, "getSchemaEngineDBID");
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
		
		switch (scope) {
		case ARRAY:
			if (joinPred == null) return new SciDBIslandJoin().construct(o1, o2, jt, null, isFilter);
			return new SciDBIslandJoin().construct(o1, o2, jt, String.join(", ", joinPred.stream().map(s -> s.replaceAll("[<>=]+", ", ")).collect(Collectors.toSet())), isFilter);
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			if (joinPred == null) return new SQLIslandJoin(o1, o2, jt, null, isFilter);
			return new SQLIslandJoin(o1, o2, jt, String.join(" AND ", joinPred), isFilter);
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		
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
		
		Operator root = null;
		
		switch (scope) {
		case ARRAY:
			AFLQueryPlan arrayQueryPlan = AFLPlanParser.extractDirect((SciDBHandler)dbSchemaHandler, queryString);
			root = arrayQueryPlan.getRootNode();
			objs.addAll(ArraySignatureBuilder.sig2(queryString));
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			SQLQueryPlan relQueryPlan = SQLPlanParser.extractDirectFromPostgreSQL((PostgreSQLHandler)dbSchemaHandler, queryString);
			root = relQueryPlan.getRootNode();
			objs.addAll(RelationalSignatureBuilder.sig2(queryString));
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		
		if (root == null) throw new UnsupportedIslandException(scope, "generateOperatorTrees");
		return root;
	} 
	
	/**
	 * For query planning, CrossIslandQueryNode
	 * @return
	 * @throws UnsupportedIslandException 
	 */
	public static Set<String> splitPredicates(Scope scope, String predicates) throws UnsupportedIslandException {
		Set<String> results = new HashSet<>();

		String joinDelim = null;
		
		switch (scope) {
		case ARRAY:
			// TODO ensure this is correct for SciDB
			joinDelim = "[,]";
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			joinDelim = "[=<>]+";
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		
		if (joinDelim == null) throw new UnsupportedIslandException(scope, "splitPredicates");

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
		
		switch (scope) {
		case ARRAY:
			return ArraySignatureBuilder.sig3(query);
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return RelationalSignatureBuilder.sig3(query);
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}

		throw new UnsupportedIslandException(scope, "getDataSetObjectsSignature");
	}
	
	/**
	 * For monitoring, signature
	 * @param scope
	 * @param query
	 * @return
	 * @throws UnsupportedIslandException
	 */
	public static String getIslandStyleQuery(Scope scope, String query) throws UnsupportedIslandException {
		
		switch (scope) {
		case ARRAY:
			return String.format("bdarray(%s);", query);
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return String.format("bdrel(%s);", query);
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
			
		throw new UnsupportedIslandException(scope, "getIslandStyleQuery");
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
		
		switch (scope) {
		case ARRAY:
			return String.format("CREATE ARRAY %s %s", name, schemaCreationQuery);
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return String.format("CREATE TABLE %s %s", name, schemaCreationQuery);
		case STREAM:
			break;
		case TEXT:
			break;
		default:
			break;
		}
		
		throw new UnsupportedIslandException(scope, "getCreationQuery");
	}
	
	
	//// island specific
	
	/**
	 * For Relational Island only.
	 * @param e
	 * @param tableName
	 * @return
	 * @throws SQLException
	 * @throws BigDawgException
	 */
	public static String getRelationalIslandCreateTableString(String tableName) throws SQLException, BigDawgException  {
		int dbid;

		if (tableName.toLowerCase().startsWith("bigdawgtag_")) dbid = psqlSchemaHandlerDBID;
		else dbid = CatalogViewer.getDbsOfObject(tableName, "postgres").get(0);
		
		Connection con = PostgreSQLHandler.getConnection((PostgreSQLConnectionInfo)CatalogViewer.getConnectionInfo(dbid));
		return PostgreSQLHandler.getCreateTable(con, tableName).replaceAll("\\scharacter[\\(]", " char(");
	}
}
