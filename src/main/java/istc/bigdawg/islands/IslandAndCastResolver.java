package istc.bigdawg.islands;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.api.ApiHandler;
import istc.bigdawg.islands.api.ApiIsland;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.catalog.Catalog;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.*;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.Myria.MyriaQueryParser;
import istc.bigdawg.islands.SStore.SStoreQueryParser;
import istc.bigdawg.islands.SciDB.ArrayIsland;
import istc.bigdawg.islands.api.ApiQueryParser;
import istc.bigdawg.islands.relational.RelationalIsland;
import istc.bigdawg.islands.text.TextIsland;
import istc.bigdawg.myria.MyriaHandler;
import istc.bigdawg.mysql.MySQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.shims.*;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;
import istc.bigdawg.vertica.VerticaConnectionInfo;

/**
 * @author Jack
 * 
 * This class should contain only static objects and functions. 
 * It is intended to contain all functions that do different things when the island context is different.
 * Island writers should go through each of these functions to add supports for their islands
 * 
 * NOTE: all relational island queries are assumed to be base in PostgreSQL. 
 *       This assumption needs to change as new engines join Relational Island 
 *
 */
public class IslandAndCastResolver {
	
	public static enum Engine {
		PostgreSQL, SciDB, SStore, Accumulo, Myria, MySQL, Vertica, REST
	};
	
	public enum Scope {
		RELATIONAL, ARRAY, KEYVALUE, TEXT, GRAPH, DOCUMENT, STREAM, CAST, MYRIA, API
	}


	public static final int  sstoreDBID = BigDawgConfigProperties.INSTANCE.getSStoreDBID();
	
	public static Pattern QueryParsingPattern	= Pattern.compile("(?i)(bdrel\\(|bdarray\\(|bdkv\\(|bdtext\\(|bdgraph\\(|bddoc\\(|bdstream\\(|bdmyria\\(|bdapi\\(|bdcast\\(|\\(|\\))");
	public static Pattern ScopeStartPattern		= Pattern.compile("^((bdrel\\()|(bdarray\\()|(bdkv\\()|(bdtext\\()|(bdgraph\\()|(bddoc\\()|(bdstream\\()|(bdmyria\\()|(bdapi\\()|(bdcast\\())");
	public static Pattern ScopeEndPattern		= Pattern.compile("\\) *;? *$");
	public static Pattern CastScopePattern		= Pattern.compile("(?i)(relational|array|keyvalue|text|graph|document|stream|myria|api)\\) *;? *$");
	public static Pattern CastSchemaPattern		= Pattern.compile("(?<=([_a-z0-9, ]+')).*(?=(' *, *(relational|array|keyvalue|text|graph|document|stream|myria|api)))");
	public static Pattern CastNamePattern		= Pattern.compile("(?<=(, ))([_@0-9a-zA-Z]+)(?=, *')");

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
		case TEXT:
		case API:
			return true;
		case DOCUMENT:
		case GRAPH:
		case KEYVALUE:
		case STREAM:
		case MYRIA:
			return false;
		default:
			throw new UnsupportedIslandException(scope, "isOperatorBasedIsland");
		}
	}
	
	/**
	 * Used in CatalogViewer
	 * @param engineString
	 * @return
	 * @throws BigDawgException
	 */
	public static IslandAndCastResolver.Engine getEngineEnum(String engineString) throws BigDawgException {
		if (engineString.startsWith(IslandAndCastResolver.Engine.PostgreSQL.name()))
			return IslandAndCastResolver.Engine.PostgreSQL;
		else if (engineString.startsWith(IslandAndCastResolver.Engine.SciDB.name()))
			return IslandAndCastResolver.Engine.SciDB;
		else if (engineString.startsWith(IslandAndCastResolver.Engine.SStore.name()))
			return IslandAndCastResolver.Engine.SStore;
		else if (engineString.startsWith(IslandAndCastResolver.Engine.Accumulo.name()))
			return IslandAndCastResolver.Engine.Accumulo;
		else if (engineString.startsWith(IslandAndCastResolver.Engine.MySQL.name()))
			return IslandAndCastResolver.Engine.MySQL;
		else if (engineString.startsWith(IslandAndCastResolver.Engine.Vertica.name()))
			return IslandAndCastResolver.Engine.Vertica;
		else if (engineString.startsWith(IslandAndCastResolver.Engine.REST.name()))
			return IslandAndCastResolver.Engine.REST;
		else {
			throw new BigDawgException("Unsupported engine: "+ engineString);
		}
	}
	
	/**
	 * Used in CatalogViewer
	 * @param cc
	 * @param e
	 * @param dbid
	 * @return
	 * @throws SQLException
	 * @throws BigDawgCatalogException
	 */
	public static ConnectionInfo getQConnectionInfo(Catalog cc, IslandAndCastResolver.Engine e, int dbid) throws SQLException, BigDawgCatalogException {
		
		ConnectionInfo extraction = null;
		ResultSet rs2 = null;
		
		try {
			switch (e) {
			case PostgreSQL:
				rs2 = cc.execRet("select dbid, eid, host, port, db.name as dbname, userid, password "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
						+ "where dbid = "+dbid);
				if (rs2.next())
					extraction = new PostgreSQLConnectionInfo(rs2.getString("host"), rs2.getString("port"),rs2.getString("dbname"), rs2.getString("userid"), rs2.getString("password"));
				break;
			case Vertica:
				rs2 = cc.execRet("select dbid, eid, host, port, db.name as dbname, userid, password "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
						+ "where dbid = "+dbid);
				if (rs2.next())
					extraction = new VerticaConnectionInfo(rs2.getString("host"), rs2.getString("port"),rs2.getString("dbname"), rs2.getString("userid"), rs2.getString("password"));
				break;
			case MySQL:
				rs2 = cc.execRet("select dbid, eid, host, port, db.name as dbname, userid, password "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
						+ "where dbid = "+dbid);
				if (rs2.next())
					extraction = new MySQLConnectionInfo(rs2.getString("host"), rs2.getString("port"),rs2.getString("dbname"), rs2.getString("userid"), rs2.getString("password"));
				break;
			case SciDB:
				rs2 = cc.execRet("select dbid, db.engine_id, host, port, bin_path, userid, password "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
						+ "join catalog.scidbbinpaths sp on db.engine_id = sp.eid where dbid = "+dbid);
				if (rs2.next())
					extraction = new SciDBConnectionInfo(rs2.getString("host"), rs2.getString("port"), rs2.getString("userid"), rs2.getString("password"), rs2.getString("bin_path"));
					break;
			case SStore:
				rs2 = cc.execRet("select dbid, eid, host, port, db.name as dbname, userid, password "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
						+ "where dbid = "+dbid);
				if (rs2.next())
					extraction = new SStoreSQLConnectionInfo(rs2.getString("host"), rs2.getString("port"),rs2.getString("dbname"), rs2.getString("userid"), rs2.getString("password"));
				break;
			case Accumulo:
				rs2 = cc.execRet("select dbid, eid, host, port, db.name as dbname, userid, password "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
						+ "where dbid = "+dbid);
				if (rs2.next())
					extraction = new AccumuloConnectionInfo(rs2.getString("host"), rs2.getString("port"),rs2.getString("dbname"), rs2.getString("userid"), rs2.getString("password"));
				break;
			case REST:
				rs2 = cc.execRet("select dbid, eid, host, port, db.name as dbname, userid, password, connection_properties, o.name "
						+ "from catalog.databases db "
						+ "join catalog.engines e on db.engine_id = e.eid "
                        + "join catalog.objects o on db.dbid = o.physical_db "
						+ "where dbid = "+dbid);
				if (rs2.next()) {
					extraction = new RESTConnectionInfo(rs2.getString("host"), rs2.getString("port"),rs2.getString("dbname"), rs2.getString("userid"), rs2.getString("password"), rs2.getString("connection_properties"));
				}
				break;
			default:
				throw new BigDawgCatalogException("This is not supposed to happen");
			}
			
			if (extraction == null) {
				rs2.close();
				throw new BigDawgCatalogException("Connection Info Cannot Be Formulated: "+dbid);
			}
				
		} catch (SQLException ex) {
			cc.rollback();
			throw ex;
		} finally {
			if (rs2 != null) {
				rs2.close();
			}
		}
		
		return extraction;
	}
	
	/**
	 * For Executor.
	 * @param scope
	 * @return Instance of a query generator
	 * @throws BigDawgException 
	 * @throws SQLException 
	 */
	public static Shim getShim(Scope scope, int dbid) throws BigDawgException, SQLException {
		
		IslandAndCastResolver.Engine e = CatalogViewer.getEngineOfDB(dbid);
		
		switch (scope) {
		case ARRAY:
			switch (e) {
			case SciDB:
				return new ArrayToSciDBShim();
			default:
				break;
			}
			throw new BigDawgException("getQueryGenerator fails for scope: "+scope.name()+" and dbid: "+dbid);
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			switch (e) {
			case PostgreSQL:
				return new RelationalToPostgresShim();
			case MySQL:
				return new RelationalToMySQLShim();
			case SciDB:
				return new RelationalToSciDBShim();
			case Vertica:
				return new RelationalToVerticaShim();
			default:
				break;
			}
			throw new BigDawgException("getQueryGenerator fails for scope: "+scope.name()+" and dbid: "+dbid);
		case STREAM:
			break;
		case TEXT:
			return new TextToAccumuloShim();
		case MYRIA:
			throw new BigDawgException("MYRIA island does not support the concept of generator; getQueryGenerator");
		case API:
			switch (e) {
				case REST:
					return new ApiToRESTShim();
			}
		default:
			break;
		}
		throw new BigDawgException("getQueryGenerator fails for scope: "+scope.name()+" and dbid: "+dbid);
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
			return "ARRAY";
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return "RELATIONAL";
		case STREAM:
			return "STREAM";
		case TEXT:
			return "TEXT";
		case MYRIA:
			return "MYRIA";
		case API:
			return "API";
		default:
			break;
		}
		throw new UnsupportedIslandException(scope, "getCatalogIslandSelectionPredicate");
	}
	
		
	public static Island getIsland(Scope scope) throws IslandException {
		switch (scope) {
		case ARRAY:
			return ArrayIsland.INSTANCE;
		case CAST:
			break;
		case DOCUMENT:
			break;
		case GRAPH:
			break;
		case KEYVALUE:
			break;
		case RELATIONAL:
			return RelationalIsland.INSTANCE;
		case STREAM:
			break;
		case TEXT:
			return TextIsland.INSTANCE; 
		case MYRIA:
			break;	
		case API:
			return ApiIsland.INSTANCE;
		default:
			break;
		}
		
		throw new UnsupportedIslandException(scope, "getCreationQuery");
	}
	
	
	//// Operator free options
	
	public static QueryResult runOperatorFreeIslandQuery(CrossIslandNonOperatorNode node) throws Exception {
		
		switch (node.sourceScope) {
		
		case DOCUMENT:
		case GRAPH:
		case KEYVALUE:
			throw new UnsupportedIslandException(node.sourceScope, "runOperatorFreeIslandQuery");
		case STREAM:
			List<String> ssparsed = (new SStoreQueryParser()).parse(node.getQueryString());
			return (new SStoreSQLHandler(sstoreDBID)).executePreparedStatement((new SStoreSQLHandler(sstoreDBID)).getConnection(), ssparsed);
// For test purposes:
//		case API:
//			List<String> apiparsed = (new ApiQueryParser()).parse(node.getQueryString());
//			return ApiHandler.executeApiQuery(apiparsed);
		case MYRIA:
			String input = node.getQueryString();
			List<String> mparsed = (new MyriaQueryParser()).parse(input);
			
			System.out.printf("MYRIA Island query: -->%s<--; parsed form: -->%s<--\n", input, mparsed);
			
			return MyriaHandler.executeMyriaQuery(mparsed);
		case API:
		case RELATIONAL:
		case ARRAY:
		case TEXT:
		default:
			throw new IslandException("Unapplicable island for runOperatorFreeIslandQuery: "+node.sourceScope.name());
		}
		
	}

	
	public static Scope convertFunctionScope (String prefix) throws UnsupportedIslandException {
		switch (prefix) {
		case "bdrel(":
		case "bdrel":
			return Scope.RELATIONAL;
		case "bdarray(":
		case "bdarray":
			return Scope.ARRAY;
		case "bdkv(":
		case "bdkv":
			return Scope.KEYVALUE;
		case "bdtext(":
		case "bdtext":
			return Scope.TEXT;
		case "bdgraph(":
		case "bdgraph":
			return Scope.GRAPH;
		case "bddoc(":
		case "bddoc":
			return Scope.DOCUMENT;
		case "bdstream(":
		case "bdstream":
			return Scope.STREAM;
		case "bdcast(":
		case "bdcast":
			return Scope.CAST;
		case "bdmyria(":
		case "bdmyria":
			return Scope.MYRIA;
		case "bdapi(":
		case "bdapi":
			return Scope.API;
		default:
			throw new UnsupportedIslandException(prefix);
		}
		
	}
	
	public static Scope convertDestinationScope (String prefix) throws UnsupportedIslandException {
		switch (prefix.toLowerCase()) {
		case "relational":
			return Scope.RELATIONAL;
		case "array":
			return Scope.ARRAY;
		case "keyvalue":
			return Scope.KEYVALUE;
		case "text":
			return Scope.TEXT;
		case "graph":
			return Scope.GRAPH;
		case "document":
			return Scope.DOCUMENT;
		case "stream":
			return Scope.STREAM;
		case "cast":
			return Scope.CAST;
		case "myria":
			return Scope.MYRIA;
		case "api":
			return Scope.API;
		default:
			throw new UnsupportedIslandException(prefix);
		}
		
	}
}