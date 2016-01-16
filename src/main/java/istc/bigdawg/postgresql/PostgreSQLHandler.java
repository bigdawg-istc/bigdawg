/**
 * 
 */
package istc.bigdawg.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;

/**
 * @author Adam Dziedzic
 * 
 */
public class PostgreSQLHandler implements DBHandler {

	private static Logger log = Logger.getLogger(PostgreSQLHandler.class.getName());
	private Connection con = null;
	private PostgreSQLConnectionInfo conInfo = null;
	private Statement st = null;
	private PreparedStatement preparedSt = null;
	private ResultSet rs = null;

	public PostgreSQLHandler(int engineId, int dbId) throws Exception {
		try {
			this.conInfo = CatalogViewer.getConnection(engineId, dbId);
		} catch (Exception e) {
			String msg = "Catalog chosen connection: " + conInfo.getHost() + " " + conInfo.getPort() + " "
					+ conInfo.getDatabase() + " " + conInfo.getUser() + " " + conInfo.getPassword() + ".";
			log.error(msg);
			e.printStackTrace();
			throw e;
		}
	}

	public PostgreSQLHandler(PostgreSQLConnectionInfo conInfo) {
		this.conInfo = conInfo;
	}

	public PostgreSQLHandler() {
		String msg = "Default handler. PostgreSQL parameters from a file.";
		log.info(msg);
	}

	/**
	 * Establish connection to PostgreSQL for this instance.
	 * 
	 * @throws SQLException
	 *             if could not establish a connection
	 */
	private Connection getConnection() throws SQLException {
		if (con == null) {
			if (conInfo != null) {
				try {
					con = getConnection(conInfo);
				} catch (SQLException e) {
					e.printStackTrace();
					log.error(e.getMessage() + " Could not connect to PostgreSQL database using: " + conInfo.toString(),
							e);
					throw e;
				}
			} else {
				con = PostgreSQLInstance.getConnection();
			}
		}
		return con;
	}

	public static Connection getConnection(PostgreSQLConnectionInfo conInfo) throws SQLException {
		Connection con;
		String url = conInfo.getUrl();
		String user = conInfo.getUser();
		String password = conInfo.getPassword();
		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			String msg = "Could not connect to the PostgreSQL instance: Url: " + url + " User: " + user + " Password: "
					+ password;
			log.error(msg);
			e.printStackTrace();
			throw e;
		}
		return con;
	}

	public class QueryResult {
		private List<List<String>> rows;
		private List<String> types;
		private List<String> colNames;

		/**
		 * @return the rows
		 */
		public List<List<String>> getRows() {
			return rows;
		}

		/**
		 * @return the types
		 */
		public List<String> getTypes() {
			return types;
		}

		/**
		 * @return the colNames
		 */
		public List<String> getColNames() {
			return colNames;
		}

		/**
		 * @param rows
		 * @param types
		 * @param colNames
		 */
		public QueryResult(List<List<String>> rows, List<String> types, List<String> colNames) {
			super();
			this.rows = rows;
			this.types = types;
			this.colNames = colNames;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		long lStartTime = System.nanoTime();
		QueryResult queryResult = null;
		try {
			queryResult = executeQueryPostgreSQL(queryString);
		} catch (SQLException e) {
			return Response.status(500)
					.entity("Problem with query execution in Postgresql: " + e.getMessage() + "; query: " + queryString)
					.build();
			// return "Problem with query execution in PostgreSQL: " +
			// queryString;
		}
		String messageQuery = "PostgreSQL query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageQuery);
		log.info(messageQuery);

		/*
		 * QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
		 * queryResult.getRows(), 1, 1, queryResult.getColNames(),
		 * queryResult.getTypes(), new Timestamp(0));
		 * 
		 * lStartTime = System.nanoTime(); String jsonResult =
		 * getJSONString(resp); String messageJSON=
		 * "format JSON Java time milliseconds: " + (System.nanoTime() -
		 * lStartTime) / 1000000 + ","; System.out.print(messageJSON);
		 * log.info(messageJSON); return
		 * Response.status(200).entity(jsonResult).build();
		 */

		lStartTime = System.nanoTime();

		String out = "";
		for (String name : queryResult.getColNames()) {
			out = out + "\t" + name;
		}
		out = out + "\n";
		Integer rowCounter = 1;
		for (List<String> row : queryResult.getRows()) {
			out = out + rowCounter.toString() + ".";
			for (String s : row) {
				out = out + "\t" + s;
			}
			out = out + "\n";
			rowCounter += 1;
		}

		String messageTABLE = "format TABLE Java time milliseconds: " + (System.nanoTime() - lStartTime) / 1000000
				+ ",";
		System.out.print(messageTABLE);
		log.info(messageTABLE);

		return Response.status(200).entity(out).build();
	}

	/**
	 * Clean resource after a query/statement was executed in PostgreSQL.
	 * 
	 * @throws SQLException
	 */
	private void cleanPostgreSQLResources() throws SQLException {
		if (rs != null) {
			rs.close();
			rs = null;
		}
		if (st != null) {
			st.close();
			st = null;
		}
		if (preparedSt != null) {
			preparedSt.close();
			preparedSt = null;
		}
		if (con != null) {
			con.close();
			con = null;
		}
	}

	/**
	 * Executes a statement (not a query) in PostgreSQL. It cleans the resources
	 * at the end.
	 * 
	 * @param statement
	 *            to be executed
	 * @throws SQLException
	 */
	public void executeStatementPostgreSQL(String statement) throws SQLException {
		try {
			getConnection();
			con.createStatement().execute(statement);
		} catch (SQLException ex) {
			ex.printStackTrace();
			// remove ' from the statement - otherwise it won't be inserted into
			// log table in Postgres
			log.error(ex.getMessage() + "; statement to be executed: " + statement.replace("'", ""), ex);
			throw ex;
		} finally {
			try {
				cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.info(ex.getMessage() + "; statement: " + statement.replace("'", ""), ex);
				throw ex;
			}
		}
	}

	/**
	 * It executes the query and releases the resources at the end.
	 * 
	 * @param query
	 * @return #QueryResult
	 * @throws SQLException
	 */
	public QueryResult executeQueryPostgreSQL(final String query) throws SQLException {
		try {
			this.getConnection();

			log.debug("\n\nquery: " + query + "");
			log.debug("ConnectionInfo: " + this.conInfo.toString() + "\n");

			st = con.createStatement();
			rs = st.executeQuery(query);

			ResultSetMetaData rsmd = rs.getMetaData();
			List<String> colNames = getColumnNames(rsmd);
			List<String> types = getColumnTypes(rsmd);
			List<List<String>> rows = getRows(rs);
			return new QueryResult(rows, types, colNames);
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			ex.printStackTrace();
			lgr.log(Level.ERROR, ex.getMessage() + "; query: " + query, ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				ex.printStackTrace();
				lgr.log(Level.INFO, ex.getMessage() + "; query: " + query, ex);
				throw ex;
			}
		}
	}

	/**
	 * NEW FUNCTION: this is written specifically for generating XML query
	 * plans, which is used to construct Operator tree, which will be used for
	 * determining equivalences.
	 * 
	 * @param query
	 * @return the String of XML execution tree plan generated by PSQL
	 * @throws SQLException
	 */
	public String generatePostgreSQLQueryXML(final String query) throws SQLException {
		try {
			this.getConnection();
			st = con.createStatement();
			// st.executeUpdate("set search_path to schemas; ");
			ResultSet rs = st.executeQuery(query);
			if (rs.next()) {
				return rs.getString(1);
			} else {
				throw new SQLException("No result is returned.");
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			ex.printStackTrace();
			lgr.log(Level.ERROR, ex.getMessage() + "; query: " + query, ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				ex.printStackTrace();
				lgr.log(Level.INFO, ex.getMessage() + "; query: " + query, ex);
				throw ex;
			}
		}
	}

	/**
	 * NEW FUNCTION: this is written for generating the dummy tables in PSQL,
	 * which is also used to construct Operator tree, which will be used for
	 * determining equivalences.
	 * 
	 * @param query
	 * @return the String of XML execution tree plan generated by PSQL
	 * @throws SQLException
	 */
	public void populateSchemasSchema(final String query, boolean drop) throws SQLException {
		try {
			this.getConnection();
			st = con.createStatement();
			if (drop) {
				st.executeUpdate("drop schema schemas cascade; ");
				st.executeUpdate("create schema schemas; ");
			}
			st.executeUpdate("set search_path to schemas; ");
			st.executeUpdate(query);
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			ex.printStackTrace();
			lgr.log(Level.ERROR, ex.getMessage() + "; query: " + query, ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				ex.printStackTrace();
				lgr.log(Level.INFO, ex.getMessage() + "; query: " + query, ex);
				throw ex;
			}
		}
	}

	public static List<List<String>> getRows(final ResultSet rs) throws SQLException {
		if (rs == null) {
			return null;
		}
		List<List<String>> rows = new ArrayList<>();
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int NumOfCol = rsmd.getColumnCount();
			while (rs.next()) {
				List<String> current_row = new ArrayList<String>();
				for (int i = 1; i <= NumOfCol; i++) {
					Object value = rs.getObject(i);
					if (value == null) {
						current_row.add("null");
					} else {
						current_row.add(value.toString());
					}
				}
				rows.add(current_row);
			}
			return rows;
		} catch (SQLException e) {
			throw e;
		}
	}

	public static List<String> getColumnNames(final ResultSetMetaData rsmd) throws SQLException {
		List<String> columnNames = new ArrayList<String>();
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			columnNames.add(rsmd.getColumnLabel(i));
		}
		return columnNames;
	}

	public static List<String> getColumnTypes(final ResultSetMetaData rsmd) throws SQLException {
		List<String> columnTypes = new ArrayList<String>();
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			columnTypes.add(rsmd.getColumnTypeName(i));
		}
		return columnTypes;
	}

	public List<Integer> getPrimaryColumns(final String table) throws SQLException {
		List<Integer> primaryColNum = new ArrayList<Integer>();
		String query = "SELECT pg_attribute.attnum " + "FROM pg_index, pg_class, pg_attribute, pg_namespace " + "WHERE "
				+ "pg_class.oid = ?::regclass AND " + "indrelid = pg_class.oid AND nspname = 'public' AND "
				+ "pg_class.relnamespace = pg_namespace.oid AND " + "pg_attribute.attrelid = pg_class.oid AND "
				+ "pg_attribute.attnum = any(pg_index.indkey) AND indisprimary";
		// System.out.println(query);
		try {
			getConnection();
			preparedSt = con.prepareStatement(query);
			preparedSt.setString(1, table);
			rs = preparedSt.executeQuery();
			while (rs.next()) {
				// System.out.println("Primary column number: "+rs.getInt(1));
				primaryColNum.add(new Integer(rs.getInt(1)));
			}
		} finally {
			cleanPostgreSQLResources();
		}
		return primaryColNum;
	}

	@Override
	public Shim getShim() {
		return Shim.PSQLRELATION;
	}

	/**
	 * NEW FUNCTION generate the "CREATE TABLE" clause from existing tables on
	 * DB. Recommend use with 'bigdawg_schema'
	 * 
	 * @param schemaAndTableName
	 * @return
	 * @throws SQLException
	 */
	public String getCreateTable(String schemaAndTableName) throws SQLException {

		StringBuilder extraction = new StringBuilder();

		this.getConnection();
		st = con.createStatement();

		rs = st.executeQuery("SELECT attrelid, attname, format_type(atttypid, atttypmod) AS type, atttypid, atttypmod "
				+ "FROM pg_catalog.pg_attribute " + "WHERE NOT attisdropped AND attrelid = '" + schemaAndTableName
				+ "'::regclass AND atttypid NOT IN (26,27,28,29) " + "ORDER BY attnum;");

		if (rs.next()) {
			extraction.append("CREATE TABLE ").append(schemaAndTableName).append(" (");
			extraction.append(rs.getString("attname")).append(" ");
			extraction.append(rs.getString("type"));
		}
		while (rs.next()) {
			extraction.append(", ");
			extraction.append(rs.getString("attname")).append(" ");
			extraction.append(rs.getString("type"));
		}
		extraction.append(");");
		// rs.close();

		return extraction.toString();
	}

	/**
	 * MOVED FROM ExecutionNodeFactory, etc.
	 * 
	 * @param dbid
	 * @return connection info associated with the DBID
	 * @throws Exception
	 */
	public static ConnectionInfo generateConnectionInfo(int dbid) throws Exception {
		ArrayList<String> infos = CatalogViewer.getConnectionInfo(dbid);
		return new PostgreSQLConnectionInfo(infos.get(0), infos.get(1), infos.get(2), infos.get(3), infos.get(4));
	}

	/**
	 * Get metadata about columns (column name, position, data type, etc) for a
	 * table in PostgreSQL.
	 * 
	 * @param conInfo
	 * @param tableName
	 * @return instance of a #PosgreSQLColumnMetaData class
	 * @throws SQLException
	 *             if the data extraction from PostgreSQL failed
	 */
	public List<PostgreSQLColumnMetaData> getColumnsMetaData(String tableNameInitial) throws SQLException {
		try {
			this.getConnection();
			PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(tableNameInitial);
			try {
				preparedSt = con.prepareStatement(
						"SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale "
								+ "FROM information_schema.columns " + "WHERE table_schema=? and table_name=?"
								+ " order by ordinal_position");
				preparedSt.setString(1, schemaTable.getSchemaName());
				preparedSt.setString(2, schemaTable.getTableName());
			} catch (SQLException e) {
				e.printStackTrace();
				log.error("PostgreSQLHandler, the query preparation failed.");
				throw e;
			}
			ResultSet resultSet = preparedSt.executeQuery();
			List<PostgreSQLColumnMetaData> result = new ArrayList<>();
			while (resultSet.next()) {
				result.add(new PostgreSQLColumnMetaData(resultSet.getString(1), resultSet.getInt(2),
						resultSet.getBoolean(3), resultSet.getString(4), resultSet.getInt(5), resultSet.getInt(6),
						resultSet.getInt(7)));
			}
			return result;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(ex.getMessage() + "; conInfo: " + conInfo.toString() + "; table: " + tableNameInitial, ex);
				throw ex;
			}
		}
	}

	/**
	 * Check if a schema exists.
	 * 
	 * @param conInfo
	 * @param schemaName
	 *            the name of the schema to be checked if exists
	 * @return
	 * @throws SQLException
	 */
	public boolean existsSchema(String schemaName) throws SQLException {
		try {
			this.getConnection();
			try {
				preparedSt = con.prepareStatement(
						"select exists (select 1 from information_schema.schemata where schema_name=?)");
				preparedSt.setString(1, schemaName);
			} catch (SQLException e) {
				e.printStackTrace();
				log.error(e.getMessage()
						+ " PostgreSQLHandler, the query preparation for checking is a schema exists failed.");
				throw e;
			}
			try {
				ResultSet rs = preparedSt.executeQuery();
				rs.next();
				return rs.getBoolean(1);
			} catch (SQLException e) {
				e.printStackTrace();
				log.error(e.getMessage() + " Failed to check if a schema exists.");
				throw e;
			}
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(ex.getMessage() + "; conInfo: " + conInfo.toString() + "; schemaName: " + schemaName, ex);
				throw ex;
			}
		}
	}

	/**
	 * Create schema if not exists.
	 * 
	 * @param conInfo
	 *            connection to PostgreSQL
	 * @param schemaName
	 *            the name of a schema to be created (if not already exists).
	 * @return true if schema was created (false is schema already existed)
	 * @throws SQLException
	 */
	public void createSchemaIfNotExists(String schemaName) throws SQLException {
		executeStatementPostgreSQL("create schema if not exists " + schemaName);
	}

	/**
	 * Create a table if it not exists.
	 * 
	 * @param schemaTable
	 *            give the name of the table and the schema where it resides
	 * @throws SQLException
	 */
	public void createTable(String createTableStatement) throws SQLException {
		executeStatementPostgreSQL(createTableStatement);
	}
	
	public void dropSchemaIfExists(String schemaName) throws SQLException {
		executeStatementPostgreSQL("drop schema if exists "+schemaName);
	}
	
	/**
	 * Drop a table. 
	 * 
	 * @param tableName the name of the table
	 * @throws SQLException
	 */
	public void dropTableIfExists(String tableName) throws SQLException {
		executeStatementPostgreSQL("drop table if exists "+tableName);
	}

	/**
	 * Check if a table exists.
	 * 
	 * @param conInfo
	 * @param schemaTable
	 *            names of a schema and a table
	 * @return true if the table exists, false if there is no such table in the
	 *         given schema
	 * @throws SQLException
	 */
	public boolean existsTable(PostgreSQLSchemaTableName schemaTable) throws SQLException {
		try {
			this.getConnection();
			try {
				preparedSt = con.prepareStatement(
						"select exists (select 1 from information_schema.tables where table_schema=? and table_name=?)");
				preparedSt.setString(1, schemaTable.getSchemaName());
				preparedSt.setString(2, schemaTable.getTableName());
			} catch (SQLException e) {
				e.printStackTrace();
				log.error(e.getMessage()
						+ " PostgreSQLHandler, the query preparation for checking if a table exists failed.");
				throw e;
			}
			try {
				ResultSet rs = preparedSt.executeQuery();
				rs.next();
				return rs.getBoolean(1);
			} catch (SQLException e) {
				e.printStackTrace();
				log.error(e.getMessage() + " Failed to check if a table exists.");
				throw e;
			}
		} finally {
			try {
				cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(ex.getMessage() + "; conInfo: " + conInfo.toString() + "; schemaName: "
						+ schemaTable.getSchemaName() + " tableName: " + schemaTable.getTableName(), ex);
				throw ex;
			}
		}
	}

}
