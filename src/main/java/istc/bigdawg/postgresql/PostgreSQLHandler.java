/**
 * 
 */
package istc.bigdawg.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

import istc.bigdawg.relational.RelationalHandler;
import istc.bigdawg.relational.RelationalSchemaTableName;
import istc.bigdawg.relational.RelationalTableMetaData;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.executor.IslandQueryResult;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 */
public class PostgreSQLHandler implements RelationalHandler {
	/**
	 * log
	 */
	private static Logger log = Logger
			.getLogger(PostgreSQLHandler.class.getName());

	private static int defaultSchemaServerDBID = BigDawgConfigProperties.INSTANCE
			.getPostgresSchemaServerDBID();

	/**
	 * Information about connection to PostgreSQL (e.g. IP, port, etc.).
	 */
	private PostgreSQLConnectionInfo conInfo = null;

	private Connection con = null;
	private Statement st = null;
	private PreparedStatement preparedSt = null;
	private ResultSet rs = null;

	/**
	 * Initialize the PostgreSQL handler with only the connection information.
	 * 
	 * @param conInfo
	 *            information about connection to an instance of PostgreSQL
	 *            database
	 */
	public PostgreSQLHandler(PostgreSQLConnectionInfo conInfo) {
		this.conInfo = conInfo;
	}

	/**
	 * Initialize the instance with connection information and physical
	 * connection.
	 * 
	 * @param conInfo
	 *            Information aobut the connection to PostgreSQL (host,
	 *            password, user, etc.).
	 * 
	 * @param connection
	 *            Physical connection to PostgreSQL.
	 */
	public PostgreSQLHandler(ConnectionInfo conInfo, Connection connection) {
		this(conInfo);
		if (connection == null) {
			throw new IllegalArgumentException(
					"Physical connection to PostgreSQL has to be not null.");
		}
		this.con = connection;
	}

	public PostgreSQLHandler(ConnectionInfo conInfo) {
		if (conInfo instanceof PostgreSQLConnectionInfo) {
			this.conInfo = (PostgreSQLConnectionInfo) conInfo;
		} else {
			Exception e = new IllegalArgumentException(
					"The conInfo parameter has to be of "
							+ "type: PostgreSQLConnectionInfo.");
			log.error(e.getMessage() + " " + StackTrace.getFullStackTrace(e));
		}
	}

	public PostgreSQLHandler(int dbId) throws SQLException, BigDawgCatalogException {
		try {
			this.conInfo = (PostgreSQLConnectionInfo) CatalogViewer
					.getConnectionInfo(dbId);
		} catch (SQLException | BigDawgCatalogException e) {
			String msg = "Catalog chosen connection: " + conInfo.getHost() + " "
					+ conInfo.getPort() + " " + conInfo.getDatabase() + " "
					+ conInfo.getUser() + " " + conInfo.getPassword() + ".";
			log.error(msg);
			e.printStackTrace();
			throw e;
		}
	}

	public PostgreSQLHandler() {
		String msg = "Default handler. PostgreSQL parameters are "
				+ "taken from the BigDAWG configuration file.";
		log.info(msg);
	}

	/**
	 * Establish connection to PostgreSQL for this instance.
	 * 
	 * @throws SQLException
	 *             if could not establish a connection
	 */
	public Connection getConnection() throws SQLException {
		if (con == null) {
			if (conInfo != null) {
				try {
					con = getConnection(conInfo);
				} catch (SQLException e) {
					e.printStackTrace();
					log.error(e.getMessage()
							+ " Could not connect to PostgreSQL database using: "
							+ conInfo.toString(), e);
					throw e;
				}
			} else {
				con = PostgreSQLInstance.getConnection();
			}
		}
		return con;
	}

	/**
	 * Get the JDBC connection to the database. see:
	 * {@link #getConnection(PostgreSQLConnectionInfo)}
	 */
	public static Connection getConnection(ConnectionInfo conInfo)
			throws SQLException {
		if (conInfo instanceof PostgreSQLConnectionInfo) {
			return getConnection((PostgreSQLConnectionInfo) conInfo);
		}
		throw new IllegalArgumentException("The conInfo parameter has to be of "
				+ "type: PostgreSQLConnectionInfo.");
	}

	/**
	 * Get the JDBC connection to the database.
	 * 
	 * @param conInfo
	 *            connection information (host, port, database name, etc.)
	 * @return the JDBC connection to the database
	 */
	public static Connection getConnection(PostgreSQLConnectionInfo conInfo)
			throws SQLException {
		Connection con;
		String url = conInfo.getUrl();
		String user = conInfo.getUser();
		String password = conInfo.getPassword();
		try {
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			String msg = "BigDAWG: Could not connect to the PostgreSQL instance: Url: "
					+ url + " User: " + user + " Password: " + password
					+ "; Original message from Postgres: " + e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e));
			throw new SQLException(msg, e);
		}
		return con;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		long lStartTime = System.nanoTime();
		JdbcQueryResult queryResult = null;
		try {
			queryResult = executeQueryOnEngine(queryString);
		} catch (SQLException e) {
			return Response.status(500)
					.entity("Problem with query execution in Postgresql: "
							+ e.getMessage() + "; query: " + queryString)
					.build();
			// return "Problem with query execution in PostgreSQL: " +
			// queryString;
		}
		String messageQuery = "PostgreSQL query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
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

		String messageTABLE = "format TABLE Java time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		log.info(messageTABLE);

		return Response.status(200).entity(out).build();
	}

	public String computeDateArithmetic(String s) throws SQLException {
		JdbcQueryResult qr = executeQueryOnEngine("select date(" + s + ");");
		return qr.getRows().get(0).get(0);
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
	 * Execute an SQL statement on a given connection.
	 * 
	 * @param connection
	 *            connection on which the statement is executed
	 * @param stringStatement
	 *            sql statement to be executed
	 * @throws SQLException
	 */
	public static void executeStatement(Connection connection,
			String stringStatement) throws SQLException {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			log.debug(
					"Statement to be executed in Postgres: " + stringStatement);
			statement.execute(stringStatement);
			statement.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
			// remove ' from the statement - otherwise it won't be inserted into
			// log table in Postgres
			log.error(ex.getMessage() + "; statement to be executed: "
					+ LogUtils.replace(stringStatement) + " "
					+ ex.getStackTrace(), ex);
			throw ex;
		} finally {
			if (statement != null) {
				statement.close();
			}
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
	public void executeStatementPostgreSQL(String statement)
			throws SQLException {
		getConnection();
		try {
			executeStatement(con, statement);
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.info(ex.getMessage() + "; statement: "
						+ LogUtils.replace(statement), ex);
				throw ex;
			}
		}
	}

	public void executeStatementOnConnection(String statement)
			throws SQLException {
		executeStatementPostgreSQL(statement);
	}

	/**
	 * It executes the SQL command and releases the resources at the end,
	 * returning a QueryResult if present
	 *
	 * @param query
	 * @return #Optional<QueryResult>
	 * @throws SQLException
	 */
	public Optional<QueryResult> execute(final String query)
			throws LocalQueryExecutionException {
		try {

			log.debug("PostgreSQLHandler is attempting query: "
					+ LogUtils.replace(query) + "");
			log.debug("ConnectionInfo:\n" + this.conInfo.toString());

			this.getConnection();
			st = con.createStatement();
			if (st.execute(query)) {
				rs = st.getResultSet();
				return Optional.of(new JdbcQueryResult(rs, this.conInfo));
			} else {
				return Optional.of(new IslandQueryResult(this.conInfo));
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			// ex.printStackTrace();
			lgr.log(Level.ERROR,
					ex.getMessage() + "; query: " + LogUtils.replace(query),
					ex);
			throw new LocalQueryExecutionException(ex);
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				// ex.printStackTrace();
				lgr.log(Level.INFO,
						ex.getMessage() + "; query: " + LogUtils.replace(query),
						ex);
				throw new LocalQueryExecutionException(ex);
			}
		}
	}

	/**
	 * It executes the query and releases the resources at the end.
	 * 
	 * @param query
	 * @return #JdbcQueryResult
	 * @throws SQLException
	 */
	public JdbcQueryResult executeQueryOnEngine(final String query)
			throws SQLException {
		try {
			this.getConnection();

			log.debug("\n\nquery: " + LogUtils.replace(query) + "");
			if (this.conInfo != null) {
				log.debug("ConnectionInfo: " + this.conInfo.toString() + "\n");
			}

			st = con.createStatement();
			rs = st.executeQuery(query);

			return new JdbcQueryResult(rs, this.conInfo);
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			// ex.printStackTrace();
			lgr.log(Level.ERROR,
					ex.getMessage() + "; query: " + LogUtils.replace(query),
					ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				// ex.printStackTrace();
				lgr.log(Level.INFO,
						ex.getMessage() + "; query: " + LogUtils.replace(query),
						ex);
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
	 * @throws BigDawgCatalogException
	 * @throws SQLException
	 */
	public String generatePostgreSQLQueryXML(final String query)
			throws BigDawgCatalogException, SQLException {
		try {
			// this.getConnection();
			con = getConnection((PostgreSQLConnectionInfo) CatalogViewer
					.getConnectionInfo(defaultSchemaServerDBID));
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
			// ex.printStackTrace();
			lgr.log(Level.ERROR,
					ex.getMessage() + "; query: " + LogUtils.replace(query),
					ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				// ex.printStackTrace();
				lgr.log(Level.INFO,
						ex.getMessage() + "; query: " + LogUtils.replace(query),
						ex);
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
	public void populateSchemasSchema(final String query, boolean drop)
			throws SQLException {
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
			lgr.log(Level.ERROR,
					ex.getMessage() + "; query: " + LogUtils.replace(query),
					ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				ex.printStackTrace();
				lgr.log(Level.INFO,
						ex.getMessage() + "; query: " + LogUtils.replace(query),
						ex);
				throw ex;
			}
		}
	}

	public List<Integer> getPrimaryColumnsNoRecourceCleaning(final String table)
			throws SQLException {
		try {
			List<Integer> primaryColNum = new ArrayList<Integer>();
			String query = "SELECT pg_attribute.attnum "
					+ "FROM pg_index, pg_class, pg_attribute, pg_namespace "
					+ "WHERE " + "pg_class.oid = ?::regclass AND "
					+ "indrelid = pg_class.oid AND nspname = 'public' AND "
					+ "pg_class.relnamespace = pg_namespace.oid AND "
					+ "pg_attribute.attrelid = pg_class.oid AND "
					+ "pg_attribute.attnum = any(pg_index.indkey) AND indisprimary";
			// System.out.println(query);
			getConnection();
			preparedSt = con.prepareStatement(query);
			preparedSt.setString(1, table);
			rs = preparedSt.executeQuery();
			while (rs.next()) {
				// System.out.println("Primary column number: "+rs.getInt(1));
				primaryColNum.add(new Integer(rs.getInt(1)));
			}
			return primaryColNum;
		} catch (SQLException ex) {
			log.error("Problem with PostgreSQL.");
			throw ex;
		}
	}

	public List<Integer> getPrimaryColumns(final String table)
			throws SQLException {
		List<Integer> primaryColNum;
		try {
			primaryColNum = getPrimaryColumnsNoRecourceCleaning(table);
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
	 * see: {@link #getCreateTable(Connection, String, String)}
	 * 
	 * @param con
	 *            connection to postgresql
	 * @param fromTable
	 *            the table for which you want the original create table
	 *            statement
	 * @return the sql statement with create table
	 * @throws SQLException
	 */
	public static String getCreateTable(Connection con, String fromTable)
			throws SQLException {
		return getCreateTable(con, fromTable, fromTable);
	}

	/**
	 * Get SQL create table statement.
	 * 
	 * @param con
	 *            Connection to the database where the table is stored.
	 * @param fromTable
	 *            Name of the schema and name of the table.
	 * @param targetTableName
	 *            The name of the table in the final sql statement (it can be
	 *            different from the current name of the table in the database)
	 *            - the current name of the table will be replace with this
	 *            name.
	 * @return The SQL create table statement from the table in the database
	 *         represetned by con.
	 * @throws SQLException
	 *             When there is a problem with accessing the database.
	 */
	public static String getCreateTable(Connection con, String fromTable,
			String targetTableName) throws SQLException {
		try {
			StringBuilder extraction = new StringBuilder();

			Statement st = con.createStatement();

			ResultSet rs = st.executeQuery(
					"SELECT attrelid, attname, format_type(atttypid, atttypmod) "
							+ "AS type, atttypid, atttypmod "
							+ "FROM pg_catalog.pg_attribute "
							+ "WHERE NOT attisdropped AND attrelid = '"
							+ fromTable
							+ "'::regclass AND atttypid NOT IN (26,27,28,29) "
							+ "ORDER BY attnum;");

			if (rs.next()) {
				extraction.append("CREATE TABLE IF NOT EXISTS ")
						.append(targetTableName).append(" (");
				extraction.append(rs.getString("attname")).append(" ");
				extraction.append(rs.getString("type"));
			}
			while (rs.next()) {
				extraction.append(", ");
				extraction.append(rs.getString("attname")).append(" ");
				extraction.append(rs.getString("type"));
			}
			extraction.append(");");
			rs.close();
			st.close();
			return extraction.toString();
		} catch (SQLException ex) {
			ex.printStackTrace();
			log.error(ex.getMessage() + "; conInfo: " + con.getClientInfo()
					+ "; fromTable: " + fromTable + " ; toTable: "
					+ targetTableName + StackTrace.getFullStackTrace(ex), ex);
			throw ex;
		}

	}

	/**
	 * NEW FUNCTION generate the "CREATE TABLE" clause from existing tables on
	 * DB. Recommend use with 'bigdawg_schema'
	 * 
	 * @param schemaAndTableName
	 * @return
	 * @throws SQLException
	 */
	public String getCreateTable(String schemaAndTableName)
			throws SQLException {
		try {
			getConnection();
			return getCreateTable(con, schemaAndTableName, schemaAndTableName);
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(ex.getMessage() + "; conInfo: " + conInfo.toString()
						+ "; schemaAndTableName: " + schemaAndTableName + " "
						+ ex.getStackTrace(), ex);
				throw ex;
			}
		}
	}

	/**
	 * MOVED FROM ExecutionNodeFactory, etc.
	 * 
	 * @param dbid
	 * @return connection info associated with the DBID
	 * @throws Exception
	 */
	@Deprecated
	public static ConnectionInfo generateConnectionInfo(int dbid)
			throws Exception {
		return CatalogViewer.getConnectionInfo(dbid);
	}

	/**
	 * see: {@link #getObjectMetaData(String)}
	 * 
	 * @param tableNameInitial
	 * @return
	 * @throws SQLException
	 */
	public RelationalTableMetaData getTableMetaData(String tableNameInitial)
			throws SQLException {
		return getObjectMetaData(tableNameInitial);
	}

	/**
	 * Get metadata about columns (column name, position, data type, etc) for a
	 * table in PostgreSQL.
	 * 
	 * @param tableNameInitial
	 *            the name of the table
	 * @return map column name to column meta data
	 * @throws SQLException
	 *             if the data extraction from PostgreSQL failed
	 */
	public PostgreSQLTableMetaData getObjectMetaData(String tableNameInitial)
			throws SQLException {
		try {
			this.getConnection();
			PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
					tableNameInitial);
			try {
				preparedSt = con.prepareStatement(
						"SELECT column_name, ordinal_position, is_nullable, "
								+ "data_type, character_maximum_length, "
								+ "numeric_precision, numeric_scale "
								+ "FROM information_schema.columns "
								+ "WHERE table_schema=? and table_name ilike ?"
								+ " order by ordinal_position;");
				preparedSt.setString(1, schemaTable.getSchemaName());
				preparedSt.setString(2, schemaTable.getTableName());
				// postgresql logger cannot accept single quotes
				log.debug("replace double quotes (\") with signle quotes "
						+ "in the query to log it in PostgreSQL: "
						+ preparedSt.toString().replace("'", "\""));
			} catch (SQLException e) {
				e.printStackTrace();
				log.error("PostgreSQLHandler, the query preparation failed. "
						+ e.getMessage() + " "
						+ StackTrace.getFullStackTrace(e));
				throw e;
			}
			ResultSet resultSet = preparedSt.executeQuery();
			Map<String, AttributeMetaData> columnsMap = new HashMap<>();
			List<AttributeMetaData> columnsOrdered = new ArrayList<>();
			/*
			 * resultSet.isBeforeFirst() returns false if the cursor is not
			 * before the first record or if there are no rows in the result set
			 */
			if (!resultSet.isBeforeFirst()) {
				throw new IllegalArgumentException(String.format(
						"No results were found for the table: %s; connection: %s",
						schemaTable.getFullName(), this.conInfo));
			}
			while (resultSet.next()) {
				/**
				 * ordinal position in PostgreSQL starts from 1 but we want it
				 * to start from 0.
				 */
				AttributeMetaData columnMetaData = new AttributeMetaData(
						resultSet.getString(1), resultSet.getInt(2) - 1,
						resultSet.getBoolean(3), resultSet.getString(4),
						resultSet.getInt(5), resultSet.getInt(6),
						resultSet.getInt(7));
				columnsMap.put(resultSet.getString(1), columnMetaData);
				columnsOrdered.add(columnMetaData);
			}
			return new PostgreSQLTableMetaData(schemaTable, columnsMap,
					columnsOrdered);
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(ex.getMessage() + "; conInfo: " + conInfo.toString()
						+ "; table: " + tableNameInitial + " "
						+ StackTrace.getFullStackTrace(ex), ex);
				throw ex;
			}
		}
	}

	/**
	 * Get names of the column in the table.
	 * 
	 * @param table
	 *            the name of the table
	 * @return list of names of columns for the table
	 * @throws SQLException
	 */
	public List<String> getColumnNames(String table) throws SQLException {
		List<String> columnNames = new ArrayList<>();
		for (AttributeMetaData meta : getObjectMetaData(table)
				.getAttributesOrdered()) {
			columnNames.add(meta.getName());
		}
		return columnNames;
	}

	/**
	 * Check if a schema exists.
	 *
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
						+ " PostgreSQLHandler, the query preparation for checking if a schema exists failed.");
				throw e;
			}
			try {
				ResultSet rs = preparedSt.executeQuery();
				rs.next();
				return rs.getBoolean(1);
			} catch (SQLException e) {
				e.printStackTrace();
				log.error(e.getMessage()
						+ " Failed to check if a schema exists.");
				throw e;
			}
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(ex.getMessage() + "; conInfo: " + conInfo.toString()
						+ "; schemaName: " + schemaName, ex);
				throw ex;
			}
		}
	}

	/**
	 * Create schema if not exists.
	 *
	 * @param schemaName
	 *            the name of a schema to be created (if not already exists).
	 * @return true if schema was created (false is schema already existed)
	 * @throws SQLException
	 */
	public void createSchemaIfNotExists(String schemaName) throws SQLException {
		executeStatementOnConnection("create schema if not exists " + schemaName);
	}

	/**
	 * Create a table if it not exists.
	 * 
	 * @param createTableStatement
	 *            give the name of the table and the schema where it resides
	 * @throws SQLException
	 */
	public void createTable(String createTableStatement) throws SQLException {
		executeStatementOnConnection(createTableStatement);
	}

	public void dropSchemaIfExists(String schemaName) throws SQLException {
		executeStatementOnConnection("drop schema if exists " + schemaName);
	}

	/**
	 * Drop a table.
	 * 
	 * @param tableName
	 *            the name of the table
	 * @throws SQLException
	 */
	public void dropDataSetIfExists(String tableName) throws SQLException {
		executeStatementOnConnection(RelationalHandler.getDropTableStatement(tableName));
	}

	/**
	 * Create a new schema and table in the connectionTo if they not exist.
	 * 
	 * The definition of the table could have been inferred e.g. from an array
	 * in SciDB.
	 * 
	 * @param postgresCon
	 *            to which database we connect to
	 * @param toTable
	 *            to which table we want to load the data
	 * @param createTableStatement
	 *            sql statement to create a table in PostgreSQL
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 * @throws NoTargetArrayException
	 */
	public static void createTargetTableSchema(Connection postgresCon,
			String name, String toTable, String createTableStatement) throws SQLException {
		PostgreSQLSchemaTableName schemaTable = new PostgreSQLSchemaTableName(
				toTable);
		PostgreSQLHandler.executeStatement(postgresCon,
				"create schema if not exists " + schemaTable.getSchemaName());
		if (name != null && BigDawgConfigProperties.INSTANCE.isPostgreSQLDropDataSet()) {
			PostgreSQLHandler.executeStatement(postgresCon, RelationalHandler.getDropTableStatement(name));
		}
		PostgreSQLHandler.executeStatement(postgresCon, createTableStatement);
	}

	/**
	 * Direction for the PostgreSQL copy command.
	 * 
	 * @author Adam Dziedzic
	 */
	public enum DIRECTION {
		TO, FROM
	};

	/**
	 * Copy out to STDOUT. Copy in from STDIN.
	 * 
	 * @author Adam Dziedzic
	 */
	public enum STDIO {
		STDOUT, STDIN
	}

	/**
	 * Get the postgresql command to copy data.
	 * 
	 * @param table
	 *            table from/to which you want to copy the data
	 * @param direction
	 *            to/from STDOUT
	 * @return the command to copy data
	 */
	public static String getCopyBinCommand(String table, DIRECTION direction,
			STDIO stdio) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append(direction.toString() + " ");
		copyFromStringBuf
				.append(stdio.toString() + " with binary");/* with binary */
		return copyFromStringBuf.toString();
	}

	public static String getExportBinCommand(String table) {
		return getCopyBinCommand(table, DIRECTION.TO, STDIO.STDOUT);
	}

	public static String getLoadBinCommand(String table) {
		return getCopyBinCommand(table, DIRECTION.FROM, STDIO.STDIN);
	}

//	public static String getMySQLExportCsvCommand(String table, String delimiter,
//			String quote, boolean isHeader, String fileName) {
//		StringBuilder copyFromStringBuf = new StringBuilder();
//		copyFromStringBuf.append("SELECT * INTO OUTFILE '");
//		copyFromStringBuf.append(fileName);
//		copyFromStringBuf.append("' FIELDS TERMINATED BY '" + delimiter + "'");
//		//copyFromStringBuf.append(" OPTIONALLY ENCLOSED BY '" + quote + "'");
//		copyFromStringBuf.append(" LINES TERMINATED BY '\n'");
//		copyFromStringBuf.append(" FROM " + table + ";");
//		String copyCsvCommand = copyFromStringBuf.toString();
//		log.debug("mysql copy csv command: " + copyCsvCommand);
//		return copyCsvCommand;
//	}

	/**
	 * Command to copy data from a table in PostgreSQL.
	 * 
	 * @param table
	 *            the name of the table from which we extract data
	 * @param delimiter
	 *            the delimiter for the output CSV file
	 * 
	 * @return the command to extract data from a table in PostgreSQL
	 */
	public static String getCopyCsvCommand(String table, DIRECTION direction,
			STDIO stdio, String delimiter, String quote, boolean isHeader) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append(direction.toString() + " ");
		copyFromStringBuf.append(stdio.toString());
		copyFromStringBuf.append(" with (format csv, delimiter '" + delimiter
				+ "', quote \"" + quote + "\", NULL 'null', header "
				+ (isHeader ? "true" : "false") + ")");
		String copyCsvCommand = copyFromStringBuf.toString();
		log.debug("copy csv command: " + copyCsvCommand);
		return copyCsvCommand;
	}

	public static String getExportCsvCommand(String table, String delimiter,
			String quote, boolean isHeader) {
		return getCopyCsvCommand(table, DIRECTION.TO, STDIO.STDOUT, delimiter,
				quote, isHeader);
	}

	public static String getLoadCsvCommand(String table, String delimiter,
			String quote, boolean isHeader) {
		return getCopyCsvCommand(table, DIRECTION.FROM, STDIO.STDIN, delimiter,
				quote, isHeader);
	}

	/**
	 * Check if the table with a given name exists in the PostgreSQL database.
	 * 
	 * @see istc.bigdawg.query.DBHandler#existsObject(java.lang.String)
	 */
	@Override
	public boolean existsObject(String name) throws SQLException {
		return existsTable(new PostgreSQLSchemaTableName(name));
	}

	/**
	 * Check if a given table exists in the PostgreSQL database.
	 * 
	 * @param connectionInfo
	 *            The information about the connection to PostgreSQL.
	 * @param table
	 *            The name of the table in PostgreSQL.
	 * @return true if table exists, false otherwise.
	 * @throws SQLException
	 *             Problem with a connection to PostgreSQL.
	 */
	public static boolean existsTable(ConnectionInfo connectionInfo,
			String table) throws SQLException {
		PostgreSQLHandler handler = new PostgreSQLHandler(connectionInfo);
		boolean existsTable = handler
				.existsTable(new PostgreSQLSchemaTableName(table));
		try {
			handler.close();
		} catch (Exception ex) {
			log.error("Could not close PostgreSQLHandler. " + ex.getMessage());
		}
		return existsTable;
	}

	/**
	 * @see #existsTable(RelationalSchemaTableName)
	 * 
	 * @param table
	 *            The name of the table.
	 * @return true if the table exists, false if there is no such table
	 * @throws SQLException
	 */
	public boolean existsTable(String table) throws SQLException {
		return existsTable(new PostgreSQLSchemaTableName(table));
	}

	/**
	 * Check if a table exists.
	 *
	 * @param schemaTable
	 *            names of a schema and a table
	 * @return true if the table exists, false if there is no such table in the
	 *         given schema
	 * @throws SQLException
	 */
	public boolean existsTable(RelationalSchemaTableName schemaTable)
			throws SQLException {
		/* Get the connection if it has not been established. */
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
	}

	/**
	 * @return Create statement for a table in PostgreSQL from a given list of
	 *         attributes.
	 */
	public static String getCreatePostgreSQLTableStatement(String toTable,
			List<AttributeMetaData> attributes) {
		StringBuilder createTableStringBuf = new StringBuilder();
		createTableStringBuf
				.append("create table if not exists " + toTable + " (");
		for (AttributeMetaData attribute : attributes) {
			String attrName = attribute.getName();
			String sqlType = attribute.getSqlDataType();

			// Escape name if necessary
			if (attrName.contains(" ") || PostgresSQLReserved.reservedWords.containsKey(attrName.toUpperCase())) {
				createTableStringBuf.append("\"");
				createTableStringBuf.append(attrName);
				createTableStringBuf.append("\"");
			}
			else {
				createTableStringBuf.append(attrName);
			}

			createTableStringBuf.append(" ");
			createTableStringBuf.append(sqlType);
			createTableStringBuf.append(",");
		}
		createTableStringBuf.deleteCharAt(createTableStringBuf.length() - 1);
		createTableStringBuf.append(")");
		log.debug("create table command: " + createTableStringBuf.toString());
		return createTableStringBuf.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#close()
	 */
	@Override
	public void close() throws Exception {
		cleanPostgreSQLResources();
	}

}
