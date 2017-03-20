/**
 * 
 */
package istc.bigdawg.relational;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;
import org.apache.log4j.Logger;

import javax.ws.rs.core.Response;
import java.sql.*;
import java.util.*;

/**
 * @author Kate Yu
 * 
 */
public interface RelationalHandler extends DBHandler, ExecutorEngine {
	/**
	 * log
	 */
	Logger log = Logger
			.getLogger(RelationalHandler.class.getName());

	/**
	 * Establish connection to the database for this instance.
	 *
	 * @throws SQLException
	 *             if could not establish a connection
	 */
	public Connection getConnection() throws SQLException;

	/**
	 * Get the JDBC connection to the database.
	 *
	 * @param conInfo
	 *            connection information (host, port, database name, etc.)
	 * @return the JDBC connection to the database
	 */
	public static Connection getConnection(ConnectionInfo conInfo)
			throws SQLException {
		Connection con;
		String url = conInfo.getUrl();
		String user = conInfo.getUser();
		String password = conInfo.getPassword();
		try {
			try {
				Class.forName("com.mysql.cj.jdbc.Driver");
				Class.forName("com.vertica.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				log.error("Could not find mysql or vertica drivers");
			}
			con = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			String msg = "BigDAWG: Could not connect to the relational instance: Url: "
					+ url + " User: " + user + " Password: " + password
					+ "; Original message from database engine: " + e.getMessage();
			//log.error(msg + " " + StackTrace.getFullStackTrace(e));
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
	public Response executeQuery(String queryString);

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
//			log.debug(
//					"Statement to be executed: " + stringStatement);
			statement.execute(stringStatement);
			statement.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
			// remove ' from the statement - otherwise it won't be inserted into
			// log table in Postgres
//			log.error(ex.getMessage() + "; statement to be executed: "
//					+ LogUtils.replace(stringStatement) + " "
//					+ ex.getStackTrace(), ex);
			throw ex;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
	}

	/**
	 * Executes a statement (not a query). It cleans the resources
	 * at the end.
	 *
	 * @param statement
	 *            to be executed
	 * @throws SQLException
	 */
	public void executeStatementOnConnection(String statement) throws SQLException;

	/**
	 * It executes the SQL command and releases the resources at the end,
	 * returning a QueryResult if present
	 *
	 * @param query
	 * @return #Optional<QueryResult>
	 * @throws SQLException
	 */
	public Optional<QueryResult> execute(final String query)
			throws LocalQueryExecutionException;

	/**
	 * It executes the query and releases the resources at the end.
	 *
	 * @param query
	 * @return #JdbcQueryResult
	 * @throws SQLException
	 */
	public JdbcQueryResult executeQueryOnEngine(final String query)
			throws SQLException;


	/**
	 * NEW FUNCTION generate the "CREATE TABLE" clause from existing tables on
	 * DB. Recommend use with 'bigdawg_schema'
	 *
	 * @param schemaAndTableName
	 * @return
	 * @throws SQLException
	 */
	public String getCreateTable(String schemaAndTableName) throws SQLException;

	/**
	 * Get names of the column in the table.
	 *
	 * @param table
	 *            the name of the table
	 * @return list of names of columns for the table
	 * @throws SQLException
	 */
	public List<String> getColumnNames(String table) throws SQLException;

	/**
	 * Check if a schema exists.
	 *
	 * @param schemaName
	 *            the name of the schema to be checked if exists
	 * @return
	 * @throws SQLException
	 */
	public boolean existsSchema(String schemaName) throws SQLException;

	/**
	 * Create schema if not exists.
	 *
	 * @param schemaName
	 *            the name of a schema to be created (if not already exists).
	 * @return true if schema was created (false is schema already existed)
	 * @throws SQLException
	 */
	public void createSchemaIfNotExists(String schemaName) throws SQLException;

	/**
	 * Create a table if it not exists.
	 *
	 * @param createTableStatement
	 *            statement used to create the desired table
	 * @throws SQLException
	 */
	public void createTable(String createTableStatement) throws SQLException;

	public void dropSchemaIfExists(String schemaName) throws SQLException;


	/**
	 * Check if the table with a given name exists in the PostgreSQL database.
	 *
	 * @see DBHandler#existsObject(String)
	 */
	@Override
	public boolean existsObject(String name) throws SQLException;

	/**
	 * @see #existsTable(RelationalSchemaTableName)
	 *
	 * @param table
	 *            The name of the table.
	 * @return true if the table exists, false if there is no such table
	 * @throws SQLException
	 */
	public boolean existsTable(String table) throws SQLException;

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
			throws SQLException;

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#close()
	 */
	@Override
	public void close() throws Exception;
}
