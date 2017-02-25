/**
 * 
 */
package istc.bigdawg.scidb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.scidb.jdbc.IStatementWrapper;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.executor.ConstructedQueryResult;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.migration.datatypes.FromSciDBToSQLTypes;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleList;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.ObjectMapperResource;
import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.Tuple.Tuple2;

/**
 * SciDB Handler to execute statements in SciDB.
 * 
 * Note: SciDB does not support transactions.
 * 
 * @author Adam Dziedzic
 * 
 */
public class SciDBHandler implements DBHandler, ExecutorEngine {

	/**
	 * log
	 */
	private static Logger log = Logger.getLogger(SciDBHandler.class.getName());

	/**
	 * SciDB does include CSV header for exported data and we cannot remove it.
	 * 
	 * By default, the data should be sent without headers.
	 */
	private static final boolean IS_CSV_EXPORT_HEADER = true;

	/**
	 * SciDB can accept data in CSV format with or without header. By default we
	 * exchange data without header, because we read the metadata about
	 * tables/arrays/object separately.
	 */
	private static final boolean IS_CSV_LOAD_HEADER = false;

	/** Delimter for SciDB export in CSV format. */
	private static final String CSV_EXPORT_DELIMITER = ",";

	/** Information about the connection to SciDB. IP, port, bin path, etc. */
	private SciDBConnectionInfo conInfo = null;

	/** Physical connection to SciDB. */
//	private Connection connection = null;

	/**
	 * Types of languages for SciDB.
	 * 
	 * @author Adam Dziedzic
	 */
	public enum Lang {
		AQL, AFL
	};

	/**
	 * Create a default SciDB Handler with parameters from the configuration
	 * file.
	 * 
	 * @throws SQLException
	 */
	public SciDBHandler() throws SQLException {
		this.conInfo = new SciDBConnectionInfo();
		try {
//			this.connection = 
					getConnection(conInfo);
		} catch (Exception e) {
			log.debug(
					"getConnection throws Exception from default SciDBHandler(); "
							+ "this is not supposed to happen. Check properties file.");
			e.printStackTrace();
		}
	}

	private SciDBHandler(String emptyInstance) {
		log.debug(emptyInstance);
	}

	public static SciDBHandler getInstance() {
		return new SciDBHandler("Private empty instance");
	}

	/**
	 * Create a new SciDB handler for a given connection. You have to close the
	 * handler at the end to release the resources.
	 * 
	 * @param conInfo
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public SciDBHandler(SciDBConnectionInfo conInfo) throws SQLException {
		this.conInfo = conInfo;
//		this.connection = getConnection(conInfo);
	}

	/**
	 * see: {@link #SciDBHandler(ConnectionInfo)}
	 */
	public SciDBHandler(ConnectionInfo conInfo) throws SQLException {
		if (!(conInfo instanceof SciDBConnectionInfo)) {
			throw new IllegalArgumentException("The conInfo has to be of type: "
					+ SciDBConnectionInfo.class.getCanonicalName());
		}
		this.conInfo = (SciDBConnectionInfo) conInfo;
//		this.connection = getConnection(this.conInfo);
	}

	public SciDBHandler(int dbid) {
		try {
			this.conInfo = (SciDBConnectionInfo) CatalogViewer
					.getConnectionInfo(dbid);
//			this.connection = getConnection(conInfo);
		} catch (Exception e) {
			log.error("Attempted to get connection info for dbid "
					+ String.valueOf(dbid) + ". Check properties file.");
			log.error(StackTrace.getFullStackTrace(e));
		}
	}

//	/**
//	 * see: {@link #getConnection(SciDBConnectionInfo)}
//	 * 
//	 * @param conInfo
//	 *            Information on the conection (host,port,etc.).
//	 * @return the JDBC connection to SciDB
//	 * @throws SQLException
//	 */
//	public static Connection getConnection(ConnectionInfo conInfo)
//			throws SQLException {
//		if (conInfo instanceof SciDBConnectionInfo) {
//			return getConnection((SciDBConnectionInfo) conInfo);
//		}
//		throw new IllegalArgumentException(
//				"The conInfo parameter should represent a connection to SciDB and be of type: "
//						+ SciDBConnectionInfo.class.getCanonicalName());
//	}

	/**
	 * Create a JDBC connectin to SciDB based on the conInfo (host,port,etc.).
	 * 
	 * @param conInfo
	 *            Information on the conection (host,port,etc.).
	 * @return the JDBC connection to SciDB
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public static Connection getConnection(ConnectionInfo connectionInfo)
			throws SQLException {
		SciDBConnectionInfo conInfo = null;
		if (!(connectionInfo instanceof SciDBConnectionInfo)) {
			throw new IllegalArgumentException(
					"The conInfo parameter should represent a connection to SciDB and be of type: "
							+ SciDBConnectionInfo.class.getCanonicalName());
		} else {
			conInfo = (SciDBConnectionInfo) connectionInfo;
		}
		try {
			Class.forName("org.scidb.jdbc.Driver");
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			log.error("SciDB jdbc driver is not in the CLASSPATH -> "
					+ ex.getMessage() + " " + StackTrace.getFullStackTrace(ex),
					ex);
			throw new RuntimeException(ex.getMessage());
		}
		try {
			return DriverManager.getConnection(conInfo.getUrl());
		} catch (SQLException ex) {
			ex.printStackTrace();
			log.error("Could not establish a connection to a SciDB database. "
					+ conInfo.toString() + " " + ex.getMessage()
					+ StackTrace.getFullStackTrace(ex), ex);
			throw ex;
		}
	}

	/**
	 * Commit the transaction in SciDB.
	 * 
	 * @throws SQLException
	 */
	@Deprecated
	public void commit() throws SQLException {
//		try {
//			if (connection != null && !connection.isClosed()) {
//				connection.commit();
//			}
//		} catch (SQLException ex) {
//			ex.printStackTrace();
//			log.error("Could not commit for a connection to a SciDB database. "
//					+ conInfo.toString() + " " + ex.getMessage()
//					+ StackTrace.getFullStackTrace(ex), ex);
//			throw ex;
//		}
	}
	
	@Override
	/**
	 * You have to close the handler at the end to release the resources.
	 * 
	 * @throws SQLException
	 */
	@Deprecated
	public void close() throws SQLException {
//		if (connection != null) {
//			try {
//				commit();
//				if (!connection.isClosed()) {
//					connection.close();
//				}
//				connection = null;
//			} catch (SQLException e) {
//				e.printStackTrace();
//				log.error("Could not close the connection to a SciDB database. "
//						+ conInfo.toString() + " " + e.getMessage()
//						+ StackTrace.getFullStackTrace(e), e);
//				throw e;
//			}
//		}
//		if (conInfo != null) {
//			conInfo = null;
//		}
	}

	@Override
	@Deprecated
	public Connection getConnection() throws SQLException {
		throw new SQLException("SciDBHandler does not support getConnection.");
	}
	
	/**
	 * The query executed via iquery (not JDBC).
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	@Deprecated
	public Response executeQuery(String queryString) {
		log.debug("run query for SciDB. SciDB queryString: " + queryString);
		String resultSciDB;
		try {
			resultSciDB = executeQueryScidb(queryString);
			return Response.status(200).entity(resultSciDB).build();
		} catch (IOException | InterruptedException | SciDBException e) {
			e.printStackTrace();
			String messageSciDB = "Problem with SciDB: " + e.getMessage();
			log.error(messageSciDB);
			return Response.status(200).entity(messageSciDB).build();
		}
	}
	
	
	/**
	 * Wrapper for execute statement. Create the handler only when needed and
	 * destroy it after the operation.
	 * 
	 * @param conTo
	 *            Connection info to SciDB.
	 * @param statement
	 *            AFL statement to be executed in SciDB.
	 * @throws SQLException
	 */
	public static void executeStatementAQL(ConnectionInfo conTo, String statement)
			throws SQLException {
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatementAQL(statement);
	}

	public static void executeStatementAFL(ConnectionInfo conTo, String stringStatement)
			throws SQLException {
		SciDBHandler handler = new SciDBHandler(conTo);
		handler.executeStatementSciDB(stringStatement, Lang.AFL);
	}
	
	/**
	 * This statement will be executed via jdbc in AFL language.
	 * 
	 * @param stringStatement
	 *            scidb statement
	 * @throws SQLException
	 */
	public void executeStatementAFL(String stringStatement)
			throws SQLException {
		executeStatementSciDB(stringStatement, Lang.AFL);
	}

	/**
	 * This statement will be executed via jdbc in AQL language.
	 * 
	 * @param stringStatement
	 *            scidb statement
	 * @throws SQLException
	 */
	public void executeStatementAQL(String stringStatement) throws SQLException {
		executeStatementSciDB(stringStatement, Lang.AQL);
	}

//	/**
//	 * Check if the current object has a connectionInfo and open the connection
//	 * based on the connectionInfo (if not connectionInfo then throw an
//	 * exception).
//	 * 
//	 * @return connection to SciDB
//	 * @throws SQLException
//	 */
//	@Deprecated
//	public Connection getConnection() throws SQLException {
////		if (connection == null) {
////			if (conInfo == null) {
////				throw new IllegalStateException(
////						"Unkonwn information about connection to SciDB.");
////			}
////			connection = getConnection(conInfo);
////		}
////		return connection;
//		return null;
//	}

	/**
	 * Execute the statement in SciDB in the given language (AFL or AQL).
	 * 
	 * @param stringStatement
	 * @param lang
	 * @throws SQLException
	 */
	private void executeStatementSciDB(String stringStatement, Lang lang)
			throws SQLException {
		Connection connection = null; 
		Statement statement = null;
		try {
			connection = getConnection(this.conInfo);
			statement = connection.createStatement();
			IStatementWrapper statementWrapper = statement
					.unwrap(IStatementWrapper.class);
			if (lang == Lang.AFL) {
				statementWrapper.setAfl(true);
			}
			log.debug("Statement to be executed in SciDB: " + stringStatement);
			statement.execute(stringStatement);
			connection.commit();
		} catch (SQLException ex) {
			ex.printStackTrace();
			// remove ' from the statement - otherwise it won't be inserted into
			// log table in Postgres
			log.error(ex.getMessage() + "; statement to be executed: "
					+ LogUtils.replace(stringStatement) + " " + " "
					+ StackTrace.getFullStackTrace(ex), ex);
			throw ex;
		} finally {
			closeStatement(statement);
			closeConnection(connection);
		}
	}

	/**
	 * Check if the array with the given name exists in SciDB.
	 * 
	 * @param arrayName
	 *            The name of the array.
	 * @return True, if the array exists, false, otherwise.
	 * @throws SQLException
	 */
	public static boolean existsArray(ConnectionInfo conTo, String arrayName)
			throws SQLException {
		Connection con = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			con = SciDBHandler.getConnection(conTo);
			statement = con.createStatement();
			String statementString = "select name from list('arrays') where name = '"
					+ arrayName + "'";
			log.debug("Statement to be executed in SciDB: " + statementString);
			rs = statement.executeQuery(statementString);
			con.commit();
			if (rs == null || rs.isAfterLast()) {
				return false;
			}
			return true;
		} catch (ArrayIndexOutOfBoundsException ex) {
			log.error(
					"SciDB idiosyncrasy: it returns the java array out of bound "
							+ "exception when the SciDB array does not exist!");
			return false;
		} catch (SQLException ex) {
			String msg = " Problem with a connection or query to SciDB. "
					+ ex.getMessage();
			if (ex.getCause()
					.getCause() instanceof ArrayIndexOutOfBoundsException) {
				log.error(
						"SciDB idiosyncrasy: it returns the java array out of bound "
								+ "exception when the SciDB array does not exist!");
				return false;
			}
			log.error(msg + StackTrace.getFullStackTrace(ex), ex);
			throw ex;
		} finally {
			closeResultSet(rs);
			closeStatement(statement);
			closeConnection(con);
		}

	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#existsObject(java.lang.String)
	 */
	@Override
	public boolean existsObject(String name) throws Exception {
		if (this.conInfo == null) {
			throw new IllegalStateException(
					"SciDB Handler was not initialized with "
							+ "a connection information.");
		}
		return existsArray(conInfo, name);
	}
	
	@Override
	public void dropDataSetIfExists(String array) throws SQLException {
		dropArrayIfExists(this.conInfo, array);
	}

	/**
	 * This is similar to dropArrayIfExists. The array is removed if it exists.
	 * Otherwise, no exception is thrown.
	 * 
	 * @throws SQLException
	 */
	public static void dropArrayIfExists(ConnectionInfo conTo, String array)
			throws SQLException {
		Connection con = null;
		Statement statement = null;
		try {
			con = SciDBHandler.getConnection(conTo);
			statement = con.createStatement();
			String statementString = "drop array " + array;
			log.debug("Statement to be executed in SciDB: " + statementString);
			statement.execute(statementString);
			con.commit();
		} catch (SQLException ex) {
			/*
			 * it can be thrown when the target array did not exists which
			 * should be a default behavior';
			 */
			if (ex.getMessage()
					.contains("Array '" + array + "' does not exist.")) {
				/* the array did not exist in the SciDB database */
				return;
			} else {
				throw ex;
			}
		} finally {
			try {
				closeStatement(statement);
			} catch (SQLException ex) {
				log.error("Could not close open statement for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeConnection(con);
			} catch (SQLException ex) {
				log.error("Could not close open connection for SciDB. "
						+ ex.getMessage());
			}
		}
		
	}




	/**
	 * For generating XML query plans, which is used to construct Operator tree,
	 * which will be used for determining equivalences.
	 * 
	 * @param query
	 *            Query string for SciDB.
	 * @return the String of tree plan generated by SciDB
	 */
	public String generateSciDBLogicalPlan(String query) {
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			connection = getConnection(conInfo);
			statement = connection.createStatement();

			// this unwraps to afl
			IStatementWrapper stWrapper = statement
					.unwrap(IStatementWrapper.class);
			stWrapper.setAfl(true);

			// System.out.printf("---> query before: %s\n---> query after : %s",
			// query, query.replaceAll("'", "\\\\'").replaceAll(";", ""));

			resultSet = statement.executeQuery("explain_logical('"
					+ query.replaceAll("'", "\\\\'").replaceAll(";", "")
					+ "', 'afl')");
			return resultSet.getString("logical_plan");

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try {
				closeResultSet(resultSet);
			} catch (SQLException ex) {
				log.error("Could not close open result set for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeStatement(statement);
			} catch (SQLException ex) {
				log.error("Could not close open statement for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeConnection(connection);
			} catch (SQLException ex) {
				log.error("Could not close open connection for SciDB. "
						+ ex.getMessage());
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#getShim()
	 */
	@Override
	public Shim getShim() {
		return BDConstants.Shim.PSQLARRAY;
	}

	/**
	 * Wrapper for execute. Create the handler only when needed and destroy it
	 * after the operation.
	 * 
	 * @param conTo
	 *            connection info for SciDB
	 * @param query
	 *            query to be executed
	 * @return
	 * @throws SQLException
	 * @throws LocalQueryExecutionException
	 */
	public static Optional<QueryResult> execute(ConnectionInfo conTo,
			String query) throws SQLException, LocalQueryExecutionException {
		SciDBHandler handler = new SciDBHandler(conTo);
		return handler.execute(query);
	}

	public Optional<QueryResult> execute(String query)
			throws LocalQueryExecutionException {
		Connection connection = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			connection = getConnection(this.conInfo);
			
			try {
				st = connection.createStatement();
				IStatementWrapper statementWrapper = st
						.unwrap(IStatementWrapper.class);
				statementWrapper.setAfl(true);

				log.debug("query: " + LogUtils.replace(query) + "");
				log.debug("ConnectionInfo: " + this.conInfo.toString() + "\n");

				st.setQueryTimeout(30);
				boolean ret = st.execute(query);
				connection.commit();
				if (ret) {
					try {
						rs = st.getResultSet();
						return Optional
								.of(new JdbcQueryResult(rs, this.conInfo));
					} catch (ArrayIndexOutOfBoundsException e) {
						List<List<String>> results = new ArrayList<>();
						results.add(new ArrayList<>());
						return Optional.of(new ConstructedQueryResult(results,
								this.conInfo));
					}
				} else {
					return Optional.empty();
				}
			} catch (SQLException ex) {
				log.error(
						ex.getMessage() + "; query: " + LogUtils.replace(query),
						ex);
				throw new LocalQueryExecutionException(ex);
			}
		} catch (SQLException ex) {
			log.error(ex.getMessage()
					+ "Could not open connection to SciDB. ; query: "
					+ LogUtils.replace(query), ex);
			throw new LocalQueryExecutionException(ex);
		} finally {
			try {
				closeResultSet(rs);
			} catch (SQLException ex) {
				log.error("Could not close open result set for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeStatement(st);
			} catch (SQLException ex) {
				log.error("Could not close open statement for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeConnection(connection);
			} catch (SQLException ex) {
				log.error("Could not close open connection for SciDB. "
						+ ex.getMessage());
			}
		}
	}

	/**
	 * Execute query in SciDB using command line iquery;
	 * 
	 * @param queryString
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws SciDBException
	 */
	private String executeQueryScidb(String queryString)
			throws IOException, InterruptedException, SciDBException {

		long lStartTime = System.nanoTime();
		String resultString = getDataFromSciDB(queryString);
		log.info("SciDB query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",");

		lStartTime = System.nanoTime();
		Tuple2<List<String>, List<List<String>>> parsedData = ParseSciDBResponse
				.parse(resultString);
		List<String> colNames = parsedData.getT1();
		List<List<String>> tuples = parsedData.getT2();

		QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
				tuples, 1, 1, colNames, new ArrayList<String>(),
				new Timestamp(0));

		log.info("Parsing data time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000);

		lStartTime = System.nanoTime();
		String responseResult = ObjectMapperResource.INSTANCE.getObjectMapper()
				.writeValueAsString(resp);

		log.info("JSON formatting time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000);

		return responseResult;
	}

	/**
	 * Get data from SciDB by running an iquery from a command line.
	 * 
	 * @param queryString
	 * @param host
	 * @param port
	 * @param binPath
	 * @return the results in a form of a String
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws SciDBException
	 */
	private String getDataFromSciDB(final String queryString)
			throws IOException, InterruptedException, SciDBException {
		InputStream resultInStream = RunShell.runSciDBAFLquery(
				conInfo.getHost(), conInfo.getPort(), conInfo.getBinPath(),
				queryString);
		String resultString = IOUtils.toString(resultInStream,
				Constants.ENCODING);
		return resultString;
	}

	/**
	 * Returns the meta data of the array for each column/attribute in the
	 * array.
	 * 
	 * @param arrayName
	 *            The arrayName string.
	 * @return Mapping column/attribute name to its meta-data.
	 * @throws SQLException
	 * @throws NoTargetArrayException
	 */
	public SciDBArrayMetaData getObjectMetaData(String arrayName)
			throws Exception {
		Map<String, AttributeMetaData> dimensionsMap = new HashMap<>();
		List<AttributeMetaData> dimensionsOrdered = new ArrayList<>();
		Map<String, AttributeMetaData> attributesMap = new HashMap<>();
		List<AttributeMetaData> attributesOrdered = new ArrayList<>();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSetDimensions = null;
		ResultSet resultSetAttributes = null;
		try {
			connection = getConnection(this.conInfo);
			statement = connection.createStatement();
			/*
			 * query which fetches dimensions of the array with name arrayName
			 */
			String getDimsQuery = "select * from dimensions(" + arrayName + ")";
			log.debug("Get dimensions query to SciDB: " + getDimsQuery);
			resultSetDimensions = statement.executeQuery(getDimsQuery);
			connection.commit();
			closeStatement(statement);
			statement = connection.createStatement();
			while (!resultSetDimensions.isAfterLast()) {
				AttributeMetaData columnMetaData = new AttributeMetaData(
						resultSetDimensions.getString(2),
						FromSciDBToSQLTypes.getSQLTypeFromSciDBType(
								resultSetDimensions.getString(9)),
						resultSetDimensions.getString(9), false, true);
				dimensionsMap.put(resultSetDimensions.getString(2),
						columnMetaData);
				dimensionsOrdered.add(columnMetaData);
				resultSetDimensions.next();
			}
			
			String getAttrQuery = "select * from attributes(" + arrayName + ")";
			log.debug(getAttrQuery);
			resultSetAttributes = statement.executeQuery(getAttrQuery);
			connection.commit();
			while (!resultSetAttributes.isAfterLast()) {
				AttributeMetaData columnMetaData = new AttributeMetaData(
						resultSetAttributes.getString(2),
						FromSciDBToSQLTypes.getSQLTypeFromSciDBType(
								resultSetAttributes.getString(3)),
						resultSetAttributes.getString(3),
						resultSetAttributes.getBoolean(4), false);
				attributesMap.put(resultSetAttributes.getString(2),
						columnMetaData);
				attributesOrdered.add(columnMetaData);
				resultSetAttributes.next();
			}
			return new SciDBArrayMetaData(arrayName, dimensionsMap,
					dimensionsOrdered, attributesMap, attributesOrdered);
		} catch (SQLException ex) {
			String message = "Array '" + arrayName + "' does not exist.";
			if (ex.getMessage().contains(message)) {
				throw new NoTargetArrayException(message, ex);
			}
			ex.printStackTrace();
			log.error("Error while trying to get meta data from SciDB. "
					+ ex.getMessage() + " " + StackTrace.getFullStackTrace(ex),
					ex);
			throw ex;
		} finally {
			try {
				closeResultSet(resultSetDimensions);
			} catch (SQLException ex) {
				log.error("Could not close open result set for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeResultSet(resultSetAttributes);
			} catch (SQLException ex) {
				log.error("Could not close open result set for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeStatement(statement);
			} catch (SQLException ex) {
				log.error("Could not close open statement for SciDB. "
						+ ex.getMessage());
			}
			try {
				closeConnection(connection);
			} catch (SQLException ex) {
				log.error("Could not close open statement for SciDB. "
						+ ex.getMessage());
			}
		}
	}

	/**
	 * Close the result set.
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	private static void closeResultSet(ResultSet resultSet)
			throws SQLException {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
				log.error(
						"Error while trying to close result set (from a query) in SciDB."
								+ ex.getMessage() + " "
								+ StackTrace.getFullStackTrace(ex),
						ex);
				throw ex;
			}
		}
	}

	/**
	 * Close the statement.
	 * 
	 * @param statement
	 * @throws SQLException
	 */
	private static void closeStatement(Statement statement)
			throws SQLException {
		if (statement != null && !statement.isClosed()) {
			try {
				statement.close();
			} catch (SQLException ex) {
				log.error(
						"Error while trying to close the AQL statement from SciDB."
								+ ex.getMessage() + " "
								+ StackTrace.getFullStackTrace(ex),
						ex);
				throw ex;
			}
		}
	}

	/**
	 * Close connection to SciDB.
	 * 
	 * @param connection
	 *            Object representing connection to SciDB.
	 * @throws SQLException
	 */
	private static void closeConnection(Connection connection)
			throws SQLException {
		if (connection != null && !connection.isClosed()) {
			try {
				connection.close();
			} catch (SQLException ex) {
				log.error("Could not close a connection for SciDB. "
						+ ex.getMessage() + " "
						+ StackTrace.getFullStackTrace(ex));
				throw ex;
			}
		}
	}

	/**
	 * CSV field types pattern: : N number, S string, s nullable string, C char.
	 * For example: "NNsCS"
	 * 
	 * @param columnsMetaData
	 * @return a string of characters from the set: NnSsCc
	 * 
	 */
	public static String getTypePatternFromPostgresTypes(
			final PostgreSQLTableMetaData postgresTableMetaData) {
		final List<AttributeMetaData> columnsMetaData = postgresTableMetaData
				.getAttributesOrdered();
		char[] scidbTypesPattern = new char[columnsMetaData.size()];
		for (AttributeMetaData columnMetaData : columnsMetaData) {
			// check the character type
			char newType = 'N'; // N - numeric by default
			if ((columnMetaData.getSqlDataType().equals("character")
					|| columnMetaData.getSqlDataType().equals("char"))
					&& columnMetaData.getCharacterMaximumLength() == 1) {
				newType = 'C';
			} else if (columnMetaData.getSqlDataType().contains("varchar")
					|| columnMetaData.getSqlDataType().contains("character")
					|| columnMetaData.getSqlDataType()
							.contains("character varying")
					|| columnMetaData.getSqlDataType().equals("text")) {
				// for "string" type
				newType = 'S';
			}
			if (columnMetaData.isNullable()) {
				newType = Character.toLowerCase(newType);
			}
			/*
			 * column positions in Postgres start from 1 but were changed to
			 * start from 0
			 */
			scidbTypesPattern[columnMetaData.getPosition()] = newType;
		}
		return String.copyValueOf(scidbTypesPattern);
	}


	@Override
	public boolean isCsvExportHeader() {
		return IS_CSV_EXPORT_HEADER;
	}

	@Override
	public String getCsvExportDelimiter() {
		return CSV_EXPORT_DELIMITER;
	}

	public static String getGeneralCsvExportDelimiter() {
		return CSV_EXPORT_DELIMITER;
	}

	public static boolean getIsCsvExportHeader() {
		return IS_CSV_EXPORT_HEADER;
	}

	public static boolean getIsCsvLoadHeader() {
		return IS_CSV_LOAD_HEADER;
	}

	/**
	 * Set the meta data for the SciDB array.
	 * 
	 * @param connectionInfo
	 *            Information about the connection to SciDB.
	 * @param array
	 *            The name of the array in SciDB.
	 * 
	 * @throws Exception
	 *             When could not connect to SciDB or extraction of the meta
	 *             data from SciDB failed.
	 */
	public static SciDBArrayMetaData getArrayMetaData(
			ConnectionInfo connectionInfo, String array) throws Exception {
		String debugMessage = " ConnectionInfo: " + connectionInfo.toString()
				+ " Array: " + array;
		try {
			SciDBHandler handler = new SciDBHandler(connectionInfo);
			try {
				return handler.getObjectMetaData(array);
			} catch (Exception e) {
				String message = e.getMessage()
						+ " Extraction of meta data on the array: " + array
						+ " in SciDB failed. " + debugMessage;
				log.error(message + StackTrace.getFullStackTrace(e));
				throw new Exception(message, e);
			} finally {
//				handler.close();
			}
		} catch (SQLException scidbException) {
			String msg = scidbException.getMessage()
					+ " Could not connect to SciDB. " + debugMessage;
			throw new Exception(msg, scidbException);
		}
	}

	/**
	 * Create a flat array in SciDB from the meta info about the
	 * multidimensional array.
	 * 
	 * We migrate data from SciDB so we use the connection from.
	 * 
	 * @throws Exception
	 * 
	 * @throws UnsupportedTypeException
	 * @throws MigrationException
	 */
	public static void createFlatArrayFromMultiDimArray(
			ConnectionInfo connectionFrom, String array, String flatArrayName)
					throws Exception {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + flatArrayName + " <");
		List<AttributeMetaData> scidbColumnsOrdered = new ArrayList<AttributeMetaData>();
		SciDBArrayMetaData scidbArrayMetaData;
		scidbArrayMetaData = SciDBHandler.getArrayMetaData(connectionFrom,
				array);
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getDimensionsOrdered());
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getAttributesOrdered());
		for (AttributeMetaData column : scidbColumnsOrdered) {
			String attributeName = column.getName();
			String attributeType = column.getSqlDataType();
			String attributeNULL = "";
			if (column.isNullable()) {
				attributeNULL = " NULL";
			}
			createArrayStringBuf.append(
					attributeName + ":" + attributeType + attributeNULL + ",");
		}

		/* delete the last comma "," */
		createArrayStringBuf.deleteCharAt(createArrayStringBuf.length() - 1);
		/* " r_regionkey:int64,r_name:string,r_comment:string> );" */
		/* this is by default 1 mln cells in a chunk */
		createArrayStringBuf
				.append("> [_flat_dimension_=0:*," + Long.MAX_VALUE + ",0]");
		SciDBHandler handler = new SciDBHandler(connectionFrom);
		handler.executeStatementAQL(createArrayStringBuf.toString());
//		handler.commit();
//		handler.close();
	}



	// /**
	// * @param args
	// */
	// public static void main(String[] args) {
	// try {
	// // String resultSciDB = new
	// // new SciDBHandler().executeQueryScidb("list(^^arrays^^)");
	// // new SciDBHandler().executeStatement("drop array adam2");
	// SciDBHandler handler = new SciDBHandler();
	//// handler.executeStatement(
	//// "create array adam2<v:string> [i=0:10,1,0]");
	// handler.close();
	// // String resultSciDB = new
	// // SciDBHandler().executeQueryScidb("scan(waveform_test_1GB)");
	// // System.out.println(resultSciDB);
	// // } catch (IOException | InterruptedException | SciDBException e) {
	// // e.printStackTrace();
	// } catch (SQLException e) {
	// e.printStackTrace();
	// }
	//
	// }

}
