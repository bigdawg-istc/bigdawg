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

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.scidb.jdbc.IStatementWrapper;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
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
public class SciDBHandler implements DBHandler {

	private static Logger log = Logger.getLogger(SciDBHandler.class.getName());
	private SciDBConnectionInfo conInfo;
	private Connection connection;

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
			this.connection = getConnection(conInfo);
		} catch (Exception e) {
			log.debug("getConnection throws Exception from default SciDBHandler(); this is not supposed to happen. Check properties file.");
			e.printStackTrace();
		}
	}
	
	public SciDBHandler(int dbid) {
		try {
			this.conInfo = CatalogViewer.getSciDBConnectionInfo(dbid);
			this.connection = getConnection(conInfo);
		} catch (Exception e) {
			log.debug("getConnection throws Exception from default SciDBHandler(); this is not supposed to happen. Check properties file.");
			e.printStackTrace();
		}
	}

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
	public static Connection getConnection(SciDBConnectionInfo conInfo)
			throws SQLException {
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
	 * Create a new SciDB hander for a given connection. You have to close the
	 * handler at the end to release the resources.
	 * 
	 * @param conInfo
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public SciDBHandler(SciDBConnectionInfo conInfo) throws SQLException {
		this.conInfo = conInfo;
		this.connection = getConnection(conInfo);
	}

	/**
	 * You have to close the handler at the end to release the resources.
	 * 
	 * @throws SQLException
	 */
	public void close() throws SQLException {
		if (connection != null) {
			try {
				connection.commit();
				connection.close();
				connection = null;
			} catch (SQLException e) {
				e.printStackTrace();
				log.error("Could not close the connection to a SciDB database. "
						+ conInfo.toString() + " " + e.getMessage()
						+ StackTrace.getFullStackTrace(e), e);
				throw e;
			}
		}
	}

	/**
	 * This statement will be executed via jdbc in AFL language.
	 * 
	 * @param statement
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
	 * @param statement
	 *            scidb statement
	 * @throws SQLException
	 */
	public void executeStatement(String stringStatement) throws SQLException {
		executeStatementSciDB(stringStatement, Lang.AQL);
	}

	/**
	 * Execute the statement in SciDB in the given language (AFL or AQL).
	 * 
	 * @param stringStatement
	 * @param lang
	 * @throws SQLException
	 */
	private void executeStatementSciDB(String stringStatement, Lang lang)
			throws SQLException {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			IStatementWrapper statementWrapper = statement
					.unwrap(IStatementWrapper.class);
			if (lang == Lang.AFL) {
				statementWrapper.setAfl(true);
			}
			statement.execute(stringStatement);
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
		}
	}

	/**
	 * Commit the transaction in SciDB.
	 * 
	 * @throws SQLException
	 */
	public void commit() throws SQLException {
		try {
			connection.commit();
		} catch (SQLException ex) {
			ex.printStackTrace();
			log.error("Could not commit for a connection to a SciDB database. "
					+ conInfo.toString() + " " + ex.getMessage()
					+ StackTrace.getFullStackTrace(ex), ex);
			throw ex;
		}
	}

	/**
	 * The query executed via iquery (not JDBC).
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		System.out.println("run query for SciDB");
		System.out.println("SciDB queryString: " + queryString);
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
	 * NEW FUNCTION: this is written specifically for generating XML query
	 * plans, which is used to construct Operator tree, which will be used for
	 * determining equivalences.
	 * 
	 * @param query
	 * @return the String of tree plan generated by SciDB
	 * @throws Exception
	 */
	public String generateSciDBLogicalPlan(String query) {
		Connection conn;
		try {
			
			Class.forName("org.scidb.jdbc.Driver");
			
			conn = DriverManager.getConnection("jdbc:scidb://"+this.conInfo.getHost()+":"+this.conInfo.getPort()+"/");
			Statement st = conn.createStatement();
			
			
			// this unwraps to afl
			IStatementWrapper stWrapper = st.unwrap(IStatementWrapper.class);
			stWrapper.setAfl(true);
			
			ResultSet res = st.executeQuery("explain_logical('"+query+"', 'afl')");
			return res.getString("logical_plan");
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
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
		// String sciDBUser = BigDawgConfigProperties.INSTANCE.getScidbUser();
		// String sciDBPassword = BigDawgConfigProperties.INSTANCE
		// .getScidbPassword();
		// System.out.println("sciDBHostname: " + sciDBHostname);
		// System.out.println("sciDBUser: " + sciDBUser);
		// System.out.println("sciDBPassword: " + sciDBPassword);
		long lStartTime = System.nanoTime();
		String resultString = getDataFromSciDB(queryString, conInfo.getHost(),
				conInfo.getPort(), conInfo.getBinPath());
		String messageGetData = "SciDB query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageGetData);
		log.info(messageGetData);
		// System.out.println("result_string: "+resultString);

		lStartTime = System.nanoTime();
		Tuple2<List<String>, List<List<String>>> parsedData = ParseSciDBResponse
				.parse(resultString);
		List<String> colNames = parsedData.getT1();
		List<List<String>> tuples = parsedData.getT2();
		QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
				tuples, 1, 1, colNames, new ArrayList<String>(),
				new Timestamp(0));
		String messageParsing = "Parsing data time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageParsing);
		log.info(messageParsing);

		lStartTime = System.nanoTime();
		String responseResult = ObjectMapperResource.INSTANCE.getObjectMapper()
				.writeValueAsString(resp);
		String messageJSON = "JSON formatting time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageJSON);
		log.info(messageJSON);

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
	private String getDataFromSciDB(final String queryString, final String host,
			final String port, final String binPath)
					throws IOException, InterruptedException, SciDBException {
		InputStream resultInStream = RunShell.runSciDBAFLquery(host, port,
				binPath, queryString);
		String resultString = IOUtils.toString(resultInStream,
				Constants.ENCODING);
		return resultString;
	}

	/**
	 * Returns the meta data of the array for each column/attribute in the array
	 * 
	 * @param arrayName
	 *            the arrayName string
	 * @return map columm/attribute name to its metadata
	 * @throws SQLException
	 * @throws NoTargetArrayException
	 */
	public SciDBArrayMetaData getArrayMetaData(String arrayName)
			throws SQLException, NoTargetArrayException {
		Map<String, SciDBColumnMetaData> dimensionsMap = new HashMap<>();
		List<SciDBColumnMetaData> dimensionsOrdered = new ArrayList<>();
		Map<String, SciDBColumnMetaData> attributesMap = new HashMap<>();
		List<SciDBColumnMetaData> attributesOrdered = new ArrayList<>();
		Statement statement = connection.createStatement();
		ResultSet resultSetDimensions = null;
		ResultSet resultSetAttributes = null;
		try {
			resultSetDimensions = statement.executeQuery(
					"select * from dimensions(" + arrayName + ")");
			while (!resultSetDimensions.isAfterLast()) {
				SciDBColumnMetaData columnMetaData = new SciDBColumnMetaData(
						resultSetDimensions.getString(2),
						resultSetDimensions.getString(9),
						false);
				dimensionsMap.put(resultSetDimensions.getString(2),
						columnMetaData);
				dimensionsOrdered.add(columnMetaData);
				resultSetDimensions.next();
			}
			resultSetAttributes = statement.executeQuery(
					"select * from attributes(" + arrayName + ")");
			while (!resultSetAttributes.isAfterLast()) {
				SciDBColumnMetaData columnMetaData = new SciDBColumnMetaData(
						resultSetAttributes.getString(2),
						resultSetAttributes.getString(3),
						resultSetAttributes.getBoolean(4));
				attributesMap.put(resultSetAttributes.getString(2),
						columnMetaData);
				attributesOrdered.add(columnMetaData);
				resultSetAttributes.next();
			}
			return new SciDBArrayMetaData(arrayName, dimensionsMap, dimensionsOrdered,
					attributesMap, attributesOrdered);
		} catch (SQLException ex) {
			if (ex.getMessage()
					.contains("Array '" + arrayName + "' does not exist.")) {
				throw new NoTargetArrayException();
			}
			ex.printStackTrace();
			log.error("Error while trying to get meta data from SciDB. "
					+ ex.getMessage() + " " + StackTrace.getFullStackTrace(ex),
					ex);
			throw ex;
		} finally {
			closeResultSet(resultSetDimensions);
			closeResultSet(resultSetAttributes);
			closeStatement(statement);
		}
	}

	/**
	 * Close the result set.
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	private void closeResultSet(ResultSet resultSet) throws SQLException {
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
	private void closeStatement(Statement statement) throws SQLException {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException ex) {
				ex.printStackTrace();
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
	 * CSV field types pattern: : N number, S string, s nullable string, C char.
	 * For example: "NNsCS"
	 * 
	 * @param columnsMetaData
	 * @return a string of characters from the set: NnSsCc
	 * 
	 */
	public static String getTypePatternFromPostgresTypes(final PostgreSQLTableMetaData postgresTableMetaData ) {
		final List<PostgreSQLColumnMetaData> columnsMetaData = postgresTableMetaData.getColumnsOrdered();
		char[] scidbTypesPattern = new char[columnsMetaData.size()];
		for (PostgreSQLColumnMetaData columnMetaData : columnsMetaData) {
			// check the character type
			char newType = 'N'; // N - numeric by default
			if ((columnMetaData.getDataType().equals("character")
					|| columnMetaData.getDataType().equals("char"))
					&& columnMetaData.getCharacterMaximumLength() == 1) {
				newType = 'C';
			} else if (columnMetaData.getDataType().contains("varchar")
					|| columnMetaData.getDataType().contains("character")
					|| columnMetaData.getDataType()
							.contains("character varying")
					|| columnMetaData.getDataType().equals("text")) {
				// for "string" type
				newType = 'S';
			}
			if (columnMetaData.isNullable()) {
				newType = Character.toLowerCase(newType);
			}
			// column positions in Postgres start from 1
			scidbTypesPattern[columnMetaData.getPosition() - 1] = newType;
		}
		return String.copyValueOf(scidbTypesPattern);
	}

	/**
	 * This is similar to dropArrayIfExists. The array is removed if it exists.
	 * Otherwise, no exception is thrown.
	 * 
	 * @throws SQLException
	 */
	public static void dropArrayIfExists(SciDBConnectionInfo conTo, String array)
			throws SQLException {
		Connection con = null;
		Statement statement = null;
		try {
			con = SciDBHandler.getConnection(conTo);
			statement = con.createStatement();
			statement.execute("drop array " + array);
			con.commit();
		} catch (SQLException ex) {
			/*
			 * it can be thrown when the target array did not exists which
			 * should be a default behavior
			 */
			if (ex.getMessage()
					.contains("Array '" + array + "' does not exist.")) {
				/* the array did not exist in the SciDB database */
				return;
			} else {
				throw ex;
			}
		} finally {
			if (statement != null) {
				statement.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// String resultSciDB = new
			// new SciDBHandler().executeQueryScidb("list(^^arrays^^)");
			// new SciDBHandler().executeStatement("drop array adam2");
			SciDBHandler handler = new SciDBHandler();
			handler.executeStatement(
					"create array adam2<v:string> [i=0:10,1,0]");
			handler.close();
			// String resultSciDB = new
			// SciDBHandler().executeQueryScidb("scan(waveform_test_1GB)");
			// System.out.println(resultSciDB);
			// } catch (IOException | InterruptedException | SciDBException e) {
			// e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
