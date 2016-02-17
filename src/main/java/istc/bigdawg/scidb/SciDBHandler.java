/**
 * 
 */
package istc.bigdawg.scidb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleList;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.ObjectMapperResource;
import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.Tuple.Tuple2;

/**
 * @author Adam Dziedzic
 * 
 */
public class SciDBHandler implements DBHandler {

	private static Logger log = Logger.getLogger(SciDBHandler.class.getName());
	private SciDBConnectionInfo conInfo;
	private Connection connection;

	public SciDBHandler() {
		this.conInfo = new SciDBConnectionInfo();
	}

	public static Connection getConnection(SciDBConnectionInfo conInfo) throws ClassNotFoundException, SQLException {
		try {
			Class.forName("org.scidb.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.error("SciDB jdbc driver is not in the CLASSPATH -> " + e.getMessage(), e);
			throw e;
		}
		try {
			return DriverManager.getConnection(conInfo.getUrl());
		} catch (SQLException ex) {
			ex.printStackTrace();
			log.error("Could not establish a connection to a SciDB database. " + conInfo.toString() + " "
					+ ex.getMessage() + StackTrace.getFullStackTrace(ex), ex);
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
	public SciDBHandler(SciDBConnectionInfo conInfo) throws SQLException, ClassNotFoundException {
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
				connection.close();
				connection = null;
			} catch (SQLException e) {
				e.printStackTrace();
				log.error("Could not close the connection to a SciDB database. " + conInfo.toString() + " "
						+ e.getMessage() + StackTrace.getFullStackTrace(e), e);
				throw e;
			}
		}
	}

	/**
	 * This statement will be executed via jdbc.
	 * 
	 * @param statement
	 *            scidb statement
	 * @throws SQLException
	 */
	public void executeStatement(String stringStatement) throws SQLException {
		Statement statement = null;
		try {
			statement = connection.createStatement();
			statement.execute(stringStatement);
			statement.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
			// remove ' from the statement - otherwise it won't be inserted into
			// log table in Postgres
			log.error(ex.getMessage() + "; statement to be executed: " + stringStatement.replace("'", "") + " "
					+ ex.getStackTrace(), ex);
			throw ex;
		} finally {
			if (statement != null) {
				statement.close();
			}
		}
	}

	/*
	 * (non-Javadoc)
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#getShim()
	 */
	@Override
	public Shim getShim() {
		return BDConstants.Shim.PSQLARRAY;
	}

	private String executeQueryScidb(String queryString) throws IOException, InterruptedException, SciDBException {
		// String sciDBUser = BigDawgConfigProperties.INSTANCE.getScidbUser();
		// String sciDBPassword = BigDawgConfigProperties.INSTANCE
		// .getScidbPassword();
		// System.out.println("sciDBHostname: " + sciDBHostname);
		// System.out.println("sciDBUser: " + sciDBUser);
		// System.out.println("sciDBPassword: " + sciDBPassword);
		long lStartTime = System.nanoTime();
		String resultString = getDataFromSciDB(queryString, conInfo.getHost(), conInfo.getPort(), conInfo.getBinPath());
		String messageGetData = "SciDB query execution time milliseconds: " + (System.nanoTime() - lStartTime) / 1000000
				+ ",";
		System.out.print(messageGetData);
		log.info(messageGetData);
		// System.out.println("result_string: "+resultString);

		lStartTime = System.nanoTime();
		Tuple2<List<String>, List<List<String>>> parsedData = ParseSciDBResponse.parse(resultString);
		List<String> colNames = parsedData.getT1();
		List<List<String>> tuples = parsedData.getT2();
		QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200, tuples, 1, 1, colNames,
				new ArrayList<String>(), new Timestamp(0));
		String messageParsing = "Parsing data time milliseconds: " + (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageParsing);
		log.info(messageParsing);

		lStartTime = System.nanoTime();
		String responseResult = ObjectMapperResource.INSTANCE.getObjectMapper().writeValueAsString(resp);
		String messageJSON = "JSON formatting time milliseconds: " + (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageJSON);
		log.info(messageJSON);

		return responseResult;
	}

	private String getDataFromSciDB(final String queryString, final String host, final String port,
			final String binPath) throws IOException, InterruptedException, SciDBException {
		InputStream resultInStream = RunShell.runSciDBAFLquery(host, port, binPath, queryString);
		String resultString = IOUtils.toString(resultInStream, Constants.ENCODING);
		return resultString;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			// String resultSciDB = new
			new SciDBHandler().executeQueryScidb("list(^^arrays^^)");
			// String resultSciDB = new
			// SciDBHandler().executeQueryScidb("scan(waveform_test_1GB)");
			// System.out.println(resultSciDB);
		} catch (IOException | InterruptedException | SciDBException e) {
			e.printStackTrace();
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
	public static String getTypePatternFromPostgresTypes(List<PostgreSQLColumnMetaData> columnsMetaData) {
		char[] scidbTypesPattern = new char[columnsMetaData.size()];
		for (PostgreSQLColumnMetaData columnMetaData : columnsMetaData) {
			// check the character type
			char newType = 'N'; // N - numeric by default
			if ((columnMetaData.getDataType().equals("character") || columnMetaData.getDataType().equals("char"))
					&& columnMetaData.getCharacterMaximumLength() == 1) {
				newType = 'C';
			} else if (columnMetaData.getDataType().equals("varchar")
					|| columnMetaData.getDataType().equals("character")
					|| columnMetaData.getDataType().contains("character varying")
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

}
