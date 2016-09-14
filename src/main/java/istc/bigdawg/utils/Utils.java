/**
 * 
 */
package istc.bigdawg.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.scidb.jdbc.IResultSetWrapper;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.executor.JdbcQueryResult;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Feb 24, 2016 7:36:20 PM
 */
public class Utils {

	/* log */
	private static Logger log = Logger.getLogger(Utils.class);

	/**
	 * Show meta data about the target array.
	 * 
	 * @param resultSet
	 * @throws SQLException
	 */
	private static void showMetaData(ResultSet resultSet) throws SQLException {
		ResultSetMetaData meta = resultSet.getMetaData();
		System.out.println("Source array name: " + meta.getTableName(0));
		System.out.println(meta.getColumnCount() + " columns:");

		IResultSetWrapper resWrapper = resultSet
				.unwrap(IResultSetWrapper.class);
		for (int i = 1; i <= meta.getColumnCount(); i++) {
			System.out.println(meta.getColumnName(i) + " - "
					+ meta.getColumnTypeName(i) + " - is attribute:"
					+ resWrapper.isColumnAttribute(i));
		}
	}

	/**
	 * Get number of cells for a given array in SciDB.
	 * 
	 * @param conTo
	 *            information about the connection to SciDB
	 * @param array
	 *            the name of the array in SciDB
	 * @return number of cells in the given array in SciDB.
	 * @throws SQLException
	 */
	public static long getNumberOfCellsSciDB(ConnectionInfo conTo, String array)
			throws SQLException {
		Connection con = null;
		Statement query = null;
		ResultSet resultSet = null;

		try {
			con = SciDBHandler.getConnection(conTo);
			query = con.createStatement();
			resultSet = query.executeQuery("select * from " + array);

			showMetaData(resultSet);
			long numberOfCellsSciDB = 0;
			while (!resultSet.isAfterLast()) {
				// System.out.println(resultSet.getString(3));
				// System.out.println(resultSet.getInt(1));
				++numberOfCellsSciDB;
				resultSet.next();
			}
			return numberOfCellsSciDB;
		} catch (SQLException ex) {
			log.error(ex.getMessage() + StackTrace.getFullStackTrace(ex));
			throw ex;
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					log.error(e.getMessage() + StackTrace.getFullStackTrace(e));
				}
			}
			if (query != null) {
				try {
					query.close();
				} catch (SQLException e) {
					log.error(e.getMessage() + StackTrace.getFullStackTrace(e));
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					log.error(e.getMessage() + StackTrace.getFullStackTrace(e));
				}
			}
		}

	}

	/**
	 * Get number of tuples in the table in the PostgreSQL instance identified
	 * by con.
	 * 
	 * @param con
	 *            connection to PostgreSQL
	 * @param table
	 *            a table in PostgreSQL
	 * @return number of tuples in the table
	 * @throws SQLException
	 */
	public static long getPostgreSQLCountTuples(PostgreSQLConnectionInfo con,
			String table) throws SQLException {
		PostgreSQLHandler handler = new PostgreSQLHandler(con);
		JdbcQueryResult qr = handler
				.executeQueryPostgreSQL("select count(*) from " + table);
		return Long.valueOf(qr.getRows().get(0).get(0));
	}

	public static void main(String[] args) throws SQLException {
		LoggerSetup.setLogging();
		String array = "waveform_test";
		long numberOfCells = getNumberOfCellsSciDB(new SciDBConnectionInfo(),
				array);
		log.debug("Number of cells in array " + array + ": " + numberOfCells);
	}

}
