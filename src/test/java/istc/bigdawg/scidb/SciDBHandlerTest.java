/**
 * 
 */
package istc.bigdawg.scidb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class SciDBHandlerTest {

	/**
	 * 
	 */
	public SciDBHandlerTest() {
		// TODO Auto-generated constructor stub
	}

	@Test
	/**
	 * Test execute statement based on create/delete an array in SciDB.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void testExecuteStatement() throws SQLException, ClassNotFoundException {
		// if the test fails, first do in iquery for SciDB: drop array
		// adam_test_scidb_011;
		SciDBHandler handler = new SciDBHandler();
		String arrayName = "adam_test_scidb_011";
		handler.executeStatement("create array " + arrayName + "<v:string> [i=0:10,1,0]");
		handler.close();

		handler = new SciDBHandler();
		handler.executeStatement("drop array " + arrayName);
		handler.close();
	}

	@Test
	public void testQueryScidbJDBCStandardConnectionPrintArrays() throws Exception {
		SciDBHandler handler = new SciDBHandler();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			// use standard connection info from the configuration file (not
			// from the catalog)
			connection = SciDBHandler.getConnection(new SciDBConnectionInfo());
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select * from list('arrays')");
			while (resultSet.next()) {
				System.out.println(resultSet.getString(2));
			}
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
			if (connection != null) {
				connection.close();
			}
			if (handler != null) {
				handler.close();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
