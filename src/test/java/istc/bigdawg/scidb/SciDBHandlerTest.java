/**
 * 
 */
package istc.bigdawg.scidb;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import istc.bigdawg.exceptions.NoTargetArrayException;

/**
 * Test the operation executed in SciDB.
 * 
 * @author Adam Dziedzic
 * 
 *
 */
public class SciDBHandlerTest {

	@Test
	/**
	 * Test execute statement based on create/delete an array in SciDB.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void testExecuteStatement()
			throws SQLException, ClassNotFoundException {
		String arrayName = "adam_test_scidb_011";
		// if the test fails, first do in iquery for SciDB: drop array
		// adam_test_scidb_011;
		SciDBHandler handler = null;
		try {
			handler = new SciDBHandler();
			handler.executeStatement(
					"create array " + arrayName + "<v:string> [i=0:10,1,0]");
			handler.close();

		} finally {
			handler = new SciDBHandler();
			handler.executeStatement("drop array " + arrayName);
			handler.close();
		}
	}

	@Test
	/**
	 * Check if the command in SciDB can be executed in the AFL language.
	 * 
	 * @throws SQLException
	 */
	public void testExecuteStatementAFL() throws SQLException {
		SciDBHandler handler = new SciDBHandler();
		handler.executeStatementAFL("list('arrays')");
		handler.close();
	}

	@Test
	public void testQueryScidbJDBCStandardConnectionPrintArrays()
			throws Exception {
		String arrayName = "adam_test_scidb_011_3";
		// if the test fails, first do in iquery for SciDB: drop array
		// adam_test_scidb_011;
		SciDBHandler handler = new SciDBHandler();
		handler.executeStatement(
				"create array " + arrayName + "<v:string> [i=0:10,1,0]");
		handler.close();
		handler = new SciDBHandler();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			// use standard connection info from the configuration file (not
			// from the catalog)
			connection = SciDBHandler.getConnection(new SciDBConnectionInfo());
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select * from list('arrays')");
			while (!resultSet.isAfterLast()) {
				System.out.println(resultSet.getString(2));
				resultSet.next();
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
		handler = new SciDBHandler();
		handler.executeStatement("drop array " + arrayName);
		handler.close();
	}

	@Test
	/**
	 * Check if we can correctly extract meta data about arrays in SciDB, for
	 * example: attributes of the arrays, dimensions of the arrays, types of
	 * attributes, names of attributes, etc.
	 * 
	 * @throws SQLException
	 * @throws NoTargetArrayException
	 */
	public void testGetArrayMetaData()
			throws SQLException, NoTargetArrayException {
		String arrayName2 = "adam_test_scidb_011_2";
		SciDBHandler handler = new SciDBHandler();
		// create array
		handler.executeStatement("create array " + arrayName2
				+ "<v:string,d:double> [i=0:10,1,0,j=0:100,1,0]");
		handler.close();
		handler = new SciDBHandler();

		SciDBArrayMetaData meta = handler.getArrayMetaData(arrayName2);

		List<SciDBColumnMetaData> dimensionsOrdered = meta
				.getDimensionsOrdered();
		assertEquals(2, dimensionsOrdered.size());

		SciDBColumnMetaData firstDimension = dimensionsOrdered.get(0);
		assertEquals("i", firstDimension.getColumnName());
		assertEquals("int64", firstDimension.getColumnType());

		SciDBColumnMetaData secondDimension = dimensionsOrdered.get(1);
		assertEquals("j", secondDimension.getColumnName());
		assertEquals("int64", secondDimension.getColumnType());

		Map<String, SciDBColumnMetaData> dimensionsMap = meta
				.getDimensionsMap();
		assertEquals(2, dimensionsMap.size());

		SciDBColumnMetaData firstDimensionMap = dimensionsMap.get("i");
		assertEquals("i", firstDimensionMap.getColumnName());
		assertEquals("int64", firstDimensionMap.getColumnType());

		SciDBColumnMetaData secondDimensionMap = dimensionsMap.get("j");
		assertEquals("j", secondDimensionMap.getColumnName());
		assertEquals("int64", secondDimensionMap.getColumnType());

		List<SciDBColumnMetaData> attributesOrdered = meta
				.getAttributesOrdered();
		assertEquals(2, attributesOrdered.size());

		SciDBColumnMetaData firstAttribute = attributesOrdered.get(0);
		assertEquals("v", firstAttribute.getColumnName());
		assertEquals("string", firstAttribute.getColumnType());

		SciDBColumnMetaData secondAttribute = attributesOrdered.get(1);
		assertEquals("d", secondAttribute.getColumnName());
		assertEquals("double", secondAttribute.getColumnType());

		Map<String, SciDBColumnMetaData> attributesMap = meta
				.getAttributesMap();
		assertEquals(2, attributesMap.size());

		SciDBColumnMetaData firstAttributeMap = attributesMap.get("v");
		assertEquals("v", firstAttributeMap.getColumnName());
		assertEquals("string", firstAttributeMap.getColumnType());

		SciDBColumnMetaData secondAttributeMap = attributesMap.get("d");
		assertEquals("d", secondAttributeMap.getColumnName());
		assertEquals("double", secondAttributeMap.getColumnType());

		handler.close();

		handler = new SciDBHandler();
		handler.executeStatement("drop array " + arrayName2);
		handler.close();
	}

}
