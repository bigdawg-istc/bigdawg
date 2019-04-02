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

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.utils.StackTrace;

/**
 * Test the operations executed in SciDB.
 *
 * @author Adam Dziedzic
 */
public class SciDBHandlerTest {

	private static Logger log = Logger.getLogger(SciDBHandlerTest.class);

	private SciDBConnectionInfo conTo = new SciDBConnectionInfoTest();

	@Before
	public void init() {
		LoggerSetup.setLogging();
	}

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
			handler.executeStatementAQL(
					"create array " + arrayName + "<v:string> [i=0:10,1,0]");
	//		handler.close();
		} finally {
			handler = new SciDBHandler();
			handler.executeStatementAQL("drop array " + arrayName);
	//		handler.close();
		}
	}

	@Test
	public void testExistsArray() throws SQLException {
		String arrayName = "adam_test_scidb_011_exists";
		//String arrayName = "test";
		SciDBHandler.dropArrayIfExists(conTo, arrayName);

		boolean existsFalse = SciDBHandler.existsArray(conTo, arrayName);
		assertEquals(false, existsFalse);
		try {
			SciDBHandler handler = new SciDBHandler();
			handler.executeStatementAQL(
					"create array " + arrayName + "<v:string> [i=0:10,1,0]");
	//		handler.close();

			boolean existsTrue = SciDBHandler.existsArray(conTo, arrayName);
			assertEquals(true, existsTrue);
		} finally {
			SciDBHandler.dropArrayIfExists(conTo, arrayName);
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
//		handler.close();
	}

	@Test
	public void testQueryScidbJDBCStandardConnectionPrintArrays()
			throws Exception {
		String arrayName = "adam_test_scidb_011_3";
		// if the test fails, first do in iquery for SciDB: drop array
		// adam_test_scidb_011;
		SciDBHandler handler = new SciDBHandler();
		handler.executeStatementAQL(
				"create array " + arrayName + "<v:string> [i=0:10,1,0]");
//		handler.close();
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
		//		handler.close();
			}
		}
		handler = new SciDBHandler();
		handler.executeStatementAQL("drop array " + arrayName);
//		handler.close();
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
			throws SQLException, NoTargetArrayException, MigrationException {
		String arrayName2 = "adam_test_scidb_011_2";
		SciDBHandler handler = new SciDBHandler();
		// create array
		handler.executeStatementAQL("drop array " + arrayName2
				+ "");
		handler.executeStatementAQL("create array " + arrayName2
				+ "<v:string,d:double> [i=0:10,1,0,j=0:100,1,0]");
//		handler.close();
		handler = new SciDBHandler();
		SciDBArrayMetaData meta = null;
		try {
			meta = handler.getObjectMetaData(arrayName2);
		} catch (Exception e) {
			log.error(e.getMessage() + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(e.getMessage(), e);
		} finally {
	//		handler.close();
		}

		List<AttributeMetaData> dimensionsOrdered = meta.getDimensionsOrdered();
		assertEquals(2, dimensionsOrdered.size());

		AttributeMetaData firstDimension = dimensionsOrdered.get(0);
		assertEquals("i", firstDimension.getName());
		assertEquals("bigint", firstDimension.getSqlDataType());

		AttributeMetaData secondDimension = dimensionsOrdered.get(1);
		assertEquals("j", secondDimension.getName());
		assertEquals("bigint", secondDimension.getSqlDataType());

		Map<String, AttributeMetaData> dimensionsMap = meta.getDimensionsMap();
		assertEquals(2, dimensionsMap.size());

		AttributeMetaData firstDimensionMap = dimensionsMap.get("i");
		assertEquals("i", firstDimensionMap.getName());
		assertEquals("bigint", firstDimensionMap.getSqlDataType());

		AttributeMetaData secondDimensionMap = dimensionsMap.get("j");
		assertEquals("j", secondDimensionMap.getName());
		assertEquals("bigint", secondDimensionMap.getSqlDataType());

		List<AttributeMetaData> attributesOrdered = meta.getAttributesOrdered();
		assertEquals(2, attributesOrdered.size());

		AttributeMetaData firstAttribute = attributesOrdered.get(0);
		assertEquals("v", firstAttribute.getName());
		assertEquals("text", firstAttribute.getSqlDataType());

		AttributeMetaData secondAttribute = attributesOrdered.get(1);
		assertEquals("d", secondAttribute.getName());
		assertEquals("double precision", secondAttribute.getSqlDataType());

		Map<String, AttributeMetaData> attributesMap = meta.getAttributesMap();
		assertEquals(2, attributesMap.size());

		AttributeMetaData firstAttributeMap = attributesMap.get("v");
		assertEquals("v", firstAttributeMap.getName());
		assertEquals("text", firstAttributeMap.getSqlDataType());

		AttributeMetaData secondAttributeMap = attributesMap.get("d");
		assertEquals("d", secondAttributeMap.getName());
		assertEquals("double precision", secondAttributeMap.getSqlDataType());

//		handler.close();

		handler = new SciDBHandler();
		handler.executeStatementAQL("drop array " + arrayName2);
//		handler.close();
	}

}
