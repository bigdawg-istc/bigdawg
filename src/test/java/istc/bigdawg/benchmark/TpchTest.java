/**
 * 
 */
package istc.bigdawg.benchmark;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.direct.FromPostgresToSciDBImplementation;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfoTest;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * Test migration of the data from the TPC-H benchmark.
 * 
 * You have to make sure that all the tables from the TPC-H benchmark are
 * present in the postgresql test instance (which should be the 1st instance).
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 29, 2016 5:02:14 PM
 */
public class TpchTest {

	private static Logger log = Logger.getLogger(TpchTest.class);

	private PostgreSQLConnectionInfo conPostgres = new PostgreSQLConnectionInfoTest();
	private SciDBConnectionInfo conSciDB = new SciDBConnectionInfoTest();
//	private List<String> tables = Arrays.asList("region", "nation", "supplier",
//			"customer", "part", "partsupp", "orders", "lineitem");
	private List<String> tables = Arrays.asList("orders", "lineitem");

	@Before
	public void initTest() throws IOException {
		LoggerSetup.setLogging();
		log.debug("Init the migration of the data from the TPC-H benchmark.");
		/* change the database in PostgreSQL from test to tpch */
		conPostgres.setDatabase("tpch");
	}

	@Test
	/**
	 * 
	 * @throws MigrationException
	 * @throws SQLException
	 */
	public void fromPostgreSQLToSciDBFlat()
			throws MigrationException, SQLException {
		/* clean the scidb arrays */
		for (String table : tables) {
			SciDBHandler.dropArrayIfExists(conSciDB, table);
		}
		for (String table : tables) {
			FromPostgresToSciDBImplementation migrator = new FromPostgresToSciDBImplementation(
					conPostgres, table, conSciDB, table);
			//migrator.migrateBin();
			migrator.migrateSingleThreadCSV();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws MigrationException
	 * @throws SQLException
	 */
	public static void main(String[] args)
			throws IOException, MigrationException, SQLException {
		TpchTest test = new TpchTest();
		test.initTest();
		test.fromPostgreSQLToSciDBFlat();
	}

}
