/**
 * 
 */
package istc.bigdawg.migration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfoTest;

/**
 * Test migration from PostgreSQL to Accumulo.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToAccumuloTest {

	private static Logger logger = Logger
			.getLogger(FromAccumuloToPostgresTest.class);

	private static final String TABLE = "table_test_postgres_accumulo";
	private static final String fromTable = TABLE;
	private static final String toTable = TABLE;

	private PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfoTest();
	private long numberOfRowsPostgres = 0;
	private long numberOfColsPostgres = 0;

	/**
	 * @throws IOException
	 * @throws SQLException
	 * 
	 */

	public FromPostgresToAccumuloTest() throws SQLException, IOException {
		LoggerSetup.setLogging();
	}

	@Before
	public void preapreDataPostgres() {
		try {
			TestMigrationUtils.RowColNumber rowColNumber = TestMigrationUtils
					.loadDataToPostgresRegionTPCH(conFrom, fromTable);
			this.numberOfRowsPostgres = rowColNumber.getRowNumber();
			this.numberOfColsPostgres = rowColNumber.getColNumber();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Test
	public void fromPostgresToAccumuloMigratorTest() throws Exception {
		logger.debug("Test migration from PostgreSQL to Accumulo "
				+ "through the Migrator.");
		preapreDataPostgres();
		AccumuloConnectionInfo conTo = AccumuloInstance.getDefaultConnection();
		AccumuloTest.recreateTable(conTo, toTable);
		Migrator.migrate(conFrom, TABLE, conTo, TABLE);
		AccumuloInstance acc = AccumuloInstance.getFullInstance(conTo);
		long countAccumulo = acc.countRows(toTable);
		assertEquals(numberOfRowsPostgres * numberOfColsPostgres,
				countAccumulo);
	}

}
