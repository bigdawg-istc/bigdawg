/**
 * 
 */
package istc.bigdawg.migration;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class FromSciDBToPostgres implements FromDatabaseToDatabase {

	private static Logger log = Logger.getLogger(FromSciDBToPostgres.class);

	/**
	 * 
	 */
	public FromSciDBToPostgres() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return {@link MigrationResult} with information about the migration
	 *         process
	 * @throws Exception
	 * 
	 */
	public MigrationResult migrateSingleThreadCSV(SciDBConnectionInfo connectionFrom, String fromArray,
			PostgreSQLConnectionInfo connectionTo, String toTable) throws Exception {
		long startTimeMigration = System.currentTimeMillis();
		SciDBHandler scidbHandler = new SciDBHandler(connectionFrom);
		scidbHandler.executeStatement(" ");
		
		long endTimeMigration = System.currentTimeMillis();
		String message = "migration from SciDB to PostgreSQL execution time: "+(endTimeMigration-startTimeMigration);
		log.debug(message);
		return new MigrationResult(0L, 0L);
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		FromSciDBToPostgres migrator = new FromSciDBToPostgres();
		SciDBConnectionInfo conFrom = new SciDBConnectionInfo("localhost", "1239", "scidb", "mypassw",
				"/opt/scidb/14.12/bin/");
		String arrayFrom = "region";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo("localhost", "5431", "tpch", "postgres",
				"test");
		String tableTo = "region";
		migrator.migrate(conFrom, arrayFrom, conTo, tableTo);
	}

	@Override
	public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
			String objectTo) throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof SciDBConnectionInfo && connectionTo instanceof PostgreSQLConnectionInfo) {
			try {
				return this.migrate((SciDBConnectionInfo) connectionFrom, objectFrom,
						(PostgreSQLConnectionInfo) connectionTo, objectTo);
			} catch (Exception e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;

	}

}
