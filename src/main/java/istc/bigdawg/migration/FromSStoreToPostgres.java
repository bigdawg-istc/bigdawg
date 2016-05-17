package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;

public class FromSStoreToPostgres 
	implements FromDatabaseToDatabase, MigrationNetworkRequest {

    private static final long serialVersionUID = 1L;
    
    
    private static Logger log = Logger.getLogger(FromSStoreToPostgres.class);

    private SStoreSQLConnectionInfo connectionFrom;
    private String fromTable;
    private PostgreSQLConnectionInfo connectionTo;
    private String toTable;


    @Override
    public MigrationResult execute() throws MigrationException {
	if (this.connectionFrom == null || this.fromTable == null
		|| this.connectionTo == null || this.toTable == null) {
	throw new MigrationException("The object was not initialized");
        }
        FromSStoreToPostgresImplementation migrator = new FromSStoreToPostgresImplementation(
        		connectionFrom, fromTable, connectionTo, toTable);
        return migrator.migrate();
//        return migrator.migrateBin();
    }

    @Override
    public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
	    String objectTo) throws MigrationException {
	log.debug("General data migration: " + this.getClass().getName());
	if (connectionFrom instanceof SStoreSQLConnectionInfo
			&& connectionTo instanceof PostgreSQLConnectionInfo) {
		this.connectionFrom = (SStoreSQLConnectionInfo) connectionFrom;
		this.fromTable = objectFrom;
		this.connectionTo = (PostgreSQLConnectionInfo) connectionTo;
		this.toTable = objectTo;
		try {
			/*
			 * check if the address is not a local host
			 */
			String hostname = connectionFrom.getHost();
			log.debug("SStore hostname: " + hostname);
			if (!isThisMyIpAddress(InetAddress.getByName(hostname))) {
				log.debug("Migration will be executed remotely.");
				Object result = NetworkOut.send(this, hostname);
				return processResult(result);
			}
			/* execute the migration locally */
			log.debug("Migration will be executed locally.");
			return execute();
		} catch (Exception e) {
			throw new MigrationException(e.getMessage(), e);
		}
	}
	return null;
    }
    
    /**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		LoggerSetup.setLogging();
		FromSStoreToPostgres migrator = new FromSStoreToPostgres();
		SStoreSQLConnectionInfo conFrom = new SStoreSQLConnectionInfo("localhost",
				"21212", "", "user", "password");
		String tableFrom = "dimtrade";
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5432", "test_db", "pguser", "test");
		String tableTo = "dimtrade1";
		migrator.migrate(conFrom, tableFrom, conTo, tableTo);
//		System.out.println(result);
	}

}
