package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;
import java.sql.*;
import java.util.Properties;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;

public class FromPostgresToSStore 
	extends FromDatabaseToDatabase implements MigrationNetworkRequest {

    private static final long serialVersionUID = 1L;
    
    
    private static Logger log = Logger.getLogger(FromPostgresToSStore.class);

    private PostgreSQLConnectionInfo connectionFrom;
    private String fromTable;
    private SStoreSQLConnectionInfo connectionTo;
    private String toTable;


    public MigrationResult execute(boolean caching) throws MigrationException {
	if (this.connectionFrom == null || this.fromTable == null
		|| this.connectionTo == null || this.toTable == null) {
	throw new MigrationException("The object was not initialized");
        }
        FromPostgresToSStoreImplementation migrator = new FromPostgresToSStoreImplementation(
        		connectionFrom, fromTable, connectionTo, toTable);
        try {
			return migrator.migrate(caching);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
    }
    
    @Override
    public MigrationResult execute() throws MigrationException {
    	return execute(false);
    }

    @Override
    public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
	    String objectTo) throws MigrationException {
    	return migrate(connectionFrom, objectFrom, connectionTo, objectTo, false);
    }
    
    public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
	    String objectTo, boolean caching) throws MigrationException {
	log.debug("General data migration: " + this.getClass().getName());
	if (connectionFrom instanceof PostgreSQLConnectionInfo
			&& connectionTo instanceof SStoreSQLConnectionInfo) {
		this.connectionFrom = (PostgreSQLConnectionInfo) connectionFrom;
		this.fromTable = objectFrom;
		this.connectionTo = (SStoreSQLConnectionInfo) connectionTo;
		this.toTable = objectTo;
		try {
			/*
			 * check if the address is not a local host
			 */
			String hostname = connectionFrom.getHost();
			log.debug("Postgres hostname: " + hostname);
			if (!isThisMyIpAddress(InetAddress.getByName(hostname))) {
				log.debug("Migration will be executed remotely.");
				Object result = NetworkOut.send(this, hostname);
//				return processResult(result);
				return (MigrationResult) result;
			}
			/* execute the migration locally */
			log.debug("Migration will be executed locally.");
			return execute(caching);
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
		FromPostgresToSStore migrator = new FromPostgresToSStore();
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "tpch", "pguser", "");
		String tableFrom = "orders";
		SStoreSQLConnectionInfo conTo = new SStoreSQLConnectionInfo("localhost",
				"21212", "", "user", "password");
		String tableTo = "orders";
		boolean caching = false;
		MigrationResult result = migrator.migrate(conFrom, tableFrom, conTo, tableTo, caching);
		System.out.println(result);
		
	}

}
