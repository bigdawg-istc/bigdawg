package istc.bigdawg.migration;

import static istc.bigdawg.network.NetworkUtils.isThisMyIpAddress;

import java.net.InetAddress;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.network.NetworkOut;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;

public class FromSStoreToPostgres 
	extends FromDatabaseToDatabase implements MigrationNetworkRequest {

    private static final long serialVersionUID = 1L;
    
    
    private static Logger log = Logger.getLogger(FromSStoreToPostgres.class);

    private SStoreSQLConnectionInfo connectionFrom;
    private String fromTable;
    private PostgreSQLConnectionInfo connectionTo;
    private String toTable;
    private String serverAddress = null;
    private int serverPort = 0;


    public MigrationResult execute(boolean caching) throws MigrationException {
	if (this.connectionFrom == null || this.fromTable == null
		|| this.connectionTo == null || this.toTable == null) {
	throw new MigrationException("The object was not initialized");
        }
        FromSStoreToPostgresImplementation migrator = new FromSStoreToPostgresImplementation(
        		connectionFrom, fromTable, connectionTo, toTable, serverAddress, serverPort);
        return migrator.migrateBin(caching);
    }
    
    @Override
    public MigrationResult execute() throws MigrationException {
    	return execute(false);
    }

    
    @Override
    public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
	    String objectTo) throws MigrationException {
    	serverAddress = serverAddress == null ? "localhost" : serverAddress;
    	serverPort = serverPort == 0 ? 18001 : serverPort;
    	return migrate(connectionFrom, objectFrom, connectionTo, objectTo, false, serverAddress, serverPort);
    }
    
    
    public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
	    String objectTo, boolean caching, String serverAddress, int serverPort) throws MigrationException {
	log.debug("General data migration: " + this.getClass().getName());
	if (connectionFrom instanceof SStoreSQLConnectionInfo
			&& connectionTo instanceof PostgreSQLConnectionInfo) {
		this.connectionFrom = (SStoreSQLConnectionInfo) connectionFrom;
		this.fromTable = objectFrom;
		this.connectionTo = (PostgreSQLConnectionInfo) connectionTo;
		this.toTable = objectTo;
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
		String url = this.connectionTo.getUrl();
		String user = this.connectionTo.getUser();
		String password = this.connectionTo.getPassword();
		try {
			/*
			 * check if the address is not a local host
			 */
			String hostname = connectionFrom.getHost();
			log.debug("SStore hostname: " + hostname);
			if (!isThisMyIpAddress(InetAddress.getByName(hostname))) {
				log.debug("Migration will be executed remotely.");
				Object result = NetworkOut.send(this, hostname);
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
		FromSStoreToPostgres migrator = new FromSStoreToPostgres();
		SStoreSQLConnectionInfo conFrom = new SStoreSQLConnectionInfo("localhost",
				"21212", "", "user", "password");
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "test", "pguser", "");
		String[] tableFroms = {"dimtrade"};
		String[] tableTos = {"dimtrade"};
		for (int i = 0; i < tableFroms.length; i++) {
			boolean caching = false;
			String tableFrom = tableFroms[i];
			String tableTo = tableTos[i];
			String serverAddress = "localhost";
			int serverPort = 18001;
			MigrationResult result = migrator.migrate(conFrom, tableFrom, conTo, tableTo, caching, serverAddress, serverPort);
			System.out.println(result);
		}
		
	}

}
