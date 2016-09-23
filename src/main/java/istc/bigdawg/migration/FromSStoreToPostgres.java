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
import istc.bigdawg.catalog.CatalogViewer;

public class FromSStoreToPostgres 
	extends FromDatabaseToDatabase implements MigrationNetworkRequest {

    private static final long serialVersionUID = 1L;
    
    
    private static Logger log = Logger.getLogger(FromSStoreToPostgres.class);

    private SStoreSQLConnectionInfo connectionFrom;
    private String fromTable;
    private PostgreSQLConnectionInfo connectionTo;
    private String toTable;


    public MigrationResult execute(boolean caching) throws MigrationException {
	if (this.connectionFrom == null || this.fromTable == null
		|| this.connectionTo == null || this.toTable == null) {
	throw new MigrationException("The object was not initialized");
        }
        FromSStoreToPostgresImplementation migrator = new FromSStoreToPostgresImplementation(
        		connectionFrom, fromTable, connectionTo, toTable);
//        return migrator.migrate();
        return migrator.migrateBin(caching);
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
//		LoggerSetup.setLogging();
		FromSStoreToPostgres migrator = new FromSStoreToPostgres();
//		SStoreSQLConnectionInfo conFrom = (SStoreSQLConnectionInfo) CatalogViewer.getConnectionInfo(7);
//		PostgreSQLConnectionInfo conTo = (PostgreSQLConnectionInfo) CatalogViewer.getConnectionInfo(9);
		SStoreSQLConnectionInfo conFrom = new SStoreSQLConnectionInfo("localhost",
				"21212", "", "user", "password");
		PostgreSQLConnectionInfo conTo = new PostgreSQLConnectionInfo(
				"localhost", "5431", "tpcc", "pguser", "");
		String tableFrom = "item";
		String tableTo = "item";
		long startTime = System.currentTimeMillis();
		boolean caching = true;
		MigrationResult result = migrator.migrate(conFrom, tableFrom, conTo, tableTo, caching);
		long endTime = System.currentTimeMillis();
		System.out.println("time duration is: " + (endTime - startTime));
		System.out.println(result);
		
//		Properties props = new Properties();
//		props.setProperty("user","pguser");
//		props.setProperty("password","test");
//		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5430/tpch", props);
//		startTime = System.currentTimeMillis();
//		String sql1 = "select count(*) from customer";
//		Statement stmt1 = conn.createStatement();
//		ResultSet results1 = stmt1.executeQuery(sql1);
//		while (results1.next()) {
//			System.out.println(results1.getInt(1));
//		}
//		stmt1.close();
//		results1.close();
//		conn.close();
//		endTime = System.currentTimeMillis();
//		System.out.println("time duration is: " + (endTime - startTime));

//		Class.forName("org.voltdb.jdbc.Driver");
//		conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
//		String sql = "Select count(*) from orders;";
//		Statement stmt = conn.createStatement();
//		ResultSet results = stmt.executeQuery(sql);
//		while(results.next()) {	
//			System.out.println(results.getInt(1));
//		}
		
		
		
//		Properties props = new Properties();
//		props.setProperty("user","pguser");
//		props.setProperty("password","test");
//		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test_db", props);
//		long startTime = System.currentTimeMillis();
//        String sql1 = "select * from orders union select * from orders1";
//        Statement stmt1 = conn.createStatement();
//        ResultSet results1 = stmt1.executeQuery(sql1);
//        long endTime = System.currentTimeMillis();
//        System.out.println("time duration is: " + (endTime - startTime));
	}

}
