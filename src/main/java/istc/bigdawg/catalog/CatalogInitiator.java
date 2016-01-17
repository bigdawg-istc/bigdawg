package istc.bigdawg.catalog;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.DriverManager;

/**
 * 
 * @author Jack
 *
 */
public class CatalogInitiator {

	/**
	 * This method connects to the catalog database on designated server.
	 * URL should directly points to the database where the catalog will reside. 
	 * 
	 * @param url
	 * @param username
	 * @param password
	 * @throws Exception
	 */
	public static void connect (Catalog cc, String url, String username, String password) throws Exception {
		
		if (cc.isInitiated()) { 
			cc.connection.close(); 
			cc.stmt.close();
		}
		//Class.forName("org.postgresql.Driver");
		cc.connection = DriverManager.getConnection(url, username, password);
		cc.stmt 	  = cc.connection.createStatement();
		cc.initiated  = true;
		cc.connected  = true;
		cc.connection.setAutoCommit(false);
		cc.setLastURL(url);
		cc.setLastUsername(username);
		cc.setLastPassword(password);
	}
	
	public static void close(Catalog cc) throws Exception {
		if (cc.isInitiated()) { 
			cc.stmt.close();
			cc.connection.close();
			cc.connected = false;
		}
	} 
	
	/**
	 * After connecting to the database where the catalog will be, 
	 * this functions creates the schema 'catalog' which will host
	 * all tables used by the catalog. It will then proceed to 
	 * create empty tables that are used to host catalog entries.
	 * 
	 * The connecting user is assumed to have CREATE privilege in the catalog host DB.
	 * 
	 * @param cc
	 * @throws Exception
	 */
	public static void createSchemaAndRelations(Catalog cc) throws Exception{
		
		CatalogUtilities.checkConnection(cc);
		
		// reading all the SQL commands
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/catalog/bigdawg_ddl.sql"));
		StringBuffer stringBuffer 	  = new StringBuffer();
		String line 				  = bufferedReader.readLine();
		while(line != null) {
			stringBuffer.append(line).append("\n");
			line = bufferedReader.readLine();
		}
		String createAllTables = stringBuffer.toString();
		bufferedReader.close();
		cc.execNoRet(createAllTables);
		cc.commit();
	}
	
}