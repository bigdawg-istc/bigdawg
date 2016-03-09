package istc.bigdawg.catalog;

import java.sql.ResultSet;
import java.util.Objects;

/**
 * 
 * @author Jack
 *
 * Currently the catalog modifier just add things one by one. 
 * I'll add updates, deletes, etc. when needs arise.
 * 
 * Also, I assume that engines and databases have distinct names
 *  
 */
public class CatalogModifier {
	
	/**
	 * addIsland determines locally if there are duplicates. 
	 * It's not really related to performance, just to save the code for comparison.
	 * 
	 * @param cc
	 * @param newIsland
	 * @param newAccessMethod
	 * @throws Exception
	 */
	public static void addIsland(String newIsland, String newAccessMethod) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newIsland, 15);
		CatalogUtilities.checkLength(newAccessMethod, 30);
		
		// check for existing record
		ResultSet rs  = cc.execRet("SELECT * FROM catalog.islands order by iid;");
		int newpos    = 0;
		boolean found = false;
		if (rs.next()) {
			do {
				newpos = rs.getInt("iid") + 1;
				if (Objects.equals(rs.getString("scope_name"), newIsland) &&
					Objects.equals(rs.getString("access_method"), newAccessMethod)) {
					found = true;
				}
			} while (rs.next());
		} 
		
		// add new record
		if (!found) {
			cc.execNoRet("INSERT INTO catalog.islands (iid, scope_name, access_method) "
        				+ "VALUES ("+ newpos 		+ 	", "
    					+ "\'"  + newIsland 		+ "\', "
						+ "\'"  + newAccessMethod 	+ "\');");
		}
		rs.close();
		
        // commit
		cc.commit();

	}
	
	/**
	 * addEngine determines remotely if there are duplicates. 
	 * It's not really related to performance, just to save the code for comparison.
	 * 
	 * @param cc
	 * @param newEngine
	 * @param newHost
	 * @param newPort
	 * @param newProperty
	 * @throws Exception
	 */
	public static void addEngine(String newEngine, String newHost, int newPort, String newProperty) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newEngine, 15);
		CatalogUtilities.checkLength(newHost, 40);
		CatalogUtilities.checkLength(newProperty, 100);
		
		// check for existing record
		ResultSet rs = cc.execRet("SELECT * FROM catalog.engines WHERE "
								+ "name = \'"					+ newEngine 	+ "\' AND "
								+ "host  = \'"					+ newHost		+ "\' AND "
								+ "port = "						+ newPort		+   " AND "
								+ "connection_properties = \'" 	+ newProperty 	+ "\';");
		if ( !rs.next() ) {
			// add new record
        	rs 			= cc.execRet("SELECT max(eid) m from catalog.engines;");
        	int newpos  = 0; 
        	if (rs.next() && rs.getString(1) != null) 
        		newpos = rs.getInt("m") + 1;
        	cc.execNoRet("INSERT INTO catalog.engines (eid, name, host, port, connection_properties) "
	        			+ "VALUES ("+ newpos 		+   ", "
	    					+ "\'"	+ newEngine 	+ "\', "
							+ "\'"	+ newHost 		+ "\', "
	    							+ newPort 		+ 	", "
							+ "\'"	+ newProperty 	+ "\');");
        }
        rs.close();

        // commit
		cc.commit();

	}
	
	public static void addShim(int newIslandId, int newEngineId, String newAccessMethod) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newAccessMethod, 30);
		
		// check for existing record
		ResultSet rs = cc.execRet( "SELECT * FROM catalog.shims WHERE "
								+ "island_id = "		+ newIslandId 		+ " AND "
								+ "engine_id = "		+ newEngineId		+ " AND "
								+ "access_method = \'" 	+ newAccessMethod 	+ "\';");
		if ( !rs.next() ) {
			// add new record
        	rs 			= cc.execRet("SELECT max(shim_id) m from catalog.shims;");
        	int newpos 	= 0; 
        	if (rs.next() && rs.getString(1) != null) newpos = rs.getInt("m") + 1;
        	cc.execNoRet("INSERT INTO catalog.shims (shim_id, island_id, engine_id, access_method) "
	        			+ "VALUES ("+ newpos 			+ ", "
	    							+ newIslandId 		+ ", "
	    							+ newEngineId 		+ ", "
							+ "\'"	+ newAccessMethod	+ "\');");
        }
        rs.close();

        // commit
		cc.commit();
			
	}
	
	public static void addCast(int newSrcEid, int newDstEid, String newAccessMethod) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newAccessMethod, 30);
		
		// check for existing record
		ResultSet rs = cc.execRet("SELECT * FROM catalog.casts WHERE "
								+ "src_eid = "			+ newSrcEid 		+ " AND "
								+ "dst_eid = "			+ newDstEid			+ " AND "
								+ "access_method = \'" 	+ newAccessMethod 	+ "\';");
		if ( !rs.next() ) {
			// add new record
        	cc.execNoRet("INSERT INTO catalog.casts (src_eid, dst_eid, access_method) "
	        			+ "VALUES ("+ newSrcEid 		+ ", "
	    							+ newDstEid 		+ ", "
							+ "\'"	+ newAccessMethod	+ "\');");
        }
        rs.close();

        // commit
		cc.commit();
			
	}
	
	public static void addDatabase(int newEngineId, String newName, String newUserid, String newPassword) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newName, 15);
		CatalogUtilities.checkLength(newUserid, 15);
		CatalogUtilities.checkLength(newPassword, 15);
		
		// check for existing record
		ResultSet rs = cc.execRet("SELECT * FROM catalog.databases WHERE "
								+ "engine_id = "	+ newEngineId 	+   " AND "
								+ "name  = \'"		+ newName		+ "\' AND "
								+ "userid = \'"		+ newUserid		+ "\' AND "
								+ "password = \'" 	+ newPassword 	+ "\';");
		if ( !rs.next() ) {
			// add new record
        	rs 			= cc.execRet("SELECT max(dbid) m from catalog.databases;");
        	int newpos  = 0; 
        	if (rs.next() && rs.getString(1) != null) newpos = rs.getInt("m") + 1;
        	cc.execNoRet("INSERT INTO catalog.databases (dbid, engine_id, name, userid, password) "
	        			+ "VALUES ("+ newpos 		+   ", "
									+ newEngineId	+ 	", " 
	    					+ "\'"	+ newName 		+ "\', "
							+ "\'"	+ newUserid 	+ "\', "
							+ "\'"	+ newPassword 	+ "\');");
        }
        rs.close();

        // commit
		cc.commit();
	}
	
	public static void addObject(String newName, String newFields, int newLogDB, int newPhyDB) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newName, 15);
		CatalogUtilities.checkLength(newFields, 300);
		
		// check for existing record
		ResultSet rs = cc.execRet("SELECT * FROM catalog.objects WHERE "
								+ "name = \'"		+ newName 		+ "\' AND "
								+ "fields = \'"		+ newFields		+ "\' AND "
								+ "logical_db = "	+ newLogDB 		+ " AND "
								+ "physical_db = "	+ newPhyDB		+ ";");
		if ( !rs.next() ) {
			// add new record
        	rs 			= cc.execRet("SELECT max(oid) m from catalog.objects;");
        	int newpos  = 0; 
        	if (rs.next() && rs.getString(1) != null) newpos = rs.getInt("m") + 1;
        	cc.execNoRet("INSERT INTO catalog.objects (oid, name, fields, logical_db, physical_db) "
	        			+ "VALUES ("+ newpos 			+ ", "
							+ "\'"	+ newName 		+ "\', "
	    					+ "\'"	+ newFields 	+ "\', "
	    							+ newLogDB 		+ ", "
	    							+ newPhyDB 		+ ");");
        }
        rs.close();

        // commit
		cc.commit();
	}

	/**
	 * Used for updating catalog entries.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		try {
			CatalogUtilities.checkConnection(cc);
			
			String host = "10.105.199.141";
//			String host = "192.168.1.13";
			
			cc.execNoRet("update catalog.engines set host = \'"+ host +"\' where eid = 1;"); 
			cc.execNoRet("update catalog.engines set host = \'"+ host +"\' where eid = 2;");
			cc.execNoRet("update catalog.engines set host = \'"+ host +"\' where eid = 4;");
			
//			// insert example: addObject(String newName, String newFields, int newLogDB, int newPhyDB)
//			addObject("go_matrix", "geneid,goid,belongs", 6, 6);
//			addObject("genes", "id,target,pos,len,func", 9, 9);
//			addObject("patients", "id,age,gender,zipcode,disease,response", 7, 7);
//			addObject("geo", "geneid,patientid,expr_value", 8, 8);

			
			cc.commit();
			System.out.println("Update complete!");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}