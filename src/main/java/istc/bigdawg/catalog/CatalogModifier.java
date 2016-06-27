package istc.bigdawg.catalog;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.Set;

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
	
	public static int addObject(String newName, String newFields, int newLogDB, int newPhyDB) throws Exception {
		
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		CatalogUtilities.checkLength(newName, 15);
		CatalogUtilities.checkLength(newFields, 300);
		
		// check for existing record
		ResultSet rs = cc.execRet("SELECT * FROM catalog.objects WHERE "
								+ "name = \'"		+ newName.toLowerCase() 	+ "\' AND "
								+ "fields = \'"		+ newFields.toLowerCase()	+ "\' AND "
								+ "logical_db = "	+ newLogDB 					+ " AND "
								+ "physical_db = "	+ newPhyDB					+ ";");
		int newpos  = 0;
		if ( !rs.next() ) {
			// add new record
        	rs 	= cc.execRet("SELECT max(oid) m from catalog.objects;");
        	if (rs.next() && rs.getString(1) != null) newpos = rs.getInt("m") + 1;
        	cc.execNoRet("INSERT INTO catalog.objects (oid, name, fields, logical_db, physical_db) "
	        			+ "VALUES ("+ newpos 					+ ", "
							+ "\'"	+ newName.toLowerCase() 	+ "\', "
	    					+ "\'"	+ newFields.toLowerCase() 	+ "\', "
	    							+ newLogDB 					+ ", "
	    							+ newPhyDB 					+ ");");
        }
        rs.close();

        // commit
		cc.commit();
		
		return newpos;
	}
	
	public static void deleteObject(int oid) throws Exception {
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		
    	cc.execNoRet("DELETE FROM catalog.objects WHERE oid = "+oid);

        // commit
		cc.commit();
	}
	
	public static void deleteMultipleObjects(Set<Integer> oids) throws Exception {
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		
		// check if cc is connected and length are correct 
		CatalogUtilities.checkConnection(cc);
		
		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM catalog.objects WHERE oid in (");
		for (Integer i : oids) sb.append(i).append(',');
		cc.execNoRet(sb.deleteCharAt(sb.length() - 1).append(')').toString());
		
//    	cc.execNoRet(String.format("DELETE FROM catalog.objects WHERE oid in (%s)"
//    			, String.join(", ", oids.stream().map(i -> {return String.valueOf(i);}).collect(Collectors.toSet()))));

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
			
			String host = "10.105.79.183";
			
			cc.execNoRet("update catalog.engines set host = \'"+ host +"\' where eid = 1;"); 
			cc.execNoRet("update catalog.engines set host = \'"+ host +"\' where eid = 2;");
			cc.execNoRet("update catalog.engines set host = \'"+ host +"\' where eid = 4;");
			
			// insert example: addObject(String newName, String newFields, int newLogDB, int newPhyDB)

//			// Genbase insert
//			addObject("go_matrix", "geneid,goid,belongs", 6, 6);
//			addObject("genes", "id,target,pos,len,func", 9, 9);
//			addObject("patients", "id,age,gender,zipcode,disease,response", 7, 7);
//			addObject("geo", "geneid,patientid,expr_value", 8, 8);
			
//			// TPCH insert
//			addObject("REGION", "R_REGIONKEY,R_NAME,R_COMMENT", 10,10);
//			addObject("NATION", "N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT", 10, 10);
//			addObject("PART","P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMEN", 10, 10);
//			addObject("SUPPLIER","S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT",10,10);
//			addObject("PARTSUPP","PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT",10,10);
//			addObject("CUSTOMER","C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMEN",10,10);
//			addObject("ORDERS","O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMEN",10,10);
//			addObject("LINEITEM","L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT",10,10);
			
			cc.commit();
			System.out.println("Update complete!");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}