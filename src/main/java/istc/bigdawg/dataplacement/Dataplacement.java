package istc.bigdawg.dataplacement;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.Main;
import istc.bigdawg.catalog.CatalogInstance;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.FromPostgresToSStore;
import istc.bigdawg.migration.FromSStoreToPostgres;
import istc.bigdawg.migration.MigratorTask;
import istc.bigdawg.monitoring.MonitoringTask;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;

import javax.ws.rs.core.Response;


public class Dataplacement implements Serializable {
	private enum Tables {
		ORDERS, ORDER_LINE, ITEM, CUSTOMER, NEW_ORDER
	}
	private enum Databases {
		SSTORE, POSTGRES
	}
	private enum Queries {
		SSTOREQ3, POSTGRESQJOINAGG, UPDATE, SSTOREUPDATE, POSTGRESUPDATE, POSTGRESQ3
	}
	private enum MigrationDirection {
		FROMSSTORETOPOSTGRES, FROMPOSTGRESTOSSTORE
	}
	
	private Map<Queries, String> queries = new HashMap<Queries, String>();
	private ArrayList<Double> percentage = new ArrayList<Double>();
	private ArrayList<Double> accumuPercentage = new ArrayList<Double>();
	
	private Map<Tables, String> tableName = new HashMap<Tables, String>(); 
	private Map<Tables, Set<Databases>> tableLocation = new HashMap<Tables, Set<Databases>>();
	private Map<Queries, ArrayList<Tables>> queryTables = new HashMap<Queries, ArrayList<Tables>>();
	private Map<Queries, Databases> queryDatabase = new HashMap<Queries, Databases>();
	private Map<Queries, Long> queryTime = new HashMap<Queries, Long>();
	private Map<Tables, Long> migrationTime = new HashMap<Tables, Long>();
	boolean caching;

    private static final int sstoreDBID = BigDawgConfigProperties.INSTANCE.getSStoreDBID();
    private static final int psqlDBID = BigDawgConfigProperties.INSTANCE.getTpccDBID();
    private static SStoreSQLConnectionInfo sstoreConnInfo;
    private static PostgreSQLConnectionInfo psqlConnInfo;
	
	private static Logger logger;
	
	Dataplacement(Boolean caching) {
		queries.put(Queries.SSTOREQ3, "Query3");
		queries.put(Queries.POSTGRESQJOINAGG, "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), SUM(i_price), COUNT(*) " +
	            		"FROM order_line, item " +
	            		"WHERE order_line.ol_i_id = item.i_id " +
	            		"GROUP BY ol_number " +
	            		"ORDER BY ol_number"
					);
		queries.put(Queries.UPDATE, "update");
		queries.put(Queries.POSTGRESQ3, "SELECT ol_o_id, ol_w_id, ol_d_id, SUM(ol_amount) as revenue, o_entry_d FROM CUSTOMER, NEW_ORDER, ORDERS, ORDER_LINE WHERE c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id and no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id and o_entry_d > '0' GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d ORDER BY o_entry_d");
//		queries.put(Queries.SSTOREUPDATE, "UpdateOrderLine");
//		queries.put(Queries.POSTGRESUPDATE, "UPDATE order_line SET ol_quantity = ol_quantity + 1 WHERE ol_o_id = 100");

		percentage.add(0.01);
		percentage.add(0.01);
		percentage.add(0.98);

		Double sumP = 0.0;
		for (Double p : percentage) {
			sumP += p;
			accumuPercentage.add(sumP);
		}
		
		tableName.put(Tables.ORDERS, "ORDERS");
		tableName.put(Tables.ORDER_LINE, "ORDER_LINE");
		tableName.put(Tables.ITEM, "ITEM");
		tableName.put(Tables.CUSTOMER, "CUSTOMER");
		tableName.put(Tables.NEW_ORDER, "NEW_ORDER");
		
		tableLocation.put(Tables.ORDERS, new HashSet<Databases>(Arrays.asList(Databases.SSTORE)));
		tableLocation.put(Tables.ORDER_LINE, new HashSet<Databases>(Arrays.asList(Databases.SSTORE)));
		tableLocation.put(Tables.ITEM, new HashSet<Databases>(Arrays.asList(Databases.SSTORE)));
		tableLocation.put(Tables.CUSTOMER, new HashSet<Databases>(Arrays.asList(Databases.SSTORE)));
		tableLocation.put(Tables.NEW_ORDER, new HashSet<Databases>(Arrays.asList(Databases.SSTORE)));
		
		queryTables.put(Queries.SSTOREQ3, new ArrayList<Tables>(Arrays.asList(Tables.CUSTOMER, 
																Tables.NEW_ORDER, 
																Tables.ORDERS, 
																Tables.ORDER_LINE)));
		queryTables.put(Queries.POSTGRESQJOINAGG, new ArrayList<Tables>(Arrays.asList(Tables.ORDER_LINE, Tables.ITEM)));
		queryTables.put(Queries.UPDATE, new ArrayList<Tables>(Arrays.asList(Tables.ORDER_LINE)));
		queryTables.put(Queries.SSTOREUPDATE, new ArrayList<Tables>(Arrays.asList(Tables.ORDER_LINE)));
		queryTables.put(Queries.POSTGRESUPDATE, new ArrayList<Tables>(Arrays.asList(Tables.ORDER_LINE)));
		queryTables.put(Queries.POSTGRESQ3, new ArrayList<Tables>(Arrays.asList(
				Tables.CUSTOMER,
				Tables.NEW_ORDER,
				Tables.ORDERS,
				Tables.ORDER_LINE)));

		queryDatabase.put(Queries.SSTOREQ3, Databases.SSTORE);
		queryDatabase.put(Queries.POSTGRESQJOINAGG, Databases.POSTGRES);
		queryDatabase.put(Queries.SSTOREUPDATE, Databases.SSTORE);
		queryDatabase.put(Queries.POSTGRESUPDATE, Databases.POSTGRES);
		queryDatabase.put(Queries.POSTGRESQ3, Databases.POSTGRES);
		
		Long baseTime = 50L;
		int timeRatio = 10;
		double olapQueryTimeRatio = 1.0;
		Long migrationBaseTime = baseTime * 80;
		queryTime.put(Queries.SSTOREQ3, baseTime*timeRatio);
		queryTime.put(Queries.POSTGRESQJOINAGG, (long)(baseTime*timeRatio*olapQueryTimeRatio));
		queryTime.put(Queries.UPDATE, baseTime);
		migrationTime.put(Tables.ORDER_LINE, migrationBaseTime);
		
		this.caching = caching;
		
    	try {
			this.sstoreConnInfo = 
					(SStoreSQLConnectionInfo) CatalogViewer.getConnectionInfo(sstoreDBID);
	    	this.psqlConnInfo =
	        		(PostgreSQLConnectionInfo) CatalogViewer.getConnectionInfo(psqlDBID);
		} catch (BigDawgCatalogException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	}
	
	private ArrayList<Queries> generateWorkload(Integer totalQueries) {
		ArrayList<Queries> workload = new ArrayList<Queries>();
		
		Random r = new Random();
		for (int i = 0; i < totalQueries; i++) {
			Double p = r.nextDouble();
			int ind = -(Collections.binarySearch(accumuPercentage, p) + 1);
			
			workload.add(Queries.values()[ind]);
		}
		
//		workload.add(Queries.SSTOREQ3);
//		workload.add(Queries.POSTGRESQJOINAGG);
//		workload.add(Queries.UPDATE);
//		workload.add(Queries.SSTOREQ3);
		
//		workload.add(Queries.POSTGRESQ3);
		
		return workload;
	}
	
	private Map<Queries, Integer> getQueryNumbers(ArrayList<Queries> workload) {
		Map<Queries, Integer> queryNumbers = new HashMap<Queries, Integer>();
		for (Queries q : workload) {
			if (queryNumbers.get(q) == null) {
				queryNumbers.put(q, 1);
			} else {
				queryNumbers.put(q, queryNumbers.get(q) + 1);
			}
		}
		
		return queryNumbers;
	}
	
	private long executeUpdateQuery(Tables t, Boolean realTime) {
		QueryClient qClient = new QueryClient();
		long lStartTime = System.nanoTime();
		String queryStr;
		Response response1;
		int updateTableNum = 0;
		for (Databases db : tableLocation.get(t)) {
			if (db==Databases.SSTORE) {
				queryStr = "UpdateOrderLine";
				response1 = qClient.query("{\"query\":\"bdstream("+queryStr+")\"}");
			} else {
				queryStr = "UPDATE order_line SET ol_quantity = ol_quantity + 1 WHERE ol_o_id = 100";
				/* Currently update is not supported in BigDawg
				response1 = qClient.query("{\"query\":\"bdrel("+queryStr+")\"}");
				*/
	    		PostgreSQLHandler postgresH = new PostgreSQLHandler(psqlConnInfo);
	    		try {
					postgresH.executeStatementOnConnection(queryStr);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();//			System.out.println("workload time: " + String.valueOf(workloadTime));

				}
			}
			updateTableNum ++;
		}
		
		return (long) (realTime ? (System.nanoTime() - lStartTime) / 1000000 :
			queryTime.get(Queries.UPDATE) * Math.pow(1.8, updateTableNum - 1)); // Hueristics: base * 1.8^(x-1)
	}
	
	private long executeQuery(String queryStr, Databases db) {
		return executeQuery(queryStr, db, true);
	}
	
	private long executeQuery(String queryStr, Databases db, Boolean realTime) {
		QueryClient qClient = new QueryClient();
		long lStartTime = System.nanoTime();
		Response response1;
		if (queryStr.equals("update")) {
			for (Tables t : queryTables.get(Queries.UPDATE)) {
				return executeUpdateQuery(t, realTime);
			}
		}
		switch(db) {
			case SSTORE:
				response1 = qClient.query("{\"query\":\"bdstream("+queryStr+")\"}");
				break;
			case POSTGRES:
				response1 = qClient.query("{\"query\":\"bdrel("+queryStr+")\"}");
				break;
			default:
				break;
		}
			
		if (realTime) { 
			return (System.nanoTime() - lStartTime) / 1000000;
		} else if (db == Databases.SSTORE) {
			return queryTime.get(Queries.SSTOREQ3);
		}
		
		return queryTime.get(Queries.POSTGRESQJOINAGG);
//		System.out.println(response1.getEntity());
	}
	
	private long executeWorkload(ArrayList<Queries> workload) {
		return executeWorkload(workload, true);
	}
	
	private long executeWorkload(ArrayList<Queries> workload, Boolean realTime) {
		if (workload == null) {
			return -1;
		}
		
		long workloadTime = 0L;
		for (Queries p : workload) {
			ArrayList<Tables> neededTables = queryTables.get(p);
			Databases currentDB = queryDatabase.get(p);
			if (currentDB != null) { // Not update
				for (Tables t: neededTables) {
					if (!tableLocation.get(t).contains(currentDB)) {
						long lStartTime = System.nanoTime();
						// Migrate
						switch (currentDB) {
						case SSTORE:
							migrateTable(t, t, MigrationDirection.FROMPOSTGRESTOSSTORE);
							break;
						case POSTGRES:
							migrateTable(t, t, MigrationDirection.FROMSSTORETOPOSTGRES);
							break;
						}
						long lEndTime = System.nanoTime();
//						System.out.println("Moving "+t.toString()+": "+String.valueOf((lEndTime - lStartTime) / 1000000)+"ms.");
						if (realTime) {
							workloadTime += (lEndTime - lStartTime) / 1000000;
						} else {
							workloadTime += migrationTime.get(Tables.ORDER_LINE);
						}
					}
				}
			}
			// execute
			String queryStr = queries.get(p);
			long execTime = executeQuery(queryStr, currentDB, realTime);
			workloadTime += execTime;
//			System.out.println("workload time: " + String.valueOf(workloadTime));
		}
		return workloadTime;
	}
	
	private void migrateTable(Tables fromTable, Tables toTable, MigrationDirection md) {
		long workloadTime = 0L;
		String fromTableName = tableName.get(fromTable);
		String toTableName = tableName.get(toTable);
		try {
			switch(md) { 
			case FROMSSTORETOPOSTGRES:
				FromSStoreToPostgres fromSStoreToPostgres = new FromSStoreToPostgres();
				fromSStoreToPostgres.migrate(sstoreConnInfo, fromTableName, psqlConnInfo, toTableName, caching);
				tableLocation.get(toTable).add(Databases.POSTGRES);
				if (!caching) {
					tableLocation.get(fromTable).remove(Databases.SSTORE);
				}
				break;
			case FROMPOSTGRESTOSSTORE:
				FromPostgresToSStore fromPostgresToSStore = new FromPostgresToSStore();
				fromPostgresToSStore.migrate(psqlConnInfo, fromTableName, sstoreConnInfo, toTableName, caching);
				tableLocation.get(toTable).add(Databases.SSTORE);
				if (!caching) {
					tableLocation.get(fromTable).remove(Databases.POSTGRES);
				}
				break;
			default:
				throw new MigrationException("Migration direction" + md.toString() + " not supported yet", new Exception());
			}
		} catch (MigrationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		LoggerSetup.setLogging();
		logger = Logger.getLogger(Main.class);
		logger.info("Starting application ...");
		
//		String workloadFilename = "workload.ser.80";
		String workloadFilename = "workload.ser.tmp";
		
//		boolean caching = true;
//		boolean realTime = false;
//		Dataplacement dp = new Dataplacement(caching);
//		ArrayList<Queries> workload = dp.generateWorkload(1000);
//		FileOutputStream workloadOut = new FileOutputStream(workloadFilename);
//		ObjectOutputStream out = new ObjectOutputStream(workloadOut);
//		out.writeObject(workload);
//		out.close();
//		workloadOut.close();
//		
//		long workloadTime = dp.executeWorkload(workload, realTime);
//		System.out.println("Workload finished in: " + String.valueOf(workloadTime) + "ms for caching.");
		
//		Map<Queries, Integer> queryNumbers = dp.getQueryNumbers(workload);
//		for (Queries q : queryNumbers.keySet()) {
//			System.out.println("Query :" + q.toString() + " Numbers: " + queryNumbers.get(q).toString());
//		}
//		for (Queries p : workload) {
//			System.out.println(dp.queries.get(p));
//		}
		
		FileInputStream workloadIn = new FileInputStream(workloadFilename);
		ObjectInputStream in = new ObjectInputStream(workloadIn);
		ArrayList<Queries> workload = null;
		try {
			workload = (ArrayList<Queries>) in.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		boolean caching = false;
		boolean realTime = false;
		Dataplacement dpMoving = new Dataplacement(caching);
		long workloadMovingTime = dpMoving.executeWorkload(workload, realTime);
		System.out.println("Workload finished in: " + String.valueOf(workloadMovingTime) + "ms for moving.");
		
	}
	
}
