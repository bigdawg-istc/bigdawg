package istc.bigdawg.sstore;

import static istc.bigdawg.utils.JdbcUtils.getColumnNames;
import static istc.bigdawg.utils.JdbcUtils.getRows;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jfree.util.Log;
import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.injection.Injection;
import istc.bigdawg.islands.CrossIslandCastNode;
import istc.bigdawg.islands.CrossIslandQueryNode;
import istc.bigdawg.islands.CrossIslandQueryPlan;
import istc.bigdawg.migration.FileFormat;
import istc.bigdawg.migration.FromDatabaseToDatabase;
import istc.bigdawg.migration.FromSStoreToPostgres;
import istc.bigdawg.migration.MigrationResult;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.monitoring.MonitoringTask;
import istc.bigdawg.network.NetworkIn;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;

/**
 * @author Jiang Du
 * 
 *         May 3, 2017 7:20:55 PM
 */
public class SStoreMigratorTask implements Runnable {

	private static Logger logger = Logger
			.getLogger(SStoreMigratorTask.class);

	private ExecutorService executor = null;
    private ScheduledExecutorService scheduledExecutor;
    public static long MIGRATION_RATE;	// In milliseconds
    private static String[] tables;
    public static int sstoreDBID;
    public static int postgresDBID;
    private Long executedTime = null; // In milliseconds
    private Stack<Long> migratedTupleCount = new Stack<Long>(); // Stores the sum of the number of tuples for each migration
    private Stack<Long> migrationTime = new Stack<Long>(); // Stores the execution time for each migration
    private Stack<Long> startedTime = new Stack<Long>(); // Stores the started time for each migration
    private Stack<Long> earliestTimestamp = new Stack<Long>(); // Stores the earliest timestamp for this migration
    
    public SStoreMigratorTask() {
		int numberOfThreads = 1;
		executor = Executors.newFixedThreadPool(numberOfThreads);
		executor.submit(new NetworkIn());
		scheduledExecutor = Executors.newScheduledThreadPool(1);
		this.tables = new String[]{"medevents"};
		sstoreDBID = BigDawgConfigProperties.INSTANCE.getSStoreDBID();
		postgresDBID = BigDawgConfigProperties.INSTANCE.getMimic2DBID();
		MIGRATION_RATE = 60000;
    }
    

	/**
	 * Run the service for migrator - this network task accepts remote request
	 * for data migration.
	 */
	public SStoreMigratorTask(String[] tables, Integer sstoreDBID, Integer postgresDBID, Long migration_rate) {
		int numberOfThreads = 1;
		executor = Executors.newFixedThreadPool(numberOfThreads);
		executor.submit(new NetworkIn());
		scheduledExecutor = Executors.newScheduledThreadPool(1);
		this.tables = tables;
		this.sstoreDBID = sstoreDBID;
		this.postgresDBID = postgresDBID;
		this.MIGRATION_RATE = migration_rate;
	}

	/**
	 * Run the service for migrator - this network task accepts remote request
	 * for data migration.
	 */
	public SStoreMigratorTask(String[] tables, Integer sstoreDBID, Integer postgresDBID, Long migration_rate,
			Long executedTime) {
		int numberOfThreads = 1;
		executor = Executors.newFixedThreadPool(numberOfThreads);
		executor.submit(new NetworkIn());
		scheduledExecutor = Executors.newScheduledThreadPool(1);
		this.tables = tables;
		this.sstoreDBID = sstoreDBID;
		this.postgresDBID = postgresDBID;
		this.MIGRATION_RATE = migration_rate;
		this.executedTime = executedTime;
	}

	/**
	 * Close the migrator task.
	 */
	public void close() {
		if (executor != null) {
			if (!executor.isShutdown()) {
				executor.shutdownNow();
			}
		}
		executor = null;
		
		if (scheduledExecutor != null) {
			if (!scheduledExecutor.isShutdown()) {
				scheduledExecutor.shutdown();
			}
		}
		scheduledExecutor = null;
	}


	public static boolean serverListening(String host, int port) {
	    Socket s = null;
	    try {
	        s = new Socket(host, port);
	        return true;
	    } catch (Exception e) {
	        return false;
	    } finally {
	        if(s != null) {
	            try {
	            	s.close();
	            } catch(Exception e){
	            	;
	            }
	        }
	    }
	}
	

	public void waitForSStore() {
		try {
			SStoreSQLConnectionInfo sstoreConnInfo = 
					(SStoreSQLConnectionInfo) CatalogViewer.getConnectionInfo(sstoreDBID);
			String host = sstoreConnInfo.getHost();
	    	int port = Integer.parseInt(sstoreConnInfo.getPort());
	    	while (!serverListening(host, port)) {
	    		try {
					Thread.sleep(1000); // sleep 1 second
				} catch (InterruptedException e) {
					;
				}
	    	}
		} catch (BigDawgCatalogException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return;
	}
	
	
    @Override
    public void run() {
//		cleanHistoricalData();
		waitForSStore();
		Task migrateTask = new Task(tables, migratedTupleCount, migrationTime, startedTime, earliestTimestamp);
	    ScheduledFuture futureTask = 
	    		this.scheduledExecutor.scheduleAtFixedRate(migrateTask, 0, MIGRATION_RATE, TimeUnit.MILLISECONDS);
	    if (executedTime != null) {
	    	try {
	    		Thread.sleep(executedTime);
	    	} catch (InterruptedException e) {
	    		// TODO Auto-generated catch block
	    		e.printStackTrace();
	    	}
	    	futureTask.cancel(true);
	    	PrintStats(migrateTask);
	    }
    }
    
    
    public void runOnce() {
    	Task migrateTask = new Task(tables, migratedTupleCount, migrationTime, startedTime, earliestTimestamp);
    	Thread t = new Thread(migrateTask);
    	t.start();
    	try {
			t.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    
    public Long getTotalMigratedTuples() {
    	return migratedTupleCount.empty() ? 0L : migratedTupleCount.peek();
    }
    
    public Long getLastMigrationTime() {
    	return migrationTime.empty() ? System.currentTimeMillis() : migrationTime.peek();
    }

    public Long getLastMigrationStartedTime() {
    	return startedTime.empty() ? System.currentTimeMillis() : startedTime.peek();
    }
    
    public Stack<Long> getMigrationStartedTime() {
    	return startedTime;
    }

    public Stack<Long> getEarliestTimestamp() {
    	return earliestTimestamp;
    }
    
	private void PrintStats(Task migrateTask) {
		System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
	    System.out.println("Executed times: " + migrateTask.getExecutedTimes());
	    System.out.println("Total executed time: " + migrateTask.getTotalExecTime());
	    System.out.println("Total migrated tuples: " + migrateTask.getTotalMigratedTuples());
	    System.out.println("Average executed time: " + migrateTask.getAvgExecTime().toString());
	    System.out.println("Average migrated tuples: " + migrateTask.getAvgTuples());
	    System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
	}

	private void cleanHistoricalData() {
		// clean the historical data before any migration
//    	int psqlDBID = BigDawgConfigProperties.INSTANCE.getSeaflowDBID();
    	PostgreSQLConnectionInfo psqlConnInfo = null;
    	Connection psqlConn = null;
    	try {
			psqlConnInfo =
					(PostgreSQLConnectionInfo) CatalogViewer.getConnectionInfo(postgresDBID);
			psqlConn = PostgreSQLHandler.getConnection(psqlConnInfo);
		} catch (BigDawgCatalogException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	assert(psqlConn != null);
    	    	
    	for (String table : tables) {
    		String cmd = "DROP TABLE IF EXISTS " + table;
    		try {
				PostgreSQLHandler.executeStatement(psqlConn, cmd);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
		try {
			psqlConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}


class Task implements Runnable {
	private static Logger logger = Logger
			.getLogger(FromSStoreToPostgres.class);

	private static final int sstoreDBID = SStoreMigratorTask.sstoreDBID;
	private static final int psqlDBID = SStoreMigratorTask.postgresDBID;
    private static SStoreSQLConnectionInfo sstoreConnInfo;
    private static PostgreSQLConnectionInfo psqlConnInfo;
    private static String[] tables;
    private Long executedTimes = 0L;
    private Long totalExecTime = 0L; // in milliseconds
    private Long totalMigratedTuples = 0L;
    private Stack<Long> migratedTupleCount;
    private Stack<Long> migrationTime;
    private Stack<Long> startedTime;
    private Stack<Long> earliestTimestamp;
    static boolean isRunning = false;

    Task(String[] tables, Stack<Long> migratedTupleCount, Stack<Long> migrationTime, 
    		Stack<Long> startedTime, Stack<Long> earliestTimestamp){
		LoggerSetup.setLogging();
		logger = Logger.getLogger(Injection.class);
		
    	this.tables = tables;
    	this.migratedTupleCount = migratedTupleCount;
    	this.migrationTime = migrationTime;
    	this.startedTime = startedTime;
    	this.earliestTimestamp = earliestTimestamp;
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
    
    @Override
    public void run(){
    	if (isRunning == false) {
    		isRunning = true;
    		for (String table : tables) {
    			String tableFrom = table;
    			String tableTo = table;
    			try {
    				long startTimeMigration = System.currentTimeMillis();
    				FromDatabaseToDatabase migrator = new FromSStoreToPostgres(
    						(SStoreSQLConnectionInfo)sstoreConnInfo, tableFrom, 
    						(PostgreSQLConnectionInfo)psqlConnInfo, tableTo,
//    						FileFormat.BIN_POSTGRES,
    						FileFormat.CSV);
    				MigrationResult migrationResult;
    				try {
    					migrationResult = migrator.migrate(migrator.getMigrationInfo());
    				} catch (MigrationException e) {
    					e.printStackTrace();
    					logger.error(e.getMessage());
    					throw e;
    				}
    				long endTimeMigration = System.currentTimeMillis();
    				long durationMsec = endTimeMigration - startTimeMigration;
    				executedTimes ++;
    				totalExecTime += durationMsec;
    				totalMigratedTuples += migrationResult.getCountLoadedElements();
    				migratedTupleCount.push(totalMigratedTuples);
    				migrationTime.push(durationMsec);
    				startedTime.push(startTimeMigration);
    				earliestTimestamp.push(migrationResult.getEarliestTimestamp());
    			} catch (MigrationException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			} finally {
    			}
    		}
    		isRunning = false;
    	}
    }
    
    public Double getAvgExecTime() {
    	return totalExecTime * 1.0 / executedTimes;
    }
    
    public Double getAvgTuples() {
    	return totalMigratedTuples * 1.0 / executedTimes;
    }
    
    public Long getExecutedTimes() {
    	return executedTimes;
    }
    
    public Long getTotalExecTime() {
    	return totalExecTime;
    }
    
    public Long getTotalMigratedTuples() {
    	return totalMigratedTuples;
    }
    
}
