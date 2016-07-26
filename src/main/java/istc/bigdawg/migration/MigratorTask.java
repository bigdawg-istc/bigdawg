/**
 * 
 */
package istc.bigdawg.migration;

import static istc.bigdawg.utils.JdbcUtils.getColumnNames;
import static istc.bigdawg.utils.JdbcUtils.getRows;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.plan.QueryExecutionPlan;
import istc.bigdawg.islands.CrossIslandCastNode;
import istc.bigdawg.islands.CrossIslandQueryNode;
import istc.bigdawg.islands.CrossIslandQueryPlan;
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.monitoring.MonitoringTask;
import istc.bigdawg.network.NetworkIn;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.signature.Signature;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.sstore.SStoreSQLHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Mar 9, 2016 7:20:55 PM
 */
public class MigratorTask implements Runnable {

	private ExecutorService executor = null;
    private final ScheduledExecutorService scheduledExecutor;
    public static final int MIGRATION_RATE_SEC = 60;

	/**
	 * Run the service for migrator - this network task accepts remote request
	 * for data migration.
	 */
	public MigratorTask() {
		int numberOfThreads = 1;
		executor = Executors.newFixedThreadPool(numberOfThreads);
		executor.submit(new NetworkIn());
		scheduledExecutor = Executors.newScheduledThreadPool(1);
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
	}

    @Override
    public void run() {
        this.scheduledExecutor.scheduleAtFixedRate(new Task(), 0, MIGRATION_RATE_SEC, TimeUnit.SECONDS);
    }
}

class Task implements Runnable {
    private static final String[] tables = {"sflavg_tbl", 
    										"sfltojson_tbl",
    										"bactojson_tbl"};
    private static final int sstoreDBID = BigDawgConfigProperties.INSTANCE.getSStoreDBID();
    private static final int psqlDBID = BigDawgConfigProperties.INSTANCE.getSeaflowDBID();
    private static SStoreSQLConnectionInfo sstoreConnInfo;
    private static PostgreSQLConnectionInfo psqlConnInfo;

    Task(){
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
    	for (String table : tables) {
    		String tableFrom = table;
    		String tableTo = "psql_"+table;
    		try {
				Migrator.migrate(psqlConnInfo, tableFrom, sstoreConnInfo, tableTo);
			} catch (MigrationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
    
}
