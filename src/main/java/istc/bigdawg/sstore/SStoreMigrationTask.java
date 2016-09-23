/**
 * 
 */
package istc.bigdawg.sstore;

import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.exceptions.BigDawgCatalogException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.migration.FromSStoreToPostgres;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.utils.StackTrace;

/**
 * Adam: Moved the migration task for S-Store to a separate class.
 * 
 * @author Adam Dziedzic
 */
public class SStoreMigrationTask {
	
	/** log */
	private static Logger log = Logger.getLogger(SStoreMigrationTask.class);

    private final ScheduledExecutorService scheduledExecutor;
    public static final int MIGRATION_RATE_SEC = 60;
    private static final String[] tables = {"sflavg_tbl"};
    
    /* How many threads do we need to run the task? */
    private int numberOfThreads = 1;
    
	/**
	 * Create a new task for exemplar migration with S-Store.
	 */
	public SStoreMigrationTask() {
		scheduledExecutor = Executors.newScheduledThreadPool(numberOfThreads);
		cleanHistoricalData();
		waitForSStore();
	    scheduledExecutor.scheduleAtFixedRate(new Task(tables), MIGRATION_RATE_SEC, MIGRATION_RATE_SEC, TimeUnit.SECONDS);
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
	            	
	            }
	        }
	    }
	}
	
	
	public void waitForSStore() {
		final int sstoreDBID = BigDawgConfigProperties.INSTANCE.getSStoreDBID();
		try {
			SStoreSQLConnectionInfo sstoreConnInfo = 
					(SStoreSQLConnectionInfo) CatalogViewer.getConnectionInfo(sstoreDBID);
			String host = sstoreConnInfo.getHost();
	    	int port = Integer.parseInt(sstoreConnInfo.getPort());
	    	while (!serverListening(host, port)) {
	    		try {
					Thread.sleep(1000); // sleep 1 second
				} catch (InterruptedException e) {
					String msg = "Problem with the SStore migration task. "
							+ e.getMessage();
					log.error(msg + " " + StackTrace.getFullStackTrace(e));
				}
	    	}
		} catch (BigDawgCatalogException | SQLException e) {
			String msg = "Problem with PostgreSQL. "
					+ e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e));
		}
		return;
	}

	private void cleanHistoricalData() {
		// clean the historical data before any migration
    	int psqlDBID = BigDawgConfigProperties.INSTANCE.getSeaflowDBID();
    	PostgreSQLConnectionInfo psqlConnInfo = null;
    	Connection psqlConn = null;
    	try {
			psqlConnInfo =
					(PostgreSQLConnectionInfo) CatalogViewer.getConnectionInfo(psqlDBID);
			psqlConn = PostgreSQLHandler.getConnection(psqlConnInfo);
		} catch (BigDawgCatalogException | SQLException e) {
			String msg = "Problem with connection to PostgreSQL. "
					+ e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e));
		}
    	
    	for (String table : tables) {
    		String cmd = "DELETE FROM psql_" + table + " WHERE s_cruise ilike 'SCOPE_7%'";
    		try {
				PostgreSQLHandler.executeStatement(psqlConn, cmd);
			} catch (SQLException e) {
				String msg = "Problem with PostgreSQL. "
						+ e.getMessage();
				log.error(msg + " " + StackTrace.getFullStackTrace(e));
			}
    	}
	}
	
}

class Task implements Runnable {
    private static final int sstoreDBID = BigDawgConfigProperties.INSTANCE.getSStoreDBID();
    private static final int psqlDBID = BigDawgConfigProperties.INSTANCE.getSeaflowDBID();
    private SStoreSQLConnectionInfo sstoreConnInfo;
    private PostgreSQLConnectionInfo psqlConnInfo;
    private String[] tables;
    
	/** log */
	private static Logger log = Logger.getLogger(Task.class);

    Task(String[] tables){
    	this.tables = tables;
    	try {
			this.sstoreConnInfo = 
					(SStoreSQLConnectionInfo) CatalogViewer.getConnectionInfo(sstoreDBID);
	    	this.psqlConnInfo =
	        		(PostgreSQLConnectionInfo) CatalogViewer.getConnectionInfo(psqlDBID);
		} catch (BigDawgCatalogException e) {
			String msg = "Problem with the BigDAWG catalog. "
					+ e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e));
		} catch (SQLException e) {
			String msg = "Problem with the sql connection. "
					+ e.getMessage();
			log.error(msg + " " + StackTrace.getFullStackTrace(e));
		}
    }

    @Override
    public void run(){
    	for (String table : tables) {
    		String tableFrom = table;
    		String tableTo = "psql_"+table;
    		try {
    			FromSStoreToPostgres migrator = new FromSStoreToPostgres();
				migrator.migrate(sstoreConnInfo, tableFrom, psqlConnInfo, tableTo);
			} catch (MigrationException e) {
				String msg = "Problem with the migration process. "
						+ e.getMessage();
				log.error(msg + " " + StackTrace.getFullStackTrace(e));
			}
    	}
    }
    
}
