package istc.bigdawg.accumulo;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.ConstructedQueryResult;
import istc.bigdawg.executor.ExecutorEngine;
import istc.bigdawg.executor.IslandQueryResult;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.TheObjectThatResolvesAllDifferencesAmongTheIslands;
import istc.bigdawg.islands.operators.Operator;
import istc.bigdawg.islands.text.operators.TextOperator;
import istc.bigdawg.islands.text.operators.TextScan;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.utils.LogUtils;

public class AccumuloExecutionEngine implements ExecutorEngine, DBHandler {

	private static Logger log = Logger
			.getLogger(AccumuloExecutionEngine.class.getName());
	
	private static Map<String, Operator> execution = new HashMap<>();
	private ZooKeeperInstance zkInstance;
	private Connector conn;
	
//	private static final long MAX_MEMORY = 50 * 1024 * 1024L;
//	private static final int MAX_THREADS = 4;
	
	public static TextScan getExecutionOperator(String s) {
		return (TextScan)execution.get(s);
	}
	
	private ConnectionInfo ci = null;
	
//	private static Pair<String, Range> allThatNeedsMigration = null;
//	
//	public static Pair<String, Range> getAllTablesThatNeedMigration() {
//		Pair<String, Range> thingToMigrate = allThatNeedsMigration;
//		allThatNeedsMigration = null;
//		return thingToMigrate;
//	}
	
//	public AccumuloExecutionEngine() throws AccumuloException, AccumuloSecurityException{
//
//		this.ci = new AccumuloConnectionInfo("localhost", "2333", "classdb55", "AccumuloUser", "bJsTTwyiMjbK%1PMEh2hioMK@");
//		
//		ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(this.ci.getDatabase()).withZkHosts(this.ci.getUrl());
//		String username = this.ci.getUser();
//		PasswordToken auth = new PasswordToken(this.ci.getPassword());
//		
//		//scan -t bk0802_oTsampleDegree -e S0100
//      
//      	zkInstance = new ZooKeeperInstance(cc);
//		conn = zkInstance.getConnector(username, auth);
//
//	};

	
	public AccumuloExecutionEngine(ConnectionInfo zooKeeperConnectionInfo) throws BigDawgException, AccumuloException, AccumuloSecurityException {
		this.ci = zooKeeperConnectionInfo;
		
		ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(this.ci.getDatabase()).withZkHosts(this.ci.getUrl());
		String username = this.ci.getUser();
		PasswordToken auth = new PasswordToken(this.ci.getPassword());
		
		//scan -t bk0802_oTsampleDegree -e S0100
      
      	zkInstance = new ZooKeeperInstance(cc);
		conn = zkInstance.getConnector(username, auth);
//		conn.tableOperations().delete(tableName);
	};
	
	public static void addExecutionTree(String queryIdentifier, Operator queryRootOperator) {
		execution.put(queryIdentifier, queryRootOperator);
	}
	
	public void dropDataSetIfExists(String dataSetName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (conn.tableOperations().exists(dataSetName)) {
			conn.tableOperations().delete(dataSetName);	
		} else {
			log.debug("Accumulo Table "+dataSetName+" does not exist; skip dropping."); 
		}
	}
	
	public Optional<QueryResult> execute(final String query) throws LocalQueryExecutionException {
		try {
			log.debug("AccumuloExecutionEngine is attempting to retrieve query: " + LogUtils.replace(query) );
			
			TextOperator op = (TextOperator) execution.get(query);
			if (op == null) {
				if (query.startsWith(TheObjectThatResolvesAllDifferencesAmongTheIslands.AccumuloCreateTableCommandPrefix)) {
					String[] splits = query.split("[ ]");
					assert(splits.length == 2);
					conn.tableOperations().create(splits[1]);
					return Optional.of(new IslandQueryResult(ci));
				} else if (query.startsWith(TheObjectThatResolvesAllDifferencesAmongTheIslands.AccumuloDeleteTableCommandPrefix)) {
					String[] splits = query.split("[ ]");
					assert(splits.length == 2);
					dropDataSetIfExists(splits[1]);
//					conn.tableOperations().delete(splits[1]);
					return Optional.of(new IslandQueryResult(ci));
				} else if (query.startsWith(TheObjectThatResolvesAllDifferencesAmongTheIslands.AccumuloTempTableCommandPrefix)) {
					
					// don't do shit but
//					// leave a note, saying that this specific thing needs to be migrated
//					String[] splits = query.split("[ ]");
//					assert(splits.length == 2);
//					op = (TextOperator) execution.get(splits[1]);
//					assert(op != null && op instanceof TextScan);
//					allThatNeedsMigration = new ImmutablePair<>(splits[1], ((TextScan)op).getRange());
					
					// this scanner into writer thing don't work
//					conn.tableOperations().create(splits[1]);
//					BatchScanner scanner = conn.createBatchScanner(splits[1], Authorizations.EMPTY, 1);
//					scanner.setRanges(Collections.singleton(scan.getRange()));
//					scanner.addScanIterator(new IteratorSetting(0, query, null));
//					BatchWriterConfig conf = new BatchWriterConfig();
//					conf.setMaxMemory(MAX_MEMORY);
//					conf.setMaxWriteThreads(MAX_THREADS);
//					BatchWriter bw = conn.createBatchWriter(splits[1], conf);
//					
//					bw.addMutations(iterable);
					return Optional.of(new IslandQueryResult(ci));
				} else 
					throw new LocalQueryExecutionException("Unsupported TEXT island query: "+query);
			}
			
			log.debug("AccumuloExecutionEngine retrieved the following query: " + op.getTreeRepresentation(true));
			log.debug("ConnectionInfo:\n" + this.ci.toString());

			if (!(op instanceof TextScan))
				throw new LocalQueryExecutionException("Unsupported Accumulo Operator Type: "+op.getClass().getName());
			
			TextScan scan = (TextScan) op;
			String tableName = scan.getSourceTableName();
			Range r = scan.getRange();
			
			BatchScanner scanner = conn.createBatchScanner(tableName, Authorizations.EMPTY, 1);
	      
			List<List<String>> result = new ArrayList<>();			
			
			scanner.setRanges(Collections.singleton(r));
			for (Map.Entry<Key, Value> entry : scanner) {
				List<String> row = new ArrayList<>();
				row.add(entry.getKey().getRow().toString());

				String column = "";
				if (entry.getKey().getColumnFamily() != null) {
					column += entry.getKey().getColumnFamily().toString();
				}
				column += ":";
				if (entry.getKey().getColumnQualifier() != null) {
					column += entry.getKey().getColumnQualifier().toString();
				}
				if (column.length() > 1) row.add(column);
				
				row.add(entry.getValue().toString());
				result.add(row);
			}
			scanner.close();
			if (result.isEmpty()) result.add(new ArrayList<>());
			
			return Optional.of(new ConstructedQueryResult(result, ci));
//			this.getConnection();
//			st = con.createStatement();
//			if (st.execute(query)) {
//				rs = st.getResultSet();
//				return Optional.of(new JdbcQueryResult(rs, this.conInfo));
//			} else {
//				return Optional.of(new IslandQueryResult(this.conInfo));
//			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			// ex.printStackTrace();
			lgr.log(Level.ERROR,
					ex.getMessage() + "; query: " + LogUtils.replace(query),
					ex);
			throw new LocalQueryExecutionException(ex);
		} finally {
//			try {
//				this.cleanPostgreSQLResources();
//			} catch (SQLException ex) {
//				Logger lgr = Logger.getLogger(QueryClient.class.getName());
//				// ex.printStackTrace();
//				lgr.log(Level.INFO,
//						ex.getMessage() + "; query: " + LogUtils.replace(query),
//						ex);
//				throw new LocalQueryExecutionException(ex);
//			}
		}
	}
	
	public void createTable(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
		conn.tableOperations().create(tableName);
	};
	
	public void deleteTable(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		this.dropDataSetIfExists(tableName);
//		conn.tableOperations().delete(tableName);
	}

	@Override
	public Response executeQuery(String queryString) {
		return null;
	}

	@Override
	public Shim getShim() {
		return null;
	}

	@Override
	public ObjectMetaData getObjectMetaData(String name) throws Exception {
		return null;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return null;
	}

	@Override
	public boolean existsObject(String name) throws Exception {
		return false;
	}

	@Override
	public void close() throws Exception {
	}

}
