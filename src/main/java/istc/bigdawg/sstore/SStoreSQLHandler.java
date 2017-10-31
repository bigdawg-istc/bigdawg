package istc.bigdawg.sstore;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.catalog.CatalogViewer;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;
import jline.internal.Log;

public class SStoreSQLHandler implements DBHandler, Serializable {

    private static Logger log = Logger.getLogger(SStoreSQLHandler.class.getName());
    // private static int defaultSchemaServerDBID =
    // BigDawgConfigProperties.INSTANCE.getSStoreSchemaServerDBID();
    private Connection con = null;
    private ConnectionInfo conInfo = null;
    private DatabaseMetaData metaData = null;
    private Statement st = null;
    private PreparedStatement preparedSt = null;
    private ResultSet rs = null;
    private Integer rc = null;

    public SStoreSQLHandler(int dbId) throws Exception {
	try {
	    this.conInfo = CatalogViewer.getConnectionInfo(dbId);
	} catch (Exception e) {
	    String msg = "Catalog chosen connection: " + conInfo.getHost() + " " + conInfo.getPort() + " "
		    + conInfo.getUser() + " " + conInfo.getPassword() + ".";
	    log.error(msg);
	    e.printStackTrace();
	    throw e;
	}
    }

    public SStoreSQLHandler(ConnectionInfo conInfo) {
	try {
		Class.forName("org.voltdb.jdbc.Driver");
	} catch (ClassNotFoundException ex) {
		ex.printStackTrace();
		log.error("SStore jdbc driver is not in the CLASSPATH -> "
				+ ex.getMessage() + " " + StackTrace.getFullStackTrace(ex),
				ex);
		throw new RuntimeException(ex.getMessage());
	}
	this.conInfo = conInfo;
    }

    public SStoreSQLHandler() {
	String msg = "Default handler. SStoreSQL parameters from a file.";
	log.info(msg);
    }

    /**
     * Establish connection to SStoreSQL for this instance.
     * 
     * @throws SQLException
     *             if could not establish a connection
     */
    public Connection getConnection() throws SQLException {
	if (con == null) {
	    if (conInfo != null) {
		try {
		    con = getConnection(conInfo);
		} catch (SQLException e) {
		    e.printStackTrace();
		    log.error(e.getMessage() + " Could not connect to SStoreSQL database using: " + conInfo.toString(),
			    e);
		    throw e;
		}
	    } else {
		con = SStoreSQLInstance.getConnection();
	    }
	}
	return con;
    }

    public static Connection getConnection(ConnectionInfo conInfo) throws SQLException {
	if (conInfo instanceof SStoreSQLConnectionInfo) {
		return getConnection((SStoreSQLConnectionInfo) conInfo);
	} throw new IllegalArgumentException("The conInfo parameter should represent a connection to SStore.");
    }

    public static Connection getConnection(SStoreSQLConnectionInfo conInfo) throws SQLException {
	Connection con;
	String url = conInfo.getUrl();
	String user = conInfo.getUser();
	String password = conInfo.getPassword();
	try {
            Class.forName("org.voltdb.jdbc.Driver");
	} catch (ClassNotFoundException ex) {
		ex.printStackTrace();
		log.error("SStore jdbc driver is not in the CLASSPATH -> "
				+ ex.getMessage() + " " + StackTrace.getFullStackTrace(ex),
				ex);
	}
	try {
	    con = DriverManager.getConnection(url);
	} catch (SQLException e) {
	    String msg = "Could not connect to the SStoreSQL instance: Url: " + url + " User: " + user + " Password: "
		    + password;
	    log.error(msg);
	    e.printStackTrace();
	    throw e;
	}
	return con;
    }

    public class SStoreQueryResult implements QueryResult {
	private List<List<String>> rows;
	private List<String> types;
	private List<String> colNames;
	private ConnectionInfo connInfo;

	/**
	 * @return the rows
	 */
	public List<List<String>> getRows() {
	    return rows;
	}

	/**
	 * @return the types
	 */
	public List<String> getTypes() {
	    return types;
	}

	/**
	 * @return the colNames
	 */
	public List<String> getColNames() {
	    return colNames;
	}

	/**
	 * @param rows
	 * @param types
	 * @param colNames
	 */
	public SStoreQueryResult(List<List<String>> rows, List<String> types, List<String> colNames) {
	    super();
	    this.rows = rows;
	    this.types = types;
	    this.colNames = colNames;
	    this.connInfo = null;
	}
	
	public SStoreQueryResult(List<List<String>> rows, List<String> types, List<String> colNames, ConnectionInfo ci) {
		this(rows,types,colNames);
		this.connInfo = ci;
	}

	@Override
	public String toPrettyString() {
		StringBuilder sb = new StringBuilder();
		String pattern = "^[+-]?([0-9]*[.])?[0-9]+$";
	    Pattern p = Pattern.compile(pattern);
		sb.append('[');
		
		for (List<String> r : rows) {
			sb.append('{');
			for(int i = 0; i<r.size();i++){
				sb.append('\"');
				sb.append(colNames.get(i).toLowerCase());
				sb.append('\"').append(':');
			    Matcher m = p.matcher(r.get(i));
			    if(m.find()){
			    	sb.append(r.get(i));
			    } else {
			    	sb.append('\"');
			    	sb.append(r.get(i));
			    	sb.append('\"');
			    }
				
				sb.append(',');
			}
			if (sb.length() > 0) sb.deleteCharAt(sb.length()-1);
			sb.append('}').append(',');
		}
		if (sb.length() > 1) sb.deleteCharAt(sb.length()-1);
		sb.append(']');
		return sb.toString();
	}

	@Override
	public ConnectionInfo getConnectionInfo() {
		return this.connInfo;
	}

    }
    
    public Long getEarliestTimestamp(String queryString) {
    	SStoreQueryResult queryResult = null;
    	Long earliestTimestamp = 0L;
    	try {
    	    queryResult = (SStoreQueryResult)executeQuerySStoreSQL(queryString);
    	} catch (SQLException e) {
    	    // return "Problem with query execution in SStoreSQL: " +
    	    // queryString;
    	}

    	Integer rowCounter = 1;
    	for (List<String> row : queryResult.getRows()) {
    	    for (String s : row) {
    	    	earliestTimestamp = Long.parseLong(s);
    	    }
    	}
    	return earliestTimestamp;
    }

    @Override
    public Response executeQuery(String queryString) {
	long lStartTime = System.nanoTime();
	SStoreQueryResult queryResult = null;
	try {
	    queryResult = (SStoreQueryResult)executeQuerySStoreSQL(queryString);
	} catch (SQLException e) {
	    return Response.status(500)
		    .entity("Problem with query execution in SSToreSQL: " + e.getMessage() + "; query: " + queryString)
		    .build();
	    // return "Problem with query execution in SStoreSQL: " +
	    // queryString;
	}
	String messageQuery = "SSToreSQL query execution time milliseconds: "
		+ (System.nanoTime() - lStartTime) / 1000000 + ",";
	log.info(messageQuery);

	lStartTime = System.nanoTime();

	String out = "";
	for (String name : queryResult.getColNames()) {
	    out = out + "\t" + name;
	}
	out = out + "\n";
	Integer rowCounter = 1;
	for (List<String> row : queryResult.getRows()) {
	    out = out + rowCounter.toString() + ".";
	    for (String s : row) {
		out = out + "\t" + s;
	    }
	    out = out + "\n";
	    rowCounter += 1;
	}

	String messageTABLE = "format TABLE Java time milliseconds: " + (System.nanoTime() - lStartTime) / 1000000
		+ ",";
	log.info(messageTABLE);

	return Response.status(200).entity(out).build();
    }
    
    public Response executeUpdateQuery(String queryString) throws SQLException {
	long lStartTime = System.nanoTime();
	try {
	    executeSStoreUpdateSQL(queryString);
	} catch (SQLException e) {
		throw e;
//	    return Response.status(500)
//		    .entity("Problem with query execution in SSToreSQL: " + e.getMessage() + "; query: " + queryString)
//		    .build();
	}
	String messageQuery = "SSToreSQL query execution time milliseconds: "
		+ (System.nanoTime() - lStartTime) / 1000000 + ",";
	log.info(messageQuery);

	lStartTime = System.nanoTime();

	String messageTABLE = "format TABLE Java time milliseconds: " + (System.nanoTime() - lStartTime) / 1000000
		+ ",";
	log.info(messageTABLE);

	return Response.status(200).entity("update").build();
    }


    public void close() {
    	try {
        cleanSStoreSQLResources();
    	} catch (SQLException e) {
    		Log.info("Failed to clean S-Store resource.");
    	}
    }
    

    /**
     * Clean resource after a query/statement was executed in SStoreSQL.
     * 
     * @throws SQLException
     */
    private void cleanSStoreSQLResources() throws SQLException {
	if (rs != null) {
	    rs.close();
	    rs = null;
	}
	if (st != null) {
	    st.close();
	    st = null;
	}
	if (preparedSt != null) {
	    preparedSt.close();
	    preparedSt = null;
	}
	if (con != null) {
	    con.close();
	    con = null;
	}
    }

    /**
     * It executes the query and releases the resources at the end.
     * 
     * @param query
     * @return #QueryResult
     * @throws SQLException
     */
    public QueryResult executeQuerySStoreSQL(final String query) throws SQLException {
	try {
	    this.getConnection();

	    log.debug("\n\nquery: " + LogUtils.replace(query) + "");
	    if (this.conInfo != null) {
		log.debug("ConnectionInfo: " + this.conInfo.toString() + "\n");
	    }

	    st = con.createStatement();
	    rs = st.executeQuery(query);

	    ResultSetMetaData rsmd = rs.getMetaData();
	    List<String> colNames = getColumnNames(rsmd);
	    List<String> types = getColumnTypes(rsmd);
	    List<List<String>> rows = getRows(rs);
	    return new SStoreQueryResult(rows, types, colNames);
	} catch (SQLException ex) {
	    Logger lgr = Logger.getLogger(QueryClient.class.getName());
	    // ex.printStackTrace();
	    lgr.log(Level.ERROR, ex.getMessage() + "; query: " + LogUtils.replace(query), ex);
	    throw ex;
	} finally {
	    try {
		this.cleanSStoreSQLResources();
	    } catch (SQLException ex) {
		Logger lgr = Logger.getLogger(QueryClient.class.getName());
		// ex.printStackTrace();
		lgr.log(Level.INFO, ex.getMessage() + "; query: " + LogUtils.replace(query), ex);
		throw ex;
	    }
	}
    }

    /**
     * It executes the update query and releases the resources at the end.
     * 
     * @param query
     * @return #QueryResult
     * @throws SQLException
     */
    public int executeSStoreUpdateSQL(final String query) throws SQLException {
	try {
	    this.getConnection();

	    log.debug("\n\nquery: " + LogUtils.replace(query) + "");
	    if (this.conInfo != null) {
		log.debug("ConnectionInfo: " + this.conInfo.toString() + "\n");
	    }

	    st = con.createStatement();
	    rc = st.executeUpdate(query);

	    return rc;
	} catch (SQLException ex) {
	    Logger lgr = Logger.getLogger(QueryClient.class.getName());
	    // ex.printStackTrace();
	    lgr.log(Level.ERROR, ex.getMessage() + "; query: " + LogUtils.replace(query), ex);
	    throw ex;
	} finally {
	    try {
		this.cleanSStoreSQLResources();
	    } catch (SQLException ex) {
		Logger lgr = Logger.getLogger(QueryClient.class.getName());
		// ex.printStackTrace();
		lgr.log(Level.INFO, ex.getMessage() + "; query: " + LogUtils.replace(query), ex);
		throw ex;
	    }
	}
    }

    public static List<List<String>> getRows(final ResultSet rs) throws SQLException {
	if (rs == null) {
	    return null;
	}
	List<List<String>> rows = new ArrayList<>();
	try {
	    ResultSetMetaData rsmd = rs.getMetaData();
	    int NumOfCol = rsmd.getColumnCount();
	    while (rs.next()) {
		List<String> current_row = new ArrayList<String>();
		for (int i = 1; i <= NumOfCol; i++) {
		    Object value = rs.getObject(i);
		    if (value == null) {
			current_row.add("null");
		    } else {
			current_row.add(value.toString());
		    }
		}
		rows.add(current_row);
	    }
	    return rows;
	} catch (SQLException e) {
	    throw e;
	}
    }

    public static List<String> getColumnNames(final ResultSetMetaData rsmd) throws SQLException {
	List<String> columnNames = new ArrayList<String>();
	for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
	    columnNames.add(rsmd.getColumnLabel(i));
	}
	return columnNames;
    }

    public static List<String> getColumnTypes(final ResultSetMetaData rsmd) throws SQLException {
	List<String> columnTypes = new ArrayList<String>();
	for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
	    columnTypes.add(rsmd.getColumnTypeName(i));
	}
	return columnTypes;
    }

    @Override
    public Shim getShim() {
	return Shim.SSTORESQLRELATION;
    }

    /**
     * Get metadata about columns (right now, just has column name, data type) for a
     * table in SStoreQL.
     * 
     * @param conInfo
     * @param tableName
     * @return map column name to column meta data
     * @throws SQLException
     *             if the data extraction from SStoreSQL failed
     */
    public SStoreSQLTableMetaData getColumnsMetaData(String tableName) throws SQLException {
	try {
	    this.getConnection();
	    try {
    	    	metaData = con.getMetaData();
	    } catch(SQLException e) {
		e.printStackTrace();
		log.error("PostgreSQLHandler, the query preparation failed. " + e.getMessage() + " "
				+ StackTrace.getFullStackTrace(e));
		throw e;
	    }
    	    ResultSet rs = metaData.getColumns(null, null, tableName, null);
    	    
    	    Map<String, SStoreSQLColumnMetaData> columnsMap = new HashMap<>();
    	    List<SStoreSQLColumnMetaData> columnsOrdered = new ArrayList<>();
    	    while (rs.next()) {
    		SStoreSQLColumnMetaData columnMetaData = new SStoreSQLColumnMetaData(rs.getString("COLUMN_NAME"), 
    			rs.getString("TYPE_NAME"), rs.getBoolean("IS_NULLABLE"), rs.getInt("RELATIVE_INDEX"),
    			rs.getInt("COLUMN_SIZE"));
    		columnsMap.put(rs.getString("COLUMN_NAME"), columnMetaData);
    		columnsOrdered.add(columnMetaData);
    	    }
	    
	    return new SStoreSQLTableMetaData(columnsMap, columnsOrdered);
	} finally {
	    try {
		this.cleanSStoreSQLResources();
	    } catch (SQLException ex) {
		ex.printStackTrace();
		log.error(ex.getMessage() + "; conInfo: " + conInfo.toString() + "; table: " + tableName + " "
			+ StackTrace.getFullStackTrace(ex), ex);
		throw ex;
	    }
	}
    }

    public QueryResult executePreparedStatement(Connection connection, List<String> parameters) throws SQLException, BigDawgException{
    	
    	PreparedStatement statement = null;
    	
    	List<List<String>> rows = new ArrayList<>();
    	List<String> types = new ArrayList<>();
    	List<String> colNames = new ArrayList<>();
    	
    	String procCommand;
		
		String params = "";
		String procName = parameters.get(0);
		if (parameters.size() > 1) {
			params = "(";
			for (String parameter : parameters.subList(1, parameters.size())) {
				params += "?,"; 
			}
			params = params.substring(0, params.length()-1);
			params += ")";
		}
		procCommand = String.format("{call %s%s}", procName, params);
    	
    	try {
		log.info("Procedure to execute: " + procCommand);
    		statement = connection.prepareCall(procCommand);
    		setProcParams(procName, statement, parameters);
    		
    		ResultSet rs = statement.executeQuery();
    		int colCount = 0;
    		if (rs.next()) {
    			colCount = rs.getMetaData().getColumnCount();
    			rows.add(new ArrayList<>());
    			for (int i = 1; i <= colCount; i++) {
    				rows.get(rows.size() - 1).add(rs.getObject(i).toString());
    				colNames.add(rs.getMetaData().getColumnLabel(i));
    				types.add(rs.getMetaData().getColumnTypeName(i));
    			}
    		}
    		while (rs.next()) {
    			rows.add(new ArrayList<>());
    			for (int i = 1; i <= colCount; i++)
    				rows.get(rows.size() - 1).add(rs.getObject(i).toString()); 
    		}
    		
    		rs.close();
    		statement.close();
    	} catch (SQLException ex) {
    		ex.printStackTrace();
    		// remove ' from the statement - otherwise it won't be inserted into
    		// log table in Postgres
    		log.error(ex.getMessage() + "; statement to be executed: " + LogUtils.replace(procCommand) + " "
    				+ ex.getStackTrace(), ex);
    		throw ex;
    	} finally {
    		if (statement != null) {
    			statement.close();
    		}
    	}
    	
    	return new SStoreQueryResult(rows, types, colNames);
    }

    
    
	private void setProcParams(String procName, PreparedStatement statement,
			List<String> parameters) throws SQLException, BigDawgException {
		if (parameters.size() < 2) {
			return;
		}
		
		ArrayList<String> dataTypes = new ArrayList<String>();
		try {
			dataTypes = CatalogViewer.getProcParamTypes(procName);
		} catch (Exception e1) {
			log.error("Cannot get data types for procedure " + procName);
			throw new BigDawgException("Cannot get data types for procedure " + procName);
		}
		for (int i = 1; i < parameters.size(); i++) {
			String dataType;
			dataType = dataTypes.get(i-1).substring(0, 3);
			if (dataType.equalsIgnoreCase("dou")) {
				statement.setDouble(i, Double.parseDouble(parameters.get(i)));
			} else if (dataType.equalsIgnoreCase("flo")) {
				statement.setFloat(i, Float.parseFloat(parameters.get(i)));
			} else if (dataType.equalsIgnoreCase("lon")) {
				statement.setLong(i, Long.parseLong(parameters.get(i)));
			} else if (dataType.equalsIgnoreCase("int")) {
				statement.setInt(i, Integer.parseInt(parameters.get(i)));
			} else if (dataType.equalsIgnoreCase("boo")) {
				statement.setBoolean(i, Boolean.parseBoolean(parameters.get(i)));
			} else if (dataType.equalsIgnoreCase("str")) {
				statement.setString(i, parameters.get(i));
			} else {
				throw new BigDawgException("Unsupported data type: "+parameters.get(i));
			}

		}
		
	};
	
	
	    
    public static Long executePreparedStatement(Connection connection, String copyFromString, String tableName,
	    String trim, String outputFile, boolean caching) throws SQLException {
	
	PreparedStatement statement = null;
	Long countExtractedRows = 0L;
	try {
		statement = connection.prepareCall(copyFromString);
		statement.setInt(1, 0);
		statement.setString(2, tableName);
		statement.setString(3, trim);
		statement.setString(4, outputFile);
		statement.setString(5, String.valueOf(caching));
		statement.setQueryTimeout(600);
		ResultSet rs;
		rs = statement.executeQuery();
		rs.next();
		countExtractedRows = rs.getLong(1);
		rs.close();
		statement.close();
	} catch (SQLException ex) {
		ex.printStackTrace();
		// remove ' from the statement - otherwise it won't be inserted into
		// log table in Postgres
		log.error(ex.getMessage() + "; statement to be executed: " + LogUtils.replace(copyFromString) + " "
				+ ex.getStackTrace(), ex);
		throw ex;
	} finally {
		if (statement != null) {
			statement.close();
		}
	}
	return countExtractedRows;
    }
    
    public static String getExportCommand() {
	return "{call @ExtractionRemote(?, ?, ?, ?, ?)}";
    }
    
    public static Long executePreparedImportStatement(
    		Connection connection, String copyToString, 
    		String tableName, InputStream inStream, String trim, String inputFile) throws SQLException {

    	PreparedStatement statement = null;
    	Long countLoadedRows = 0L;
    	try {
    		statement = connection.prepareCall(copyToString);
    		statement.setInt(1, 0);
    		statement.setString(2, tableName);
    		statement.setString(3, trim);
    		statement.setString(4, inputFile);
    		statement.setQueryTimeout(600);
    		ResultSet rs = statement.executeQuery();
    		rs.next();
    		countLoadedRows = rs.getLong(1);
    		rs.close();
    		statement.close();
    	} catch (SQLException ex) {
    		if (ex.getMessage().startsWith("Connection failure: 'Interrupted while waiting for response'")) {
    			// Temporary solution:
    			// This exception is thrown because we use socket communication for migration 
    			//     from Postgres and S-Store, and S-Store doesn't like it. 
    			//     But it doesn't seem to affect the loading process.
    			;
    		} else {
    			ex.printStackTrace();
    			// remove ' from the statement - otherwise it won't be inserted into
    			// log table in Postgres
    			log.error(ex.getMessage() + "; statement to be executed: " + LogUtils.replace(copyToString) + " "
    					+ ex.getStackTrace(), ex);
    			throw ex;
    		}
    	} finally {
    		if (statement != null) {
    			statement.close();
    		}
    	}
    	return countLoadedRows;
    }
        
   public static String getImportCommand() {
    	return "{call @LoadTableFromFile(?, ?, ?, ?)}";
    }

   public boolean existsObject(String name) throws Exception {
	   // TODO:
	   return true;
   }
   
   public ObjectMetaData getObjectMetaData(String name) throws Exception {
	   // TODO:
	   return null;
   }
   
   public Connection getCon() {
	   return con;
   }
   
}
