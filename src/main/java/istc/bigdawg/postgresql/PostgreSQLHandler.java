/**
 * 
 */
package istc.bigdawg.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;
import teddy.bigdawg.catalog.CatalogInstance;
import teddy.bigdawg.catalog.CatalogViewer;

/**
 * @author Adam Dziedzic
 * 
 */
public class PostgreSQLHandler implements DBHandler {

	private static Logger log = Logger.getLogger(PostgreSQLHandler.class.getName());
	private Connection con = null;
	private PostgreSQLConnectionInfo conInfo = null;
	private Statement st = null;
	private PreparedStatement preparedSt = null;
	private ResultSet rs = null;

	public PostgreSQLHandler(int engineId, int dbId) throws Exception {
		try {
			conInfo = CatalogViewer.getConnection(CatalogInstance.INSTANCE.getCatalog(), engineId, dbId);
		} catch (Exception e) {
			String msg = "Catalog chosen connection: " + conInfo.getHost() + " " + conInfo.getPort() + " "
					+ conInfo.getDatabase() + " " + conInfo.getUser() + " " + conInfo.getPassword() + ".";
			log.error(msg);
			e.printStackTrace();
			throw e;
		}
	}

	public PostgreSQLHandler() {
		String msg = "Default handler. PostgreSQL parameters from a file.";
		System.out.println(msg);
		log.info(msg);
	}

	private void getConnection() throws SQLException {
		if (conInfo != null) {
			con = DriverManager.getConnection(
					"jdbc:postgresql://" + conInfo.getHost() + ":" + conInfo.getPort() + "/" + conInfo.getDatabase(),
					conInfo.getUser(), conInfo.getPassword());
		} else {
			con = PostgreSQLInstance.getConnection();
		}
	}

	public class QueryResult {
		private List<List<String>> rows;
		private List<String> types;
		private List<String> colNames;

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
		public QueryResult(List<List<String>> rows, List<String> types, List<String> colNames) {
			super();
			this.rows = rows;
			this.types = types;
			this.colNames = colNames;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		long lStartTime = System.nanoTime();
		QueryResult queryResult = null;
		try {
			queryResult = executeQueryPostgreSQL(queryString);
		} catch (SQLException e) {
			return Response.status(500)
					.entity("Problem with query execution in Postgresql: " + e.getMessage() + "; query: " + queryString)
					.build();
			// return "Problem with query execution in PostgreSQL: " +
			// queryString;
		}
		String messageQuery = "PostgreSQL query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageQuery);
		log.info(messageQuery);

		/*
		 * QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
		 * queryResult.getRows(), 1, 1, queryResult.getColNames(),
		 * queryResult.getTypes(), new Timestamp(0));
		 * 
		 * lStartTime = System.nanoTime(); String jsonResult =
		 * getJSONString(resp); String messageJSON=
		 * "format JSON Java time milliseconds: " + (System.nanoTime() -
		 * lStartTime) / 1000000 + ","; System.out.print(messageJSON);
		 * log.info(messageJSON); return
		 * Response.status(200).entity(jsonResult).build();
		 */

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
		System.out.print(messageTABLE);
		log.info(messageTABLE);

		return Response.status(200).entity(out).build();
	}

	private void cleanPostgreSQLResources() throws SQLException {
		if (rs != null) {
			rs.close();
		}
		if (st != null) {
			st.close();
		}
		if (preparedSt != null) {
			preparedSt.close();
		}
		if (con != null) {
			con.close();
		}
	}

	public QueryResult executeQueryPostgreSQL(final String query) throws SQLException {
		try {
			this.getConnection();
			st = con.createStatement();
			rs = st.executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			List<String> colNames = getColumnNames(rsmd);
			List<String> types = getColumnTypes(rsmd);
			List<List<String>> rows = getRows(rs);
			return new QueryResult(rows, types, colNames);
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			ex.printStackTrace();
			lgr.log(Level.ERROR, ex.getMessage() + "; query: " + query, ex);
			throw ex;
		} finally {
			try {
				this.cleanPostgreSQLResources();
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				ex.printStackTrace();
				lgr.log(Level.INFO, ex.getMessage() + "; query: " + query, ex);
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

	public List<Integer> getPrimaryColumns(final String table) throws SQLException {
		List<Integer> primaryColNum = new ArrayList<Integer>();
		String query = "SELECT pg_attribute.attnum " + "FROM pg_index, pg_class, pg_attribute, pg_namespace " + "WHERE "
				+ "pg_class.oid = ?::regclass AND " + "indrelid = pg_class.oid AND nspname = 'public' AND "
				+ "pg_class.relnamespace = pg_namespace.oid AND " + "pg_attribute.attrelid = pg_class.oid AND "
				+ "pg_attribute.attnum = any(pg_index.indkey) AND indisprimary";
		// System.out.println(query);
		try {
			this.getConnection();
			preparedSt = con.prepareStatement(query);
			preparedSt.setString(1, table);
			rs = preparedSt.executeQuery();
			while (rs.next()) {
				// System.out.println("Primary column number: "+rs.getInt(1));
				primaryColNum.add(new Integer(rs.getInt(1)));
			}
		} finally {
			cleanPostgreSQLResources();
		}
		return primaryColNum;
	}

	@Override
	public Shim getShim() {
		return Shim.PSQLRELATION;
	}

}
