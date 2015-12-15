/**
 * 
 */
package istc.bigdawg.postgresql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.query.QueryResponseTupleList;
import istc.bigdawg.utils.ObjectMapperResource;

/**
 * @author Adam Dziedzic
 * 
 */
public class PostgreSQLHandler implements DBHandler {

	Logger log = org.apache.log4j.Logger.getLogger(PostgreSQLHandler.class
			.getName());

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
		public QueryResult(List<List<String>> rows, List<String> types,
				List<String> colNames) {
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
			return Response
					.status(500)
					.entity("Problem with query execution in Postgresql: "
							+ e.getMessage() + "; query: " + queryString)
					.build();
		}
		String messageQuery = "PostgreSQL query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageQuery);
		log.info(messageQuery);
		QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
				queryResult.getRows(), 1, 1, queryResult.getColNames(),
				queryResult.getTypes(), new Timestamp(0));
		try {
			lStartTime = System.nanoTime();
			String jsonResult = getJSONString(resp);
			String messageJSON="format JSON Java time milliseconds: "
					+ (System.nanoTime() - lStartTime) / 1000000 + ",";
			System.out.print(messageJSON);
			log.info(messageJSON);
			return Response.status(200).entity(jsonResult).build();
		} catch (JsonProcessingException e) {
			return Response
					.status(200)
					.entity("Problem with JSON Parsing for PostgreSQL: "
							+ e.getMessage()).build();
		}
	}

	private String getJSONString(QueryResponseTupleList resp)
			throws JsonProcessingException {
		String responseResult;
		try {
			responseResult = ObjectMapperResource.INSTANCE.getObjectMapper()
					.writeValueAsString(resp);
			return responseResult;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public QueryResult executeQueryPostgreSQL(final String query)
			throws SQLException {
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = PostgreSQLInstance.getConnection();
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
				if (rs != null) {
					rs.close();
				}
				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				ex.printStackTrace();
				lgr.log(Level.INFO, ex.getMessage() + "; query: " + query, ex);
				throw ex;
			}
		}
	}

	public static List<List<String>> getRows(final ResultSet rs)
			throws SQLException {
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

	public static List<String> getColumnNames(final ResultSetMetaData rsmd)
			throws SQLException {
		List<String> columnNames = new ArrayList<String>();
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			columnNames.add(rsmd.getColumnLabel(i));
		}
		return columnNames;
	}

	public static List<String> getColumnTypes(final ResultSetMetaData rsmd)
			throws SQLException {
		List<String> columnTypes = new ArrayList<String>();
		for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
			columnTypes.add(rsmd.getColumnTypeName(i));
		}
		return columnTypes;
	}

	@Override
	public Shim getShim() {
		return Shim.PSQLRELATION;
	}

}
