/**
 * 
 */
package istc.bigdawg.postgresql;

import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryClient;
import istc.bigdawg.query.QueryResponseTupleList;
import istc.bigdawg.utils.Row;
import istc.bigdawg.utils.Tuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.core.Response;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Adam Dziedzic
 * 
 */
public class PostgreSQLHandler implements DBHandler {

	Logger log = org.apache.log4j.Logger.getLogger(PostgreSQLHandler.class
			.getName());

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		Tuple.Tuple3<List<String>, List<String>, List<List<String>>> result = null;
		try {
			result = executeQueryPostgres(queryString);
		} catch (SQLException e1) {
			e1.printStackTrace();
			return Response.status(200)
					.entity("SQLException in PostgreSQL: " + e1.getMessage())
					.build();
		}
		List<String> colNames = result.getT1();
		List<String> colTypes = result.getT2();
		List<List<String>> rows = result.getT3();
		QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
				rows, 1, 1, colNames, colTypes, new Timestamp(0));
		String responseResult;
		try {
			responseResult = new ObjectMapper().writeValueAsString(resp);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return Response
					.status(200)
					.entity("Problem with JSON Parsing for PostgreSQL: "
							+ e.getMessage()).build();
		}
		return Response.status(200).entity(responseResult).build();
	}

	private Tuple.Tuple3<List<String>, List<String>, List<List<String>>> executeQueryPostgres(
			final String query) throws SQLException {
		List<String> colNames = null;
		List<String> colTypes = null;
		List<List<String>> rows = new ArrayList<List<String>>();

		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			con = PostgreSQLInstance.getConnection();
			st = con.createStatement();
			rs = st.executeQuery(query);

			if (rs == null)
				return null;
			final ResultSetMetaData rsmd = rs.getMetaData();
			colNames = Row.getColumnNames(rsmd);
			colTypes = Row.getColumnTypes(rsmd);
			List<Row> table = new ArrayList<Row>();
			Row.formTable(rs, table);
			for (Row row : table) {
				List<String> resultRowList = new ArrayList<String>();
				for (Entry<Object, Class> col : row.row) {
					// System.out.print(" > "
					// + ((col.getValue()).cast(col.getKey())));
					resultRowList.add(col.getValue().cast(col.getKey())
							.toString());
				}
				// System.out.println();
				rows.add(resultRowList);
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			ex.printStackTrace();
			lgr.log(Level.ERROR, ex.getMessage(), ex);
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
				lgr.log(Level.INFO, ex.getMessage(), ex);
				throw ex;
			}
		}
		Tuple.Tuple3<List<String>, List<String>, List<List<String>>> result = new Tuple.Tuple3<List<String>, List<String>, List<List<String>>>(
				colNames, colTypes, rows);
		return result;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

	@Override
	public Shim getShim() {
		return Shim.PSQLRELATION;
	}

}
