/**

 * 
 */
package istc.bigdawg.query;

import istc.bigdawg.utils.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import javassist.expr.NewArray;

import javax.swing.ListModel;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.annotation.JsonFormat.Value;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

/**
 * @author Adam Dziedzic tests: 1) curl -v -H "Content-Type: application/json"
 *         -X POST -d '{"query":"this is a
 *         query","authorization":{},"tuplesPerPage
 *         ":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}'
 *         http://localhost:8080/bigdawg/query 2) curl -v -H
 *         "Content-Type: application/json" -X POST -d '{"query":"select
 *         version(
 *         )","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp
 *         ":"2012-04-23T18:25:43.511Z"}' http://localhost:8080/bigdawg/query 3)
 *         curl -v -H "Content-Type: application/json" -X POST -d
 *         '{"query":"select * from
 *         authors","authorization":{},"tuplesPerPage":1,"
 *         pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}'
 *         http://localhost:8080/bigdawg/query
 */
@Path("/")
public class QueryClient {
	private Connection con = null;
	private Statement st = null;
	private ResultSet rs = null;
	private PreparedStatement pst = null;

	private String url = "jdbc:postgresql://localhost/testdb";
	private String user = "test";
	private String password = "test";

	/**
	 * Answer a query from a client.
	 * 
	 * @param istream
	 * @return
	 */
	@Path("query")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response query(String istream) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			RegisterQueryRequest st = mapper.readValue(istream,
					RegisterQueryRequest.class);
			System.out.println(mapper.writeValueAsString(st));
			Entry<List<String>, List<List<String>>> result = executeQueryPostgres(st
					.getQuery());
			List<String> colNames = result.getKey();
			List<List<String>> rows = result.getValue();
			RegisterQueryResponse resp = new RegisterQueryResponse("OK", 200,
					rows, 1, 1, colNames, new Timestamp(0));
			String responseResult = mapper.writeValueAsString(resp);
			return Response.status(200).entity(responseResult).build();
		} catch (UnrecognizedPropertyException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(500).entity("yikes").build();
		}
	}

	private Entry<List<String>, List<List<String>>> executeQueryPostgres(
			final String query) {
		List<String> colNames = null;
		List<List<String>> rows = new ArrayList<List<String>>();
		try {
			con = DriverManager.getConnection(url, user, password);
			st = con.createStatement();
			rs = st.executeQuery(query);
			colNames = Row.getColumnNames(rs);
			List<Row> table = new ArrayList<Row>();
			Row.formTable(rs, table);
			for (Row row : table) {
				List<String> resultRowList = new ArrayList<String>();
				for (Entry<Object, Class> col : row.row) {
					System.out.print(" > "
							+ ((col.getValue()).cast(col.getKey())));
					resultRowList.add(col.getValue().cast(col.getKey())
							.toString());
				}
				System.out.println();
				rows.add(resultRowList);
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
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
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		Entry<List<String>, List<List<String>>> result = new AbstractMap.SimpleEntry<List<String>, List<List<String>>>(
				colNames, rows);
		return result;
	}

	private void createAuthor(final String author) {
		try {
			con = DriverManager.getConnection(url, user, password);

			String stm = "INSERT INTO authors(name) VALUES(?)";
			pst = con.prepareStatement(stm);
			pst.setString(1, author);
			pst.executeUpdate();

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(QueryClient.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {

			try {
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(QueryClient.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}
	}

	public static void main(String[] args) {
		QueryClient qClient = new QueryClient();
		// qClient.executeQueryPostgres("Select * from books");
		qClient.query("{\"query\":\"select * from authors\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
	}
}