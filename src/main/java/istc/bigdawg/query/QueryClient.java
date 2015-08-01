/**

 *
 */
package istc.bigdawg.query;

import istc.bigdawg.BDConstants;
import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.exceptions.ShellScriptException;
import istc.bigdawg.myria.MyriaClient;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.parser.Parser;
import istc.bigdawg.query.parser.simpleParser;
import istc.bigdawg.scidb.ParseSciDBResponse;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.Row;
import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.Tuple;
import istc.bigdawg.utils.Tuple.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

/**
 * @author Adam Dziedzic
 * 
 *         tests: 1) curl -v -H "Content-Type: application/json" -X POST -d
 *         '{"query":"this is a query","authorization":{},"tuplesPerPage
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

	static org.apache.log4j.Logger log = org.apache.log4j.Logger
			.getLogger(QueryClient.class.getName());

	private Connection con = null;
	private Statement st = null;
	private ResultSet rs = null;
	private PreparedStatement pst = null;

	/**
	 * Answer a query from a client.
	 * 
	 * @param istream
	 * @return
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 * @throws ShellScriptException
	 * @throws InterruptedException
	 */
	@Path("query")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response query(String istream) throws TableNotFoundException,
			AccumuloException, AccumuloSecurityException, InterruptedException,
			ShellScriptException {
		// System.out.println(istream);
		log.info("istream: " + istream);
		ObjectMapper mapper = new ObjectMapper();
		try {
			QueryRequest st = mapper.readValue(istream,
					QueryRequest.class);
			System.out.println(mapper.writeValueAsString(st));
			Parser parser = new simpleParser();
			ASTNode parsed = parser.parseQueryIntoTree(st.getQuery());
			System.out.println(parsed.getShim());
			String queryString = parsed.getTarget();
			if (parsed.getShim() == BDConstants.Shim.ACCUMULOTEXT) {
				String[] params = queryString.split(" ");
				String database = params[0];
				String table = params[1];
				String query = params[2];
				System.out.println("databse: " + database + " table: " + table
						+ " query: " + query);
				return Response
						.status(200)
						.entity(executeAccumuloShellScript(database, table,
								query)).build();
			} else if (parsed.getShim() == BDConstants.Shim.PSQLRELATION) {
				Tuple.Tuple3<List<String>, List<String>, List<List<String>>> result = executeQueryPostgres(queryString);
				List<String> colNames = result.getT1();
				List<String> colTypes = result.getT2();
				List<List<String>> rows = result.getT3();
				QueryResponseTupleList resp = new QueryResponseTupleList(
						"OK", 200, rows, 1, 1, colNames, colTypes,
						new Timestamp(0));
				String responseResult = mapper.writeValueAsString(resp);
				return Response.status(200).entity(responseResult).build();
			} else if (parsed.getShim() == BDConstants.Shim.MYRIA) {
				String resultMyria = getMyriaData(queryString);
				return Response.status(200).entity(resultMyria).build();
			} else if (parsed.getShim() == BDConstants.Shim.PSQLARRAY) {
				System.out.println("run query for SciDB");
				System.out.println("SciDB queryString: "+queryString);
				String resultSciDB;
				try {
					resultSciDB = executeQueryScidb(queryString);
					return Response.status(200).entity(resultSciDB).build();
				} catch (SciDBException e) {
					e.printStackTrace();
					String messageSciDB = "Problem with SciDB: "+e.getMessage();
					log.error(messageSciDB);
					return Response.status(200).entity(messageSciDB).build();
				}
			} else {
				QueryResponseTupleList resp = new QueryResponseTupleList(
						"ERROR: Unrecognized shim "
								+ parsed.getShim().toString(), 412, null, 1, 1,
						null, null, new Timestamp(0));
				String responseResult = mapper.writeValueAsString(resp);
				return Response.status(412).entity(responseResult).build();
			}
		} catch (NotSupportIslandException e) {
			e.printStackTrace();
			return Response.status(412).entity(e.getMessage()).build();
		} catch (UnrecognizedPropertyException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(500).entity("yikes").build();
		} catch (SQLException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
		}
	}
	
	private String executeQueryScidb(String queryString) throws IOException, InterruptedException, SciDBException {
		String sciDBHostname = BigDawgConfigProperties.INSTANCE.getScidbHostname();
		String sciDBUser = BigDawgConfigProperties.INSTANCE.getScidbUser();
		String sciDBPassword = BigDawgConfigProperties.INSTANCE.getScidbPassword();
		System.out.println("sciDBHostname: "+sciDBHostname);
		System.out.println("sciDBUser: "+sciDBUser);
		System.out.println("sciDBPassword: "+sciDBPassword);
		InputStream resultInStream=RunShell.runSciDB(sciDBHostname,queryString);
		String resultString = IOUtils.toString(resultInStream,
				Constants.ENCODING);
		Tuple2<List<String>, List<List<String>>> parsedData = ParseSciDBResponse.parse(resultString);
				List<String> colNames = parsedData.getT1();
				List<List<String>> tuples = parsedData.getT2();
				QueryResponseTupleList resp = new QueryResponseTupleList(
						"OK", 200, tuples, 1, 1, colNames, new ArrayList<String>(),
						new Timestamp(0));
				String responseResult =new ObjectMapper().writeValueAsString(resp);
		return responseResult;
	}

	private Tuple.Tuple3<List<String>, List<String>, List<List<String>>> executeQueryPostgres(
			final String query) throws SQLException {
		List<String> colNames = null;
		List<String> colTypes = null;
		List<List<String>> rows = new ArrayList<List<String>>();
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
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
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
				lgr.log(Level.WARNING, ex.getMessage(), ex);
				throw ex;
			}
		}
		Tuple.Tuple3<List<String>, List<String>, List<List<String>>> result = new Tuple.Tuple3<List<String>, List<String>, List<List<String>>>(
				colNames, colTypes, rows);
		return result;
	}

	private void createAuthor(final String author) {
		try {
			con = PostgreSQLInstance.getConnection();

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

	public String getMyriaData(String query) {
		String myriaResult;
		try {
			myriaResult = MyriaClient.execute(query);
		} catch (MyriaException e) {
			return e.getMessage();
		}
		QueryResponseTupleString resp = new QueryResponseTupleString(
				"OK", 200, myriaResult, 1, 1, new ArrayList<String>(),
				new ArrayList<String>(), new Timestamp(0));
		ObjectMapper mapper = new ObjectMapper();
		String responseResult;
		try {
			responseResult = mapper.writeValueAsString(resp).replace("\\", "");
			return responseResult;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
			String message = "JSON processing error for Myria.";
			log.error(message);
			return message;
		}
	}

	public String executeAccumuloShellScript(String database, String table,
			String query) throws IOException, InterruptedException,
			ShellScriptException {
		String accumuloScriptPath = BigDawgConfigProperties.INSTANCE
				.getAccumuloShellScript();
		System.out.println("accumuloScriptPath: " + accumuloScriptPath);
		InputStream scriptResultInStream = RunShell.run(
				accumuloScriptPath, database, table, query);
		String scriptResult = IOUtils.toString(scriptResultInStream,
				Constants.ENCODING);
		System.out.println("Accumulo script result: " + scriptResult);
		QueryResponseTupleString resp = new QueryResponseTupleString(
				"OK", 200, scriptResult, 1, 1, AccumuloInstance.schema,
				AccumuloInstance.types, new Timestamp(0));
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(resp).replace("\\", "");
	}

	public String executeQueryAccumuloPure(String table)
			throws TableNotFoundException, AccumuloException,
			AccumuloSecurityException, IOException {
		// specify which visibilities we are allowed to see
		// Authorizations auths = new Authorizations("public");
		Authorizations auths = new Authorizations();
		AccumuloInstance accInst = AccumuloInstance.getInstance();
		Connector conn = accInst.getConnector();
		conn.securityOperations().changeUserAuthorizations(
				accInst.getUsername(), auths);
		Scanner scan = conn.createScanner(table, auths);
		scan.setRange(new Range("", null));
		scan.fetchColumnFamily(new Text(""));
		List<List<String>> allRows = new ArrayList<List<String>>();
		for (Entry<Key, Value> entry : scan) {
			// System.out.println(entry.getKey());
			// System.out.println(entry.getValue());
			List<String> oneRow = new ArrayList<String>();
			Text rowIdResult = entry.getKey().getRow();
			Text colFamResult = entry.getKey().getColumnFamily();
			Text colKeyResult = entry.getKey().getColumnQualifier();
			Text visibility = entry.getKey().getColumnVisibility();
			Value valueResult = entry.getValue();
			oneRow.add(rowIdResult.toString());
			oneRow.add(colFamResult.toString());
			oneRow.add(colKeyResult.toString());
			oneRow.add(visibility.toString());
			oneRow.add(valueResult.toString());
			allRows.add(oneRow);
		}
		ObjectMapper mapper = new ObjectMapper();
		String allRowsString = mapper.writeValueAsString(allRows);
		QueryResponseTupleString resp = new QueryResponseTupleString(
				"OK", 200, allRowsString, 1, 1, AccumuloInstance.fullSchema,
				AccumuloInstance.fullTypes, new Timestamp(0));
		return mapper.writeValueAsString(resp);
	}

	public static void main(String[] args) {
		QueryClient qClient = new QueryClient();
		// qClient.executeQueryPostgres("Select * from books");
		// Response response =
		// qClient.query("{\"query\":\"RELATION(select * from mimic2v26.d_patients limit 5)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
		try {
			Response response = qClient
					.query("{\"query\":\"RELATION(SELECT * FROM pg_catalog.pg_tables)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
			System.out.println("Postgresql response: "+response.getEntity());
			// qClient.query("{\"query\":\"RELATION(SELECT * FROM test2)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
//			System.out.println(response.getEntity());
			// String accumuloData = qClient
			// .executeQueryAccumuloPure("note_events_TedgeDeg");
			// System.out.println(accumuloData);
			String accumuloScript = qClient.executeAccumuloShellScript(
					"database", "table", "query");
			System.out.println(accumuloScript);
			String resultSciDB;
			try {
				resultSciDB = qClient.executeQueryScidb("adam");
				System.out.println(resultSciDB);
			} catch (SciDBException e) {
				e.printStackTrace();
			}
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ShellScriptException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
