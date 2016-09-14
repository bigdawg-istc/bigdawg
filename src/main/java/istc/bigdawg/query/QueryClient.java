/**

 *
 */
package istc.bigdawg.query;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import jline.internal.Log;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

import istc.bigdawg.accumulo.AccumuloHandler;
import istc.bigdawg.exceptions.AccumuloShellScriptException;
import istc.bigdawg.planner.Planner;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.sstore.SStoreSQLConnectionInfo;
import istc.bigdawg.utils.LogUtils;
import istc.bigdawg.utils.StackTrace;
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

	private static Logger log = Logger
			.getLogger(QueryClient.class.getName());

	private static List<DBHandler> registeredDbHandlers;

	static {
		registeredDbHandlers = new ArrayList<DBHandler>();
		int postgreSQL1Engine=0;
		try {
			registeredDbHandlers.add(new PostgreSQLHandler(postgreSQL1Engine));
		} catch (Exception e) {
			e.printStackTrace();
			String msg = "Could not register PostgreSQL handler!";
			System.err.println(msg);
			log.error(msg);
			System.exit(1);
		}
		registeredDbHandlers.add(new AccumuloHandler());
		try {
			registeredDbHandlers.add(new SciDBHandler());
		} catch (SQLException e) {
			e.printStackTrace();
			log.error(e.getMessage() + " " + StackTrace.getFullStackTrace(e));
		}
	}

	/**
	 * Answer a query from a client.
	 * 
	 * @param istream
	 * @return
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws TableNotFoundException
	 * @throws AccumuloShellScriptException
	 * @throws InterruptedException
	 */
	@Path("query")
	@POST
//	@Consumes(MediaType.APPLICATION_JSON)
//	@Produces(MediaType.APPLICATION_JSON)
	public Response query(String istream) {
		log.info("istream: " + istream.replaceAll("[\"']", "*"));
		try {
			return Planner.processQuery(istream, false);
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(412).entity(e.getMessage()).build();
		}
	}
	
//	/**
//	 * Run a procedure from a client
//	 * @param istream
//	 * @return
//	 */
//	public Response procedure(String istream) {
//		Log.info("procedure: " + istream.replaceAll("[\"']", "*"));
//		
//		// get a frontend handler
//		
//		// get input from the frontend handler
//		
//		// run the procedure
//		
//		// return the result to the frontend
//		
//		// keep listening from the frontend handler
//		
//	}
//
//	protected void doGet(
//		    HttpServletRequest request,
//		    HttpServletResponse response)
//		      throws ServletException, IOException {
//
//		    String procedureName = request.getParameter("procedure");
//		    String callProcedure, param1, param2, param3, param4;
//	    	PreparedStatement statement = null;
//	    	Long countExtractedRows = 0L;
//			SStoreSQLConnectionInfo connectionSStore = new SStoreSQLConnectionInfo("localhost",
//					"21212", "", "user", "password");
//			SStoreSQLConnection connectionSStore
//	    	try {
//		    if (procedureName.startsWith("GetTracks")) {
//		    	param1 = request.getParameter("lat_low");
//		    	param2 = request.getParameter("lat_high");
//		    	param3 = request.getParameter("lon_low");
//		    	param4 = request.getParameter("lon_high");
//		    	callProcedure = "{call @" + procedureName + "(?, ?, ?, ?)}";
//	    		statement = connectionSStore.prepareCall(callProcedure);
//	    		statement.setDouble(1, Double.parseDouble(param1));
//	    		statement.setDouble(2, Double.parseDouble(param2));
//	    		statement.setDouble(3, Double.parseDouble(param3));
//	    		statement.setDouble(4, Double.parseDouble(param4));
//	    		ResultSet rs = statement.executeQuery();
//	    		rs.next();
//	    		countExtractedRows = rs.getLong(1);
//	    		rs.close();
//	    		statement.close();
//		    } else {
//		    	callProcedure = "{call @" + procedureName + "()";
//	    		statement = connectionSStore.prepareCall(callProcedure);
//	    		ResultSet rs = statement.executeQuery();
//	    		rs.next();
//	    		countExtractedRows = rs.getLong(1);
//	    		rs.close();
//	    		statement.close();
//		    }
//	    	} catch (SQLException ex) {
//	    		ex.printStackTrace();
//	    		// remove ' from the statement - otherwise it won't be inserted into
//	    		// log table in Postgres
//	    		log.error(ex.getMessage() + "; statement to be executed: " + LogUtils.replace(copyToString) + " "
//	    				+ ex.getStackTrace(), ex);
//	    		throw ex;
//	    	} finally {
//	    		if (statement != null) {
//	    			statement.close();
//	    		}
//	    	}
//	    	return countExtractedRows;
//
//	}	
//	
	public static void main(String[] args) {
		/*
		QueryClient qClient = new QueryClient();
		 qClient.executeQueryPostgres("Select * from books");
		Response response1 =
		qClient.query("{\"query\":\"RELATION(select * from mimic2v26.d_patients limit 5)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
		System.out.println(response1.getEntity());

		int max_limit = 10000;
		for (int limit = 1; limit <= max_limit; limit = limit * 2) {
			System.out.print("limit: " + limit + ",");
			long lStartTime = System.nanoTime();
			 Response response = qClient
			 .query("{\"query\":\"RELATION(SELECT * FROM pg_catalog.pg_tables)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
			 Response response = qClient
			 .query("{\"query\":\"RELATION(select * from mimic2v26.d_patients limit "
			 + limit
			 +
			 ")\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
			Response response = qClient
					.query("{\"query\":\"RELATION(select * from mimic2v26.chartevents limit "
							+ limit
							+ ")\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");

			 System.out.println("Postgresql response: " +
			 response.getEntity());
			System.out.println("Elapsed total time milliseconds: "
					+ (System.nanoTime() - lStartTime) / 1000000);
		}
		 qClient.query("{\"query\":\"RELATION(SELECT * FROM test2)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
		 System.out.println(response.getEntity());
		 String accumuloData = qClient
		 .executeQueryAccumuloPure("note_events_TedgeDeg");
		 System.out.println(accumuloData);
		 */
	}
}
