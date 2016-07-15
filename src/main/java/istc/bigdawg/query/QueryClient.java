/**

 *
 */
package istc.bigdawg.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

import istc.bigdawg.accumulo.AccumuloHandler;
import istc.bigdawg.exceptions.AccumuloShellScriptException;
import istc.bigdawg.myria.MyriaHandler;
import istc.bigdawg.planner.Planner;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.scidb.SciDBHandler;
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
		registeredDbHandlers.add(new MyriaHandler());
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
