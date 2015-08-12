/**

 *
 */
package istc.bigdawg.query;

import istc.bigdawg.accumulo.AccumuloHandler;
import istc.bigdawg.exceptions.NotSupportIslandException;
import istc.bigdawg.exceptions.ShellScriptException;
import istc.bigdawg.myria.MyriaHandler;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.query.parser.Parser;
import istc.bigdawg.query.parser.simpleParser;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.ObjectMapperResource;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;

import com.fasterxml.jackson.databind.ObjectMapper;

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

	private static List<DBHandler> registeredDbHandlers;

	static {
		registeredDbHandlers = new ArrayList<DBHandler>();
		registeredDbHandlers.add(new PostgreSQLHandler());
		registeredDbHandlers.add(new AccumuloHandler());
		registeredDbHandlers.add(new MyriaHandler());
		registeredDbHandlers.add(new SciDBHandler());
	}

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
	public Response query(String istream) {
		// System.out.println(istream);
		log.info("istream: " + istream);
		QueryRequest st;
		try {
			st = ObjectMapperResource.INSTANCE.getObjectMapper().readValue(
					istream, QueryRequest.class);
			// System.out.println(mapper.writeValueAsString(st));
			Parser parser = new simpleParser();
			ASTNode parsed;
			parsed = parser.parseQueryIntoTree(st.getQuery());

			// System.out.println(parsed.getShim());
			String queryString = parsed.getTarget();

			for (DBHandler handler : registeredDbHandlers) {
				if (handler.getShim() == parsed.getShim()) {
					return handler.executeQuery(queryString);
				}
			}
			// no handler found
			QueryResponseTupleList resp = new QueryResponseTupleList(
					"ERROR: Unrecognized island"
							+ parsed.getIsland().toString(), 412, null, 1, 1,
					null, null, new Timestamp(0));
			String responseResult = ObjectMapperResource.INSTANCE
					.getObjectMapper().writeValueAsString(resp);
			return Response.status(412).entity(responseResult).build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(412).entity(e.getMessage()).build();
		} catch (NotSupportIslandException e) {
			e.printStackTrace();
			return Response
					.status(412)
					.entity("The island in the query is not supported. "
							+ e.getMessage()).build();
		}
	}

	public static void main(String[] args) {
		QueryClient qClient = new QueryClient();
		// qClient.executeQueryPostgres("Select * from books");
		// Response response =
		// qClient.query("{\"query\":\"RELATION(select * from mimic2v26.d_patients limit 5)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
		int[] limit_tab = { 100000 };
		int max_limit = 10000;
		for (int limit = 1; limit <= max_limit; limit = limit * 2) {
			System.out.print("limit: " + limit + ",");
			long lStartTime = System.nanoTime();
			// Response response = qClient
			// .query("{\"query\":\"RELATION(SELECT * FROM pg_catalog.pg_tables)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
			// Response response = qClient
			// .query("{\"query\":\"RELATION(select * from mimic2v26.d_patients limit "
			// + limit
			// +
			// ")\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
			Response response = qClient
					.query("{\"query\":\"RELATION(select * from mimic2v26.chartevents limit "
							+ limit
							+ ")\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");

			// System.out.println("Postgresql response: " +
			// response.getEntity());
			System.out.println("Elapsed total time milliseconds: "
					+ (System.nanoTime() - lStartTime) / 1000000);
		}
		// qClient.query("{\"query\":\"RELATION(SELECT * FROM test2)\",\"authorization\":{},\"tuplesPerPage\":1,\"pageNumber\":1,\"timestamp\":\"2012-04-23T18:25:43.511Z\"}");
		// System.out.println(response.getEntity());
		// String accumuloData = qClient
		// .executeQueryAccumuloPure("note_events_TedgeDeg");
		// System.out.println(accumuloData);
	}
}
