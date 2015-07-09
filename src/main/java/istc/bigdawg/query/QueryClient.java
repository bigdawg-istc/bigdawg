/**

 * 
 */
package istc.bigdawg.query;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

/**
 * @author Adam Dziedzic
 * test: curl -v -H "Content-Type: application/json" -X POST -d '{"query":"check heart rate","authorization":{},"tuplesPerPage":1,"pageNumber":1,"timestamp":"2012-04-23T18:25:43.511Z"}' http://localhost:8080/bigdawg/query
 */
@Path("/")
public class QueryClient {
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
				RegisterQueryRequest st = mapper.readValue(istream,RegisterQueryRequest.class);
				System.out.println(mapper.writeValueAsString(st));
				List<String> one = Arrays.asList("1","jack");
				List<String> two = Arrays .asList("2","mark");
				List<List<String>>  list = new ArrayList<List<String>>();
				list.add(one);
				list.add(two);
				List<String> schemaList = Arrays.asList("id","name");
				RegisterQueryResponse resp = new RegisterQueryResponse("OK", 200, list, 1, 1 ,schemaList, new Timestamp(0));
				return Response.status(200).entity(mapper.writeValueAsString(resp))
						.build();

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
}