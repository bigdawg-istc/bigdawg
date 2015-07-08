package istc.bigdawg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

@Path("/")
public class Stream {

    	
	/**
	 * Register a stream alert
	 * @param istream
	 * @return
	 */
	@Path("registeralert")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response register(String istream) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			RegisterStreamRequest st = mapper.readValue(istream, RegisterStreamRequest.class);
			System.out.println(mapper.writeValueAsString(st));
			RegisterStreamResponse resp = new RegisterStreamResponse("OK", 200, "statusurl");
			return Response.status(200).entity(mapper.writeValueAsString(resp)).build();

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
	
	
	/**
	 * Status page with no property set.
	 * @param stream
	 * @return
	 */
	@Path("status")   
	@GET
    @Produces(MediaType.TEXT_PLAIN)
    public String status() {
        return "Please provide the status stream ID status/XXX!";
    }
	
	/**
	 * Check if a registered alert has fired.
	 * @param stream
	 * @return
	 */
	@Path("status/{stream}")   
	@GET
    @Produces(MediaType.TEXT_PLAIN)
    public String status(@PathParam("stream") String stream) {
		System.out.println("looking up stream status for " + stream);
        return "Got it!";
    }
}
