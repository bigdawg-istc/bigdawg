package istc.bigdawg.stream;

import istc.bigdawg.Main;
import istc.bigdawg.exceptions.AlertException;
import istc.bigdawg.stream.StreamDAO.ClientAlert;
import istc.bigdawg.stream.StreamDAO.DBAlert;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;

@Path("/")
public class Stream {
	public static final String STATUS = "status";
	public static final String ALERT = "alert";

	public static final String REGISTER = "registeralert";
	/**
	 * Register a stream alert
	 * 
	 * @param istream
	 * @return
	 */
	@Path(REGISTER)
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response register(String istream) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			RegisterStreamRequest st = mapper.readValue(istream,
					RegisterStreamRequest.class);
			System.out.println(mapper.writeValueAsString(st));
			StreamDAO dao = Main.getStreamDAO();
			DBAlert dbalert = dao.createOrGetDBAlert(st.getQuery(), st.isOneTime());
			dbalert.receiveURL = Main.BASE_URI+ALERT+"/"+dbalert.dbAlertID;
			System.out.println("DB Alert created: " + dbalert.receiveURL);
			//TODO should we only do this once? de-dup?
			AlertManager.RegisterDBEvent(dbalert);
			ClientAlert clientAlert = dao.createClientAlert(dbalert.dbAlertID, st.isPushNotify(), st.isOneTime(), st.getNotifyURL());
			String statusURL = "pushing to :"+st.getNotifyURL();
			if (!clientAlert.push){
				statusURL = Main.BASE_URI+STATUS+"/"+clientAlert.clientAlertID;
			}
			RegisterStreamResponse resp = new RegisterStreamResponse("OK", 200,
					statusURL);
			return Response.status(200).entity(mapper.writeValueAsString(resp))
					.build();
		} catch (AlertException e) {
			e.printStackTrace();
			return Response.status(500).entity(e.getMessage()).build();
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
	 * 
	 * @param stream
	 * @return
	 */
	@Path(STATUS)
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String status() {
		return "Please provide the status stream ID status/XXX!";
	}

	/**
	 * Check if a registered alert has fired.
	 * 
	 * @param stream
	 * @return
	 */
	@Path(STATUS+"/{stream}")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String status(@PathParam("stream") String stream) {
		System.out.println("looking up stream status for " + stream);
		StreamDAO dao = Main.getStreamDAO();
		try{
			boolean hit = dao.checkForNewPull(Integer.parseInt(stream));
			return ""+hit;
		} catch (Exception ex){
			return ex.getMessage();
		}
	}
	
	/**
	 * Check if a registered alert has fired.
	 * 
	 * @param stream
	 * @return
	 */
	@Path(ALERT+"/{stream}")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String alert(@PathParam("stream") String stream) {
		System.out.println("registering stream event:" + stream);
		StreamDAO dao = Main.getStreamDAO();
		try{
			int streamInt = Integer.parseInt(stream);
			List<Integer> clientAlertIds = dao.addAlertEvent(streamInt, "");
			List<String> urls = dao.updatePullsAndGetPushURLS(clientAlertIds);
			AlertManager.PushEvents(urls);
			if (urls.isEmpty()){
				if (clientAlertIds.isEmpty()){
					return "Alert received but no one listening";
				}
				else {
					return "Alert received, no pushes only pulls";
				}
			}
			return "Pushing:\n" + StringUtils.join(urls, ",");
		} catch (Exception ex){
			return ex.getMessage();
		}
	}
}
