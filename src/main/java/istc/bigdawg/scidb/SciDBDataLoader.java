/**
 * 
 */
package istc.bigdawg.scidb;

import istc.bigdawg.exceptions.SciDBLoaderException;
import istc.bigdawg.utils.Constants;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Adam Dziedzic
 * 
 */
@Path("/load/scidb")
public class SciDBDataLoader {

	static Logger log = org.apache.log4j.Logger.getLogger(SciDBDataLoader.class.getName());

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response load(String istream) {
		log.info("istream for SciDB loading: " + istream);
		ObjectMapper mapper = new ObjectMapper();
		SciDBLoadRequest requestData;
		try {
			requestData = mapper.readValue(istream, SciDBLoadRequest.class);
			System.out.println(mapper.writeValueAsString(requestData));
		} catch (IOException e1) {
			String message = "The request could not be processed. Check your input data." + " " + e1.getMessage()
					+ "\n";
			e1.printStackTrace();
			log.error("istream: " + istream + " message: " + message);
			return Response.status(HttpStatus.SC_BAD_REQUEST).entity(message).build();
		}
		try {
			InputStream inStream = executeLoading(requestData.getScript(), requestData.getDataLocation(),
					requestData.getFlatArrayName(), requestData.getArrayName());
			String result = IOUtils.toString(inStream, Constants.ENCODING);
			String responseMessage = "Script executed. Returned message: " + result;
			return Response.status(HttpStatus.SC_OK).entity(responseMessage).build();
		} catch (IOException | InterruptedException | SciDBLoaderException e) {
			e.printStackTrace();
			String message = "The data loading to SciDB failed." + " " + e.getMessage() + "\n";
			log.error(message);
			return Response.status(HttpStatus.SC_OK).entity(message).build();
		}
	}

	private InputStream executeLoading(String script, String dataLocation, String flatArrayName, String arrayName)
			throws IOException, InterruptedException, SciDBLoaderException {
		Process prop = new ProcessBuilder(script, dataLocation, flatArrayName, arrayName).start();
		prop.waitFor();
		int exitVal = prop.exitValue();
		if (exitVal != 0) {
			throw new SciDBLoaderException("Problem with the data loading script: " + script + " data path: "
					+ dataLocation + " flatArrayName: " + flatArrayName + " arrayName: " + arrayName
					+ ". Process returned value: " + exitVal);
		}
		return prop.getInputStream();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Present Project Directory : " + System.getProperty("user.dir"));
		String script = System.getProperty("user.dir") + "/scripts/test_script/scidbLoad.sh";
		String dataLocation = System.getProperty("user.dir") + "/scripts/test_script/";
		String flatArrayName = "myFlatArrayName";
		String arrayName = "myArrayName";
		try {
			InputStream inStream = new SciDBDataLoader().executeLoading(script, dataLocation, flatArrayName, arrayName);
			String result = IOUtils.toString(inStream, Constants.ENCODING);
			System.out.println("Result of the script: " + result);
		} catch (IOException | InterruptedException | SciDBLoaderException e) {
			System.out.println("Loading to SciDB failed");
			e.printStackTrace();
		}

	}

}
