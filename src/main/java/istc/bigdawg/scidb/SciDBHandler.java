/**
 * 
 */
package istc.bigdawg.scidb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleList;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.ObjectMapperResource;
import istc.bigdawg.utils.RunShell;
import istc.bigdawg.utils.Tuple.Tuple2;

/**
 * @author adam
 * 
 */
public class SciDBHandler implements DBHandler {

	Logger log = org.apache.log4j.Logger
			.getLogger(SciDBHandler.class.getName());

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		System.out.println("run query for SciDB");
		System.out.println("SciDB queryString: " + queryString);
		String resultSciDB;
			try {
				resultSciDB = executeQueryScidb(queryString);
				return Response.status(200).entity(resultSciDB).build();		
			} catch (IOException | InterruptedException | SciDBException e) {
				e.printStackTrace();
				String messageSciDB = "Problem with SciDB: " + e.getMessage();
				log.error(messageSciDB);
				return Response.status(200).entity(messageSciDB).build();
			}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#getShim()
	 */
	@Override
	public Shim getShim() {
		return BDConstants.Shim.PSQLARRAY;
	}

	private String executeQueryScidb(String queryString) throws IOException,
			InterruptedException, SciDBException {
		String sciDBHostname = BigDawgConfigProperties.INSTANCE
				.getScidbHostname();
//		String sciDBUser = BigDawgConfigProperties.INSTANCE.getScidbUser();
//		String sciDBPassword = BigDawgConfigProperties.INSTANCE
//				.getScidbPassword();
//		System.out.println("sciDBHostname: " + sciDBHostname);
//		System.out.println("sciDBUser: " + sciDBUser);
//		System.out.println("sciDBPassword: " + sciDBPassword);
		
		long lStartTime = System.nanoTime();
		String resultString = getDataFromSciDB(queryString,sciDBHostname);
		String messageGetData="SciDB query execution time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageGetData);
		log.info(messageGetData);
		//System.out.println("result_string: "+resultString);
		
		lStartTime=System.nanoTime();
		Tuple2<List<String>, List<List<String>>> parsedData = ParseSciDBResponse
				.parse(resultString);
		List<String> colNames = parsedData.getT1();
		List<List<String>> tuples = parsedData.getT2();
		QueryResponseTupleList resp = new QueryResponseTupleList("OK", 200,
				tuples, 1, 1, colNames, new ArrayList<String>(), new Timestamp(
						0));
		String messageParsing = "Parsing data time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ","; 
		System.out.print(messageParsing);
		log.info(messageParsing);
		
		lStartTime=System.nanoTime();
		String responseResult = ObjectMapperResource.INSTANCE.getObjectMapper().writeValueAsString(resp);
		String messageJSON="JSON formatting time milliseconds: "
				+ (System.nanoTime() - lStartTime) / 1000000 + ",";
		System.out.print(messageJSON);
		log.info(messageJSON);
		
		return responseResult;
	}
	
	private String getDataFromSciDB(final String queryString, final String sciDBHostname) throws IOException, InterruptedException, SciDBException {
		InputStream resultInStream = RunShell.runSciDBquery(sciDBHostname,
				queryString);
		String resultString = IOUtils.toString(resultInStream,
				Constants.ENCODING);
		return resultString;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			//String resultSciDB = new SciDBHandler().executeQueryScidb("list(^^arrays^^)");
			String resultSciDB = new SciDBHandler().executeQueryScidb("scan(waveform_test_1GB)");
			//System.out.println(resultSciDB);
		} catch (IOException | InterruptedException | SciDBException e) {
			e.printStackTrace();
		}


	}

}
