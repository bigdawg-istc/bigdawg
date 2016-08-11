/**
 * 
 */
package istc.bigdawg.myria;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.AccumuloShellScriptException;
import istc.bigdawg.exceptions.BigDawgException;
import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.executor.QueryResult;
import istc.bigdawg.islands.Myria.MyriaQueryResult;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleString;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.RunShell;

/**
 * @author Adam Dziedzic
 * 
 */
public class MyriaHandler implements DBHandler {

	/**
	 * log
	 */
	Logger log = Logger.getLogger(MyriaHandler.class.getName());
	private static final String myriaQueryString = "curl@@@-X@@@POST@@@-F@@@query=%s@@@-F@@@language=myrial@@@-F@@@push_sql=False@@@-F@@@multiway_join=False@@@http://%s:%s/execute";
	public static final String myriaDataRetrievalString = "curl@@@-i@@@-XGET@@@%s:%s/dataset/public/adhoc/%s/data?format=json";
	public static final String myriaInquieryString = "curl@@@http://%s:%s/execute?queryId=%s"; 
	
	
	public static QueryResult executeMyriaQuery(List<String> inputs) throws IOException, InterruptedException, AccumuloShellScriptException, JSONException, BigDawgException {
		
		String myriaQueryHost = BigDawgConfigProperties.INSTANCE.getMyriaHost();
		String myriaQueryPort = BigDawgConfigProperties.INSTANCE.getMyriaPort();
		String myriaDownloadPort = BigDawgConfigProperties.INSTANCE.getMyriaDownloadPort();
		
		boolean isQuery;
		boolean isDownload;
		
		String queryString;
		if (inputs.size() == 1) {
			queryString = String.format(myriaDataRetrievalString, myriaQueryHost, myriaDownloadPort, inputs.get(0));
			isDownload = true;
			isQuery = false;
		} else {
			queryString = String.format(myriaQueryString, inputs.get(2), myriaQueryHost, myriaQueryPort);
			isQuery = true;
			isDownload = inputs.get(0).equalsIgnoreCase("materialize;");
		}
		
		System.out.printf("Query string: %s; isQuery: %s; isDownload: %s;", queryString, isQuery, isDownload);
		
		InputStream scriptResultInStream = RunShell.runMyriaCommand(queryString, inputs.get(1), isQuery, isDownload);
		String scriptResult = IOUtils.toString(scriptResultInStream, Constants.ENCODING);
		
		
		return new MyriaQueryResult(scriptResult);
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		String resultMyria = getMyriaData(queryString);
		return Response.status(200).entity(resultMyria).build();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#getShim()
	 */
	@Override
	public Shim getShim() {
		return BDConstants.Shim.MYRIA;
	}

	public String getMyriaData(String query) {
		String myriaResult;
		try {
			myriaResult = MyriaClient.execute(query);
		} catch (MyriaException e) {
			return e.getMessage();
		}
		QueryResponseTupleString resp = new QueryResponseTupleString("OK", 200,
				myriaResult, 1, 1, new ArrayList<String>(),
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#getObjectMetaData(java.lang.String)
	 */
	@Override
	public ObjectMetaData getObjectMetaData(String name) throws Exception {
		// TODO
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#existsObject(java.lang.String)
	 */
	@Override
	public boolean existsObject(String name) throws Exception {
		// TODO
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see istc.bigdawg.query.DBHandler#close()
	 */
	@Override
	public void close() throws Exception {
		log.debug("No aciton for closing Myria.");
	}

	/* (non-Javadoc)
	 * @see istc.bigdawg.query.DBHandler#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		// TODO
		throw new UnsupportedOperationException();
	}

}
