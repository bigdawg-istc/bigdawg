/**
 * 
 */
package istc.bigdawg.myria;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.MyriaException;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleString;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Adam Dziedzic
 * 
 */
public class MyriaHandler implements DBHandler {

	/**
	 * log
	 */
	Logger log = Logger.getLogger(MyriaHandler.class.getName());

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
