/**
 * 
 */
package istc.bigdawg.query;

import java.sql.Timestamp;
import java.util.List;

/**
 * @author adam
 *
 */
public class RegisterQueryResponsePostgreSQL extends RegisterQueryResponse {
	
	private List<List<String>> tuples;

	/**
	 * @param message
	 * @param responseCode
	 * @param tuples
	 * @param pageNumber
	 * @param totalPages
	 * @param schema
	 * @param types
	 * @param cacheTimestamp
	 */
	public RegisterQueryResponsePostgreSQL(String message, int responseCode,
			List<List<String>> tuples, int pageNumber, int totalPages, List<String> schema,
			List<String> types, Timestamp cacheTimestamp) {
		super(message, responseCode,  pageNumber, totalPages, schema,
				types, cacheTimestamp);
		this.tuples=tuples;
	}
	
	
	/**
	 * @return the tuples
	 */
	public List<List<String>> getTuples() {
		return tuples;
	}



	/**
	 * @param tuples the tuples to set
	 */
	public void setTuples(List<List<String>> tuples) {
		this.tuples = tuples;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
