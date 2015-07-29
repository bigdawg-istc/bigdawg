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
public class RegisterQueryResponseGeneral extends RegisterQueryResponse {

	private String tuples;

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
	public RegisterQueryResponseGeneral(String message, int responseCode,
			String tuples, int pageNumber, int totalPages, List<String> schema,
			List<String> types, Timestamp cacheTimestamp) {
		super(message, responseCode, pageNumber, totalPages, schema, types,
				cacheTimestamp);
		this.tuples = tuples;
	}

	/**
	 * @return the tuples
	 */
	public String getTuples() {
		return tuples;
	}

	/**
	 * @param tuples
	 *            the tuples to set
	 */
	public void setTuples(String tuples) {
		this.tuples = tuples;
	}

}
