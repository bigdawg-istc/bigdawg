package istc.bigdawg.query;

import java.sql.Timestamp;
import java.util.List;

public class RegisterQueryResponse {
	
	private String message;
	private int responseCode;
	private List<List<String>> tuples;
	private int pageNumber;
	private int totalPages;
	private List<String> schema;
	private Timestamp cacheTimestamp;

	/**
	 * @param message
	 * @param responseCode
	 * @param tuples
	 * @param pageNumber
	 * @param totalPages
	 * @param schema
	 * @param timeStamp
	 */
	public RegisterQueryResponse(String message, int responseCode,
			List<List<String>> tuples, int pageNumber, int totalPages,
			List<String> schema, Timestamp cacheTimestamp) {
		super();
		this.message = message;
		this.responseCode = responseCode;
		this.tuples = tuples;
		this.pageNumber = pageNumber;
		this.totalPages = totalPages;
		this.schema = schema;
		this.cacheTimestamp = cacheTimestamp;
	}
	
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
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
	 * @return the pageNumber
	 */
	public int getPageNumber() {
		return pageNumber;
	}
	/**
	 * @param pageNumber the pageNumber to set
	 */
	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}
	/**
	 * @return the totalPages
	 */
	public int getTotalPages() {
		return totalPages;
	}
	/**
	 * @param totalPages the totalPages to set
	 */
	public void setTotalPages(int totalPages) {
		this.totalPages = totalPages;
	}
	/**
	 * @return the schema
	 */
	public List<String> getSchema() {
		return schema;
	}
	/**
	 * @param schema the schema to set
	 */
	public void setSchema(List<String> schema) {
		this.schema = schema;
	}
	/**
	 * @return the cacheTimestamp
	 */
	public Timestamp getCacheTimestamp() {
		return cacheTimestamp;
	}
	/**
	 * @param cacheTimestamp the cacheTimestamp to set
	 */
	public void setCacheTimestamp(Timestamp cacheTimestamp) {
		this.cacheTimestamp = cacheTimestamp;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

}
