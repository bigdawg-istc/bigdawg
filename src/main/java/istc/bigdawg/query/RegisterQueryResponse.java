package istc.bigdawg.query;

import java.sql.Timestamp;
import java.util.List;

public abstract class RegisterQueryResponse {
	
	private String message;
	private int responseCode;
	private int pageNumber;
	private int totalPages;
	private List<String> schema;
	private List<String> types;
	private Timestamp cacheTimestamp;

	/**
	 * @param message
	 * @param responseCode
	 * @param pageNumber
	 * @param totalPages
	 * @param schema
	 * @param types
	 * @param timeStamp
	 */
	public RegisterQueryResponse(String message, int responseCode,
			int pageNumber, int totalPages,
			List<String> schema, List<String> types, Timestamp cacheTimestamp) {
		super();
		this.message = message;
		this.responseCode = responseCode;
		this.pageNumber = pageNumber;
		this.totalPages = totalPages;
		this.schema = schema;
		this.types=types;
		this.cacheTimestamp = cacheTimestamp;
	}
	
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
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

	/**
	 * @return the types
	 */
	public List<String> getTypes() {
		return types;
	}

	/**
	 * @param types the types to set
	 */
	public void setTypes(List<String> types) {
		this.types = types;
	}

}
