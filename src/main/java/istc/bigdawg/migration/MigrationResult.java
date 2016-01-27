/**
 * 
 */
package istc.bigdawg.migration;

/**
 * @author Adam Dziedzic
 * 
 *
 */
public class MigrationResult {

	private Long countExtractedElements;
	private Long countLoadedElements;
	private String message;
	private boolean isError;
	
	public static MigrationResult getFailedInstance(String message) {
		return new MigrationResult(message,true);
	}

	public MigrationResult(Long countExtractedRows, Long countLoadedRows) {
		this.countExtractedElements = countExtractedRows;
		this.countLoadedElements = countLoadedRows;
		this.isError = false;
	}

	public MigrationResult(Long countExtractedRows, Long countLoadedRows, String message, boolean isError) {
		this.countExtractedElements = countExtractedRows;
		this.countLoadedElements = countLoadedRows;
		this.message = message;
		this.isError = isError;
	}
	
	public MigrationResult(String message, boolean isError) {
		this.countExtractedElements=0L;
		this.countLoadedElements=0L;
		this.message=message;
		this.isError=isError;
	}

	public String getMessage() {
		return message;
	}

	public boolean isError() {
		return isError;
	}

	public Long getCountExtractedElements() {
		return countExtractedElements;
	}

	public Long getCountLoadedElements() {
		return countLoadedElements;
	}

}
