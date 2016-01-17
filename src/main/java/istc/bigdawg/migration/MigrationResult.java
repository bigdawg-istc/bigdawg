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

	private Long countExtractedRows;
	private Long countLoadedRows;
	private String message;
	private boolean isError;
	
	public static MigrationResult getFailedInstance(String message) {
		return new MigrationResult(message,true);
	}

	public MigrationResult(Long countExtractedRows, Long countLoadedRows) {
		this.countExtractedRows = countExtractedRows;
		this.countLoadedRows = countLoadedRows;
		this.isError = false;
	}

	public MigrationResult(Long countExtractedRows, Long countLoadedRows, String message, boolean isError) {
		this.countExtractedRows = countExtractedRows;
		this.countLoadedRows = countLoadedRows;
		this.message = message;
		this.isError = isError;
	}
	
	public MigrationResult(String message, boolean isError) {
		this.countExtractedRows=0L;
		this.countLoadedRows=0L;
		this.message=message;
		this.isError=isError;
	}

	public String getMessage() {
		return message;
	}

	public boolean isError() {
		return isError;
	}

	public Long getCountExtractedRows() {
		return countExtractedRows;
	}

	public Long getCountLoadedRows() {
		return countLoadedRows;
	}

}
