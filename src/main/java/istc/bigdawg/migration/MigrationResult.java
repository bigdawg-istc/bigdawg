/**
 * 
 */
package istc.bigdawg.migration;

/**
 * Results from a migration execution.
 * 
 * @author Adam Dziedzic
 */
public class MigrationResult {

	private Long countExtractedElements;
	private Long countLoadedElements;
	private String message;
	private boolean isError;

	public static MigrationResult getEmptyInstance(String message) {
		return new MigrationResult(message, true);
	}

	public static MigrationResult getFailedInstance(String message) {
		return new MigrationResult(message, true);
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
		this.countExtractedElements = 0L;
		this.countLoadedElements = 0L;
		this.message = message;
		this.isError = isError;
	}

	/** General message about the migration process. */
	public String getMessage() {
		return message;
	}

	/** Was there any error during the migration process? */
	public boolean isError() {
		return isError;
	}

	/**
	 * @return number of extracted elements/tuples/records/cells from a source
	 *         database (some database do not provide such information)
	 */
	public Long getCountExtractedElements() {
		return countExtractedElements;
	}

	/**
	 * @return number of loaded elements/tuples/records/cells to a destination
	 *         database (some database do not provide such information)
	 */
	public Long getCountLoadedElements() {
		return countLoadedElements;
	}

}
