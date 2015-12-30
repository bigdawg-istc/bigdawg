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

	public MigrationResult(Long countExtractedRows, Long countLoadedRows) {
		this.countExtractedRows = countExtractedRows;
		this.countLoadedRows = countLoadedRows;
	}

	public Long getCountExtractedRows() {
		return countExtractedRows;
	}

	public Long getCountLoadedRows() {
		return countLoadedRows;
	}

}
