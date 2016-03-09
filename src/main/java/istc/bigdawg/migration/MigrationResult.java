/**
 * 
 */
package istc.bigdawg.migration;

import java.io.Serializable;

/**
 * Results from a migration execution.
 * 
 * @author Adam Dziedzic
 */
public class MigrationResult implements Serializable {

	/**
	 * the objects of the class are serializable
	 */
	private static final long serialVersionUID = 1L;
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

	public MigrationResult(Long countExtractedRows, Long countLoadedRows,
			String message, boolean isError) {
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MigrationResult [countExtractedElements="
				+ countExtractedElements + ", countLoadedElements="
				+ countLoadedElements + ", message=" + message + ", isError="
				+ isError + "]";
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((countExtractedElements == null) ? 0
				: countExtractedElements.hashCode());
		result = prime * result + ((countLoadedElements == null) ? 0
				: countLoadedElements.hashCode());
		result = prime * result + (isError ? 1231 : 1237);
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MigrationResult other = (MigrationResult) obj;
		if (countExtractedElements == null) {
			if (other.countExtractedElements != null)
				return false;
		} else if (!countExtractedElements.equals(other.countExtractedElements))
			return false;
		if (countLoadedElements == null) {
			if (other.countLoadedElements != null)
				return false;
		} else if (!countLoadedElements.equals(other.countLoadedElements))
			return false;
		if (isError != other.isError)
			return false;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		return true;
	}
	
}
