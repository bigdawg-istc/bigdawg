/**
 * 
 */
package istc.bigdawg.migration;

import java.io.Serializable;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NetworkException;

/**
 * Results from a migration execution.
 * 
 * @author Adam Dziedzic
 */
public class MigrationResult implements Serializable {

	/**
	 * The objects of the class are serializable.
	 */
	private static final long serialVersionUID = 8348835503338320036L;

	private Long countExtractedElements;
	private Long countLoadedElements;
	private Long startTimeMigration;
	private Long endTimeMigration;
	private Long durationMsec;
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

	public MigrationResult(Long countExtractedElements,
			Long countLoadedElements, Long startTimeMigration,
			Long endTimeMigration, Long durationMsec) {
		this.countExtractedElements = countExtractedElements;
		this.countLoadedElements = countLoadedElements;
		this.startTimeMigration = startTimeMigration;
		this.endTimeMigration = endTimeMigration;
		this.durationMsec = durationMsec;
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

	/**
	 * @return the startTimeMigration
	 */
	public Long getStartTimeMigration() {
		return startTimeMigration;
	}

	/**
	 * @return the endTimeMigration
	 */
	public Long getEndTimeMigration() {
		return endTimeMigration;
	}

	/**
	 * @return the durationMsec
	 */
	public Long getDurationMsec() {
		return durationMsec;
	}

	/**
	 * Process the result returned by the remote request to migrate some data.
	 * 
	 * @param result
	 *            the result (Object) returned by the request.
	 * @return {@link MigrationResult} if request was successful
	 * @throws MigrationException
	 */
	public static MigrationResult processResult(Object result)
			throws MigrationException {
		/* log */
		Logger log = Logger.getLogger(MigrationResult.class);
		if (result == null) {
			String message = "No result returned from migration!";
			log.error(message);
			throw new MigrationException(message);
		}
		if (result instanceof MigrationResult) {
			log.debug("Final result: " + result.toString());
			return (MigrationResult) result;
		} else if (result instanceof MigrationException) {
			MigrationException ex = (MigrationException) result;
			log.error(ex.toString());
			throw ex;
		} else if (result instanceof NetworkException) {
			NetworkException ex = (NetworkException) result;
			String message = "Problem with network: " + ex.getMessage();
			log.error(message);
			throw new MigrationException(message);
		} else if (result instanceof Exception) {
			Exception e = (Exception) result;
			log.error(e.getMessage());
			throw new MigrationException(e.getMessage());
		}
		String message = "Migration was executed on a remote host but the returned result is unexepcted: "
				+ result.toString();
		log.error(message);
		throw new MigrationException(message);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MigrationResult [countExtractedElements="
				+ countExtractedElements + ", countLoadedElements="
				+ countLoadedElements + ", durationMsec=" + durationMsec
				+ ", startTime=" + startTimeMigration + ", endTime="
				+ endTimeMigration + ", message=" + message + ", isError="
				+ isError + "]";
	}

	/*
	 * (non-Javadoc)
	 * 
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
		result = prime * result
				+ ((durationMsec == null) ? 0 : durationMsec.hashCode());
		result = prime * result + ((endTimeMigration == null) ? 0
				: endTimeMigration.hashCode());
		result = prime * result + (isError ? 1231 : 1237);
		result = prime * result + ((message == null) ? 0 : message.hashCode());
		result = prime * result + ((startTimeMigration == null) ? 0
				: startTimeMigration.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
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
		if (durationMsec == null) {
			if (other.durationMsec != null)
				return false;
		} else if (!durationMsec.equals(other.durationMsec))
			return false;
		if (endTimeMigration == null) {
			if (other.endTimeMigration != null)
				return false;
		} else if (!endTimeMigration.equals(other.endTimeMigration))
			return false;
		if (isError != other.isError)
			return false;
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		if (startTimeMigration == null) {
			if (other.startTimeMigration != null)
				return false;
		} else if (!startTimeMigration.equals(other.startTimeMigration))
			return false;
		return true;
	}

}
