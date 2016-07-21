/**
 * 
 */
package istc.bigdawg.migration;

import java.io.Serializable;

/**
 * Results and information about the loading process.
 * 
 * @author Adam Dziedzic
 */
public class LoadRemoteResult implements Serializable {

	/**
	 * Determines if a de-serialized file is compatible with this class.
	 */
	private static final long serialVersionUID = 1130797454923493891L;

	private Long countLoadedElements;
	private Long startTimeLoading;
	private Long endTimeLoading;
	private Long durationMsec;
	private Long bytesReceived;
	private String message;

	public LoadRemoteResult(Long countLoadedElements, Long startTimeLoading,
			Long endTimeLoading, Long durationMsec, Long bytesReceived,
			String message) {
		this.countLoadedElements = countLoadedElements;
		this.startTimeLoading = startTimeLoading;
		this.endTimeLoading = endTimeLoading;
		this.durationMsec = durationMsec;
		this.bytesReceived = bytesReceived;
		this.message = message;
	}

	/**
	 * @return the countLoadedElements
	 */
	public Long getCountLoadedElements() {
		return countLoadedElements;
	}

	/**
	 * @return the startTimeLoading
	 */
	public Long getStartTimeLoading() {
		return startTimeLoading;
	}

	/**
	 * @return the endTimeLoading
	 */
	public Long getEndTimeLoading() {
		return endTimeLoading;
	}

	/**
	 * @return the durationMsec
	 */
	public Long getDurationMsec() {
		return durationMsec;
	}

	/**
	 * @return the message
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @return the bytesReceived The number of bytes received via network.
	 */
	public Long getBytesReceived() {
		return bytesReceived;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "LoadRemoteResult [countLoadedElements=" + countLoadedElements
				+ ", startTimeLoading=" + startTimeLoading + ", endTimeLoading="
				+ endTimeLoading + ", durationMsec=" + durationMsec
				+ ", bytesReceived=" + bytesReceived + ", message=" + message
				+ "]";
	}

}
