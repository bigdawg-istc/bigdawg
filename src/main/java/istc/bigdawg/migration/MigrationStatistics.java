/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.query.ConnectionInfo;

/**
 * Statistics about a single migration execution.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 19, 2016 1:00:57 PM
 */
public class MigrationStatistics {

	private ConnectionInfo from;
	private ConnectionInfo to;
	private String objectFrom;
	private String objectTo;
	private long startTimeMigration;
	private long endTimeMigration;
	private long countExtractedElements;
	private long countLoadedElements;
	private String message;

	/**
	 * @param from
	 * @param to
	 * @param objectFrom
	 * @param objectTo
	 * @param startTimeMigration
	 * @param endTimeMigration
	 * @param countExtractedElements
	 * @param countLoadedElements
	 * @param message
	 */
	public MigrationStatistics(ConnectionInfo from, ConnectionInfo to, String objectFrom, String objectTo,
			long startTimeMigration, long endTimeMigration, long countExtractedElements, long countLoadedElements,
			String message) {
		this.from = from;
		this.to = to;
		this.objectFrom = objectFrom;
		this.objectTo = objectTo;
		this.startTimeMigration = startTimeMigration;
		this.endTimeMigration = endTimeMigration;
		this.countExtractedElements = countExtractedElements;
		this.countLoadedElements = countLoadedElements;
		this.message = message;
	}

	/**
	 * @return the from Information from which database the data is migrated.
	 *         (not null)
	 */
	public ConnectionInfo getFrom() {
		return from;
	}

	/**
	 * @return the to Information to which database the data is migrated. (not
	 *         null)
	 */
	public ConnectionInfo getTo() {
		return to;
	}

	/**
	 * @return the objectFrom Array/Table or another object from which we
	 *         extract data. (not null)
	 */
	public String getObjectFrom() {
		return objectFrom;
	}

	/**
	 * @return the objectTo Array/Table or another object to which we load the
	 *         data. (not null)
	 */
	public String getObjectTo() {
		return objectTo;
	}

	/**
	 * @return the startTimeMigration Start time of the migration
	 *         (milliseconds). (not null) the difference, measured in
	 *         milliseconds, between the current time and midnight, January 1,
	 *         1970 UTC.
	 */
	public long getStartTimeMigration() {
		return startTimeMigration;
	}

	/**
	 * @return the endTimeMigration End time of the migration (milliseconds).
	 *         (not null) the difference, measured in milliseconds, between the
	 *         current time and midnight, January 1, 1970 UTC.
	 */
	public long getEndTimeMigration() {
		return endTimeMigration;
	}

	/**
	 * @return the countExtractedElements Number of extracted elements
	 *         (rows/objects) (may not be available)
	 */
	public long getCountExtractedElements() {
		return countExtractedElements;
	}

	/**
	 * @return the countLoadedElements Number of loaded elements (rows/objects)
	 *         (may not be available)
	 */
	public long getCountLoadedElements() {
		return countLoadedElements;
	}

	/**
	 * @return the message Some additional information about the migration, e.g.
	 *         names of databases, migration type, extraction / loading time, if
	 *         available
	 */
	public String getMessage() {
		return message;
	}

}
