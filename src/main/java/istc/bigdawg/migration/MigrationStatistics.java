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

	private ConnectionInfo connectionFrom;
	private ConnectionInfo connectionTo;
	private String objectFrom;
	private String objectTo;
	private long startTimeMigration;
	private long endTimeMigration;
	private Long countExtractedElements;
	private Long countLoadedElements;
	private String message;

	/**
	 * @param connectionFrom
	 * @param connectionTo
	 * @param objectFrom
	 * @param objectTo
	 * @param startTimeMigration
	 * @param endTimeMigration
	 * @param countExtractedElements
	 * @param countLoadedElements
	 * @param message
	 */
	public MigrationStatistics(ConnectionInfo connectionFrom, ConnectionInfo connectionTo, String objectFrom, String objectTo,
			long startTimeMigration, long endTimeMigration, long countExtractedElements, long countLoadedElements,
			String message) {
		this.connectionFrom = connectionFrom;
		this.connectionTo = connectionTo;
		this.objectFrom = objectFrom;
		this.objectTo = objectTo;
		this.startTimeMigration = startTimeMigration;
		this.endTimeMigration = endTimeMigration;
		this.countExtractedElements = countExtractedElements;
		this.countLoadedElements = countLoadedElements;
		this.message = message;
	}

	/**
	 * @return the connectionFrom Information from which database the data is migrated.
	 *         (not null)
	 */
	public ConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/**
	 * @return the connectionTo Information to which database the data is migrated. (not
	 *         null)
	 */
	public ConnectionInfo getConnectionTo() {
		return connectionTo;
	}

	/**
	 * @return the objectFrom Array/Table or another object from which we
	 *         extract data. (not null) The object resided in the database
	 *         identified by connectionFrom {@link #getConnectionFrom()}
	 */
	public String getObjectFrom() {
		return objectFrom;
	}

	/**
	 * @return the objectTo Array/Table or another object to which we load the
	 *         data. (not null) The object resided in the database
	 *         identified by connectionTo {@link #getConnectionTo()}
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
	 *         (rows/objects) (may not be available). These elements are extracted from the {@link #getObjectTo()}
	 */
	public Long getCountExtractedElements() {
		return countExtractedElements;
	}

	/**
	 * @return the countLoadedElements Number of loaded elements (rows/objects)
	 *         (may not be available) These elements are loaded to the {@link #getObjectTo()}
	 */
	public Long getCountLoadedElements() {
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
