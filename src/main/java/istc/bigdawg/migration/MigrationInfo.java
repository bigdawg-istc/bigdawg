/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.query.ConnectionInfo;

/**
 * 
 * Information about the migration: meta data (connection, objects/tables/arrays
 * from/to which migrate the data, etc.)
 *
 * This class is accessible only from within this package.
 * 
 * @author Adam Dziedzic
 */
class MigrationInfo {

	/** The connection to the database from which we migrate the data. */
	private ConnectionInfo connectionFrom;

	/** The array/table from which we migrate the data. */
	private String objectFrom;

	/** The connection to the database to which we migrate the data. */
	private ConnectionInfo connectionTo;

	/** The array/table to which we migrate the data. */
	private String objectTo;

	/**
	 * @param connectionFrom
	 *            the connection to the database from which we migrate the data.
	 * @param objectFrom
	 *            the array/table from which we migrate the data
	 * @param connectionTo
	 *            the connection to the database to which we migrate the data
	 * @param objectTo
	 *            the array/table to which we migrate the data
	 */
	public MigrationInfo(ConnectionInfo connectionFrom, String objectFrom,
			ConnectionInfo connectionTo, String objectTo) {
		this.connectionFrom = connectionFrom;
		this.objectFrom = objectFrom;
		this.connectionTo = connectionTo;
		this.objectTo = objectTo;
	}

	/**
	 * Factory for the migration info only with information about the connection
	 * to the database to which we will load the data.
	 * 
	 * @param connectionTo
	 * @return the instance of the MigrationInfo class
	 */
	public static MigrationInfo forConnectionTo(ConnectionInfo connectionTo) {
		return new MigrationInfo(null, null, connectionTo, null);
	}

	/**
	 * @return the connectionFrom
	 */
	public ConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/**
	 * @return the objectFrom
	 */
	public String getObjectFrom() {
		return objectFrom;
	}

	/**
	 * @return the connectionTo
	 */
	public ConnectionInfo getConnectionTo() {
		return connectionTo;
	}

	/**
	 * @return the objectTo
	 */
	public String getObjectTo() {
		return objectTo;
	}

}
