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
	 * Additional parameters for the migration process. {@link MigrationParams }
	 */
	private MigrationParams migrationParams;

	/**
	 * @param connectionFrom
	 *            the connection to the database from which we migrate the data.
	 * @param objectFrom
	 *            the array/table from which we migrate the data
	 * @param connectionTo
	 *            the connection to the database to which we migrate the data
	 * @param migrationParams
	 *            additional parameters for the migration process
	 *            {@link MigrationParams }
	 * @param objectTo
	 *            the array/table to which we migrate the data
	 */
	public MigrationInfo(ConnectionInfo connectionFrom, String objectFrom,
			ConnectionInfo connectionTo, String objectTo,
			MigrationParams migrationParams) {
		this.connectionFrom = connectionFrom;
		this.objectFrom = objectFrom;
		this.connectionTo = connectionTo;
		this.objectTo = objectTo;
		this.migrationParams = migrationParams;
	}

	/**
	 * Factory for the migration info only with information about the connection
	 * to the database to which we will load the data.
	 * 
	 * @param connectionTo
	 * @return the instance of the MigrationInfo class
	 */
	public static MigrationInfo forConnectionTo(ConnectionInfo connectionTo) {
		return new MigrationInfo(null, null, connectionTo, null, null);
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
	 * @return the objectTo the array/table from which we migrate the data; see:
	 *         {@link #MigrationInfo(ConnectionInfo, String, ConnectionInfo, String, MigrationParams)}
	 */
	public String getObjectTo() {
		return objectTo;
	}

	/**
	 * 
	 * @return Additional parameters for the migration process.
	 *         {@link MigrationParams } see:
	 *         {@link #MigrationInfo(ConnectionInfo, String, ConnectionInfo, String, MigrationParams)}
	 */
	public MigrationParams getMigrationParams() {
		return migrationParams;
	}

}
