/**
 * 
 */
package istc.bigdawg.migration;

import java.io.Serializable;
import java.util.Optional;

import istc.bigdawg.query.ConnectionInfo;

/**
 * 
 * Information about the migration: meta data (connection, objects/tables/arrays
 * from/to which migrate the data, etc.)
 * 
 * @author Adam Dziedzic
 */
public class MigrationInfo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

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
	 *            the connection to the database from which we migrate the data
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
	 * Similar to:
	 * {@link #MigrationInfo(ConnectionInfo, String, ConnectionInfo, String, MigrationParams)}
	 * but no additional parameters for the migration.
	 * 
	 * @param connectionFrom
	 * @param objectFrom
	 * @param connectionTo
	 * @param objectTo
	 */
	public MigrationInfo(ConnectionInfo connectionFrom, String objectFrom,
			ConnectionInfo connectionTo, String objectTo) {
		this(connectionFrom, objectFrom, connectionTo, objectTo, null);
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
	 * @return the connectionFrom the connection to the database from which we
	 *         migrate the data
	 */
	public ConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/**
	 * @return the objectFrom the array/table from which we migrate the data
	 */
	public String getObjectFrom() {
		return objectFrom;
	}

	/**
	 * @return the connectionTo the connection to the database to which we
	 *         migrate the data
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
	public Optional<MigrationParams> getMigrationParams() {
		return Optional.ofNullable(migrationParams);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MigrationInfo [connectionFrom=" + connectionFrom
				+ ", objectFrom=" + objectFrom + ", connectionTo="
				+ connectionTo + ", objectTo=" + objectTo + ", migrationParams="
				+ migrationParams + "]";
	}

}
