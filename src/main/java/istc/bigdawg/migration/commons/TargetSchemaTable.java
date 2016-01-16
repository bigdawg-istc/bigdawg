/**
 * 
 */
package istc.bigdawg.migration.commons;

import istc.bigdawg.postgresql.PostgreSQLSchemaTableName;

/**
 * It/what schema/table was created for PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Jan 15, 2016 3:47:23 PM
 */
public class TargetSchemaTable {
	private PostgreSQLSchemaTableName schemaTableName;
	private boolean wasSchemaCreated;
	private boolean wasTableCreated;

	public TargetSchemaTable(PostgreSQLSchemaTableName schemaTableName, boolean wasSchemaCreated,
			boolean wasTableCreated) {
		this.wasSchemaCreated = wasSchemaCreated;
		this.wasTableCreated = wasTableCreated;
	}

	public boolean wasSchemaCreated() {
		return wasSchemaCreated;
	}

	public boolean wasTableCreated() {
		return wasTableCreated;
	}

	/**
	 * @return the schemaTableName
	 */
	public PostgreSQLSchemaTableName getSchemaTableName() {
		return schemaTableName;
	}
}