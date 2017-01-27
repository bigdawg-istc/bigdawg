package istc.bigdawg.accumulo;

import org.apache.accumulo.core.data.Range;

import istc.bigdawg.migration.MigrationParams;

public class AccumuloMigrationParams extends MigrationParams {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5420157002948174313L;
	private Range sourceTableRange = null;

	/**
	 * Create migration params for Accumulo as a source database.
	 * 
	 * @param queryString
	 *            The query string to be executed in the destination database
	 *            which creates the target object/table/array, etc.
	 * @param sourceTableRange
	 *            The range for scanning of table in Accumulo, when we do
	 *            migration from Accumulo to another database engine.
	 */
	public AccumuloMigrationParams(String queryString, Range sourceTableRange) {
		super(queryString);
		this.sourceTableRange = sourceTableRange;
	}

	public Range getSourceTableRange() {
		return sourceTableRange;
	}

}
