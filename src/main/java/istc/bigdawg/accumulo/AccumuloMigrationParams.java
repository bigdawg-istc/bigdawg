package istc.bigdawg.accumulo;

import org.apache.accumulo.core.data.Range;

import istc.bigdawg.migration.MigrationParams;

public class AccumuloMigrationParams extends MigrationParams {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5420157002948174313L;
	private Range sourceTableRange = null;
	
	public AccumuloMigrationParams(Range sourceTableRange) {
		super(null);
		this.sourceTableRange = sourceTableRange;
	}

	public Range getSourceTableRange() {
		return sourceTableRange;
	}
	
	

}
