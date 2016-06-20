/**
 * 
 */
package istc.bigdawg.migration;

/**
 * 
 * Additional parameters for data migration.
 * 
 * see: {@link #getCreateStatement()}
 * 
 * @author Adam Dziedzic
 */
public class MigrationParams {

	/** see: {@link #getCreateStatement()} */
	private String createStatement;

	/**
	 * 
	 * @param createStatement
	 *            see: {@link #getCreateStatement()}
	 */
	public MigrationParams(String createStatement) {
		this.createStatement = createStatement;
	}

	/**
	 * 
	 * @return the createStatement: the create statement that should be executed
	 *         in the target engine (it can create a target array/table, etc.)
	 *         and to which the data should be loaded.
	 */
	public String getCreateStatement() {
		return createStatement;
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
		result = prime * result
				+ ((createStatement == null) ? 0 : createStatement.hashCode());
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
		MigrationParams other = (MigrationParams) obj;
		if (createStatement == null) {
			if (other.createStatement != null)
				return false;
		} else if (!createStatement.equals(other.createStatement))
			return false;
		return true;
	}

}
