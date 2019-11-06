/**
 * 
 */
package istc.bigdawg.migration;

import istc.bigdawg.islands.IntraIslandQuery;

import java.io.Serializable;
import java.util.Optional;

/**
 * Additional parameters for data migration.
 * 
 * see: {@link #getCreateStatement()}
 * 
 * @author Adam Dziedzic
 */
public class MigrationParams implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7869941786159490966L;
	/** see: {@link #getCreateStatement()} */
	private String createStatement;
	private String name;

	transient private IntraIslandQuery source;
	transient private IntraIslandQuery target;

	/**
	 * 
	 * @param createStatement
	 *            see: {@link #getCreateStatement()}
	 */
	public MigrationParams(String createStatement) {
		this.createStatement = createStatement;
	}

	public MigrationParams(String createStatement, String name) {
		this(createStatement);
		this.name = name;
	}

	public MigrationParams(String createStatement, IntraIslandQuery source, IntraIslandQuery target) {
		this(createStatement);
		this.source = source;
		this.target = target;
	}

	public MigrationParams(String name, String createStatement, IntraIslandQuery source, IntraIslandQuery target) {
		this(createStatement, source, target);
		this.name = name;
	}


	/**
	 * The create statement (for array/table/object) which was passed directly
	 * by a user.
	 * 
	 * @return the createStatement: the create statement that should be executed
	 *         in the target engine (it can create a target array/table, etc.)
	 *         The data should be loaded to the target object.
	 */
	public Optional<String> getCreateStatement() {
		return Optional.ofNullable(createStatement);
	}

	/**
	 * The name (for array/table/object) used in the corresponding create statement (if any).
	 *
	 * @return the array/table/object name.
	 */
	public Optional<String> getName() {
		return Optional.ofNullable(name);
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

		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;

		if (source != other.source) {
			return false;
		}

		return (target != other.target);
	}

	public IntraIslandQuery getSource() {
		return source;
	}

	public IntraIslandQuery getTarget() {
		return target;
	}
}
