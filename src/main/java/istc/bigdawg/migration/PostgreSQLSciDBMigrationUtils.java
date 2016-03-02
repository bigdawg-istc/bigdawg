/**
 * 
 */
package istc.bigdawg.migration;

import java.util.List;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;

/**
 * Common methods for the migration between SciDB and PostgreSQL.
 * 
 * @author Adam Dziedzic
 * 
 *         Mar 2, 2016 11:54:48 AM
 */
public class PostgreSQLSciDBMigrationUtils {

	/* log */
	private static Logger log = Logger
			.getLogger(PostgreSQLSciDBMigrationUtils.class);

	/**
	 * Check if this is a flat array in SciDB. It also verifies the mapping from
	 * a table in PostgreSQL to an array in SciDB: - number of attributes in
	 * SciDB has to be the same as the number of attributes in the table - the
	 * attributes in SciDB has to be in the same order and have the same name as
	 * columns in the table in PostgreSQL
	 * 
	 * @param scidbArrayMetaData
	 * @param postgresqlTableMetaData
	 * @return
	 * @throws MigrationException
	 */
	public static boolean isFlatArray(SciDBArrayMetaData scidbArrayMetaData,
			PostgreSQLTableMetaData postgresqlTableMetaData)
					throws MigrationException {
		List<SciDBColumnMetaData> scidbDimensionsOrdered = scidbArrayMetaData
				.getDimensionsOrdered();
		// check if this is the flat array only
		if (scidbDimensionsOrdered.size() != 1) {
			return false;
		}
		if (areAttributesSameAsColumns(scidbArrayMetaData,
				postgresqlTableMetaData)) {
			return true;
		}
		return false;
	}

	public static boolean areAttributesSameAsColumns(
			SciDBArrayMetaData scidbArrayMetaData,
			PostgreSQLTableMetaData postgresqlTableMetaData)
					throws MigrationException {
		List<SciDBColumnMetaData> scidbAttributesOrdered = scidbArrayMetaData
				.getAttributesOrdered();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getColumnsOrdered();
		if (scidbAttributesOrdered.size() == postgresColumnsOrdered.size()) {
			/*
			 * check if the flat array attributes are at the same order as
			 * columns in PostgreSQL
			 */
			for (int i = 0; i < scidbAttributesOrdered.size(); ++i) {
				if (!scidbAttributesOrdered.get(i).getColumnName()
						.equals(postgresColumnsOrdered.get(i).getName())) {
					String msg = "The attribute "
							+ postgresColumnsOrdered.get(i).getName()
							+ " from PostgreSQL's table: "
							+ postgresqlTableMetaData.getSchemaTable()
									.getFullName()
							+ " is not matched in the same ORDER with "
							+ "attribute/dimension in the array in SciDB: "
							+ scidbArrayMetaData.getArrayName() + " (position "
							+ i + " PostgreSQL is for the attribute "
							+ postgresColumnsOrdered.get(i).getName()
							+ " whereas the position " + i
							+ " in the array in SciDB is: "
							+ scidbAttributesOrdered.get(i).getColumnName()
							+ ").";
					log.error(msg);
					throw new MigrationException(msg);
				}
			}
			return true;
		}
		return false;
	}

}
