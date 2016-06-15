/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;

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
	 * attributes in SciDB have to be in the same order and have the same names
	 * as columns in the table in PostgreSQL
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

	/**
	 * Check if only the attributes (without dimensions) in the array in SciDB
	 * are the same as the columns in PostgreSQL.
	 * 
	 * @param scidbArrayMetaData
	 * @param postgresqlTableMetaData
	 * @return true if attributes in the array match exactly the columns in the
	 *         table
	 * @throws MigrationException
	 */
	public static boolean areAttributesSameAsColumns(
			SciDBArrayMetaData scidbArrayMetaData,
			PostgreSQLTableMetaData postgresqlTableMetaData)
					throws MigrationException {
		List<SciDBColumnMetaData> scidbAttributesOrdered = scidbArrayMetaData
				.getAttributesOrdered();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getColumnsOrdered();
		return areSciDBColumnsSameAsPostgresColumns(scidbArrayMetaData,
				postgresqlTableMetaData, scidbAttributesOrdered,
				postgresColumnsOrdered);
	}

	/**
	 * Check if only the attributes (without dimensions) in the array in SciDB
	 * are the same as the columns in PostgreSQL.
	 * 
	 * @param scidbArrayMetaData
	 * @param postgresqlTableMetaData
	 * @return true if attributes and dimensions in the array match exactly the
	 *         columns in the table
	 * @throws MigrationException
	 */
	public static boolean areDimensionsAndAttributesSameAsColumns(
			SciDBArrayMetaData scidbArrayMetaData,
			PostgreSQLTableMetaData postgresqlTableMetaData)
					throws MigrationException {
		List<SciDBColumnMetaData> scidbColumnsOrdered = new ArrayList<SciDBColumnMetaData>();
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getDimensionsOrdered());
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getAttributesOrdered());
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getColumnsOrdered();
		return areSciDBColumnsSameAsPostgresColumns(scidbArrayMetaData,
				postgresqlTableMetaData, scidbColumnsOrdered,
				postgresColumnsOrdered);
	}

	/**
	 * Check if the given (arbitrary) dimensions/attributes or mix of them given
	 * in the list are the same as the columns in PostgreSQL table: we check
	 * names in order (we do not verify the data types here).
	 * 
	 * @return true if all the attributes/dimensions in SciDB match the columns
	 *         in PostgreSQL.
	 * @throws MigrationException
	 */
	public static boolean areSciDBColumnsSameAsPostgresColumns(
			SciDBArrayMetaData scidbArrayMetaData,
			PostgreSQLTableMetaData postgresqlTableMetaData,
			List<SciDBColumnMetaData> scidbColumnsOrdered,
			List<PostgreSQLColumnMetaData> postgresColumnsOrdered)
					throws MigrationException {
		if (scidbColumnsOrdered.size() == postgresColumnsOrdered.size()) {
			/*
			 * check if the flat array attributes are at the same order as
			 * columns in PostgreSQL
			 */
			for (int i = 0; i < scidbColumnsOrdered.size(); ++i) {
				if (!scidbColumnsOrdered.get(i).getColumnName()
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
							+ scidbColumnsOrdered.get(i).getColumnName() + ").";
					log.error(msg);
					throw new MigrationException(msg);
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Remove the given array from SciDB.
	 * 
	 * @param connectionTo
	 * @param arrayName
	 * @throws SQLException
	 */
	public static void removeArray(SciDBConnectionInfo connectionTo,
			String arrayName) throws SQLException {
		SciDBHandler handler = new SciDBHandler(connectionTo);
		handler.executeStatement("drop array " + arrayName);
		handler.close();
	}

	/**
	 * Remove the intermediate flat array if it was created. If the
	 * multi-dimensional array was the target one, then the intermediate array
	 * should be deleted (the array was created in this migration process).
	 * 
	 * @param msg
	 *            the message constructed up to this moment (it is the
	 *            information about this special action)
	 * @throws MigrationException
	 */
	public static void removeArrays(SciDBConnectionInfo connection, String msg,
			Set<String> arrays) throws MigrationException {
		/* remove the arrays */
		for (String array : arrays) {
			try {
				PostgreSQLSciDBMigrationUtils.removeArray(connection, array);
			} catch (SQLException e1) {
				e1.printStackTrace();
				msg = "Could not remove intermediate arrays from SciDB! " + msg;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
		arrays.clear();
	}

}
