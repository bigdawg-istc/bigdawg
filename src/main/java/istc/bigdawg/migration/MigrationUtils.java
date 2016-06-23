/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.datatypes.FromSQLTypesToSciDB;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBHandler;

/**
 * Common methods/utilities for the migration process.
 * 
 * @author Adam Dziedzic
 */
public class MigrationUtils {

	/* log */
	private static Logger log = Logger.getLogger(MigrationUtils.class);

	/**
	 * Check if this is a flat array in SciDB.
	 * 
	 * @param scidbArrayMetaData
	 * @return true if there are any dimensions in the array.
	 * @throws MigrationException
	 */
	public static boolean isFlatArray(SciDBArrayMetaData scidbArrayMetaData)
			throws MigrationException {
		List<AttributeMetaData> scidbDimensionsOrdered = scidbArrayMetaData
				.getDimensionsOrdered();
		if (scidbDimensionsOrdered.size() != 1) {
			return false;
		}
		return false;
	}

	/**
	 * Check if only the attributes in both objects are the same.
	 * 
	 * @param metaDataFrom
	 * @param metaDataTo
	 * @return true if attributes in both objects/tables/arrays have the same
	 *         cardinality and names (in the same order). We take into account
	 *         the order because the data in different formats are ordered and
	 *         this order have to be the same for migration.
	 * @throws MigrationException
	 */
	public static boolean areAttributesTheSame(ObjectMetaData metaDataFrom,
			ObjectMetaData metaDataTo) throws MigrationException {
		List<AttributeMetaData> attrOrderedFrom = metaDataFrom
				.getAttributesOrdered();
		List<AttributeMetaData> attrOrderedTo = metaDataTo
				.getAttributesOrdered();
		if (attrOrderedFrom.size() == attrOrderedTo.size()) {
			/*
			 * check if the flat array attributes are at the same order as
			 * columns in PostgreSQL
			 */
			for (int i = 0; i < attrOrderedFrom.size(); ++i) {
				if (!attrOrderedFrom.get(i).getName()
						.equals(attrOrderedTo.get(i).getName())) {
					String msg = "The attribute "
							+ attrOrderedTo.get(i).getName() + " from: "
							+ metaDataTo.getName()
							+ " is not matched in the same ORDER with "
							+ "attribute in the object: "
							+ metaDataFrom.getName()
							+ " (in the first object, the position " + i
							+ " is for the attribute "
							+ attrOrderedTo.get(i).getName()
							+ " whereas the position " + i
							+ " in the other object is: "
							+ attrOrderedFrom.get(i).getName() + ").";
					log.error(msg);
					throw new MigrationException(msg);
				}
			}
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
	 * @return true if attributes and dimensions in the array match exactly the
	 *         columns in the table
	 * @throws MigrationException
	 */
	public static boolean areDimensionsAndAttributesSameAsColumns(
			SciDBArrayMetaData scidbArrayMetaData,
			PostgreSQLTableMetaData postgresqlTableMetaData)
					throws MigrationException {
		List<AttributeMetaData> scidbColumnsOrdered = new ArrayList<AttributeMetaData>();
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getDimensionsOrdered());
		scidbColumnsOrdered.addAll(scidbArrayMetaData.getAttributesOrdered());
		return areAttributesTheSame(scidbArrayMetaData,
				postgresqlTableMetaData);
	}

	/**
	 * example types=int32_t,int32_t null,double,double null,string,string null
	 * 
	 * @param objectMetaData
	 *            meta data for another object (by default the object should
	 *            support SQL types)
	 * @return String representing SciDB bin format for the transformation.
	 * 
	 * 
	 * @throws UnsupportedTypeException
	 */
	public static String getSciDBBinFormat(ObjectMetaData objectMetaData)
			throws UnsupportedTypeException {
		StringBuilder binFormatBuffer = new StringBuilder();
		List<AttributeMetaData> attrOrdered = objectMetaData
				.getAttributesOrdered();
		for (AttributeMetaData attrMetaData : attrOrdered) {
			String postgresColumnType = attrMetaData.getDataType();
			String attributeType = FromSQLTypesToSciDB
					.getSciDBTypeFromSQLType(postgresColumnType);
			String attributeNULL = "";
			if (attrMetaData.isNullable()) {
				attributeNULL = " null";
			}
			binFormatBuffer.append(attributeType + attributeNULL + ",");
		}
		/* delete the last comma "," */
		binFormatBuffer.deleteCharAt(binFormatBuffer.length() - 1);
		return binFormatBuffer.toString();
	}

	/**
	 * Remove the given array from SciDB.
	 * 
	 * @param connectionTo
	 * @param arrayName
	 * @throws SQLException
	 */
	public static void removeArray(ConnectionInfo connectionTo,
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
	public static void removeArrays(ConnectionInfo connection, String msg,
			Set<String> arrays) throws MigrationException {
		/* remove the arrays */
		for (String array : arrays) {
			try {
				MigrationUtils.removeArray(connection, array);
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
