/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import istc.bigdawg.database.AttributeMetaData;
import istc.bigdawg.database.ObjectMetaData;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.migration.datatypes.FromSQLTypesToSciDB;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;

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

	public static void removeIntermediateArrays(SciDBArrays arrays,
			MigrationInfo migrationInfo) throws MigrationException {
		/**
		 * the migration was successful so only clear the intermediate arrays
		 */
		MigrationUtils.removeArrays(migrationInfo.getConnectionTo(),
				"clean the intermediate arrays",
				arrays.getIntermediateArrays());
	}

	/**
	 * Create a flat array in SciDB from the meta info about the table in
	 * PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 * @throws MigrationException
	 */
	public static void createFlatArray(String arrayName,
			MigrationInfo migrationInfo, ObjectMetaData objectToMetaData)
					throws SQLException, UnsupportedTypeException,
					MigrationException {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + arrayName + " <");
		List<AttributeMetaData> postgresColumnsOrdered = objectToMetaData
				.getAttributesOrdered();
		for (AttributeMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String attributeName = postgresColumnMetaData.getName();
			String postgresColumnType = postgresColumnMetaData.getDataType();
			String attributeType = FromSQLTypesToSciDB
					.getSciDBTypeFromSQLType(postgresColumnType);
			String attributeNULL = "";
			if (postgresColumnMetaData.isNullable()) {
				attributeNULL = " NULL";
			}
			createArrayStringBuf.append(
					attributeName + ":" + attributeType + attributeNULL + ",");
		}
		/* delete the last comma "," */
		createArrayStringBuf.deleteCharAt(createArrayStringBuf.length() - 1);
		/* " r_regionkey:int64,r_name:string,r_comment:string> );" */
		/* this is by default 1 mln cells in a chunk */
		createArrayStringBuf.append("> [_flat_dimension_=0:*,1000000,0]");
		SciDBHandler handler = new SciDBHandler(
				migrationInfo.getConnectionTo());
		handler.executeStatement(createArrayStringBuf.toString());
		handler.commit();
		handler.close();
	}

	/**
	 * Extract create target object statement (e.g. create statement for a
	 * target array/table).
	 * 
	 * @param migrationInfo
	 *            Information about data migration.
	 * @return String representing the create statement.
	 */
	public static String getUserCreateStatement(MigrationInfo migrationInfo) {
		MigrationParams migrationParams = migrationInfo.getMigrationParams()
				.get();
		if (migrationParams != null) {
			return migrationParams.getCreateStatement().get();
		} else {
			return null;
		}
	}

	/**
	 * Get the create table statement from the parameters to the migration (the
	 * create statement was passed directly by a user).
	 * 
	 * @throws SQLException
	 * @throws LocalQueryExecutionException
	 * 
	 * @return the name of the created array
	 */
	public static String createArrayFromUserStatement(
			MigrationInfo migrationInfo)
					throws SQLException, LocalQueryExecutionException {
		String toArray = migrationInfo.getObjectTo();
		String createArrayStatement = getUserCreateStatement(migrationInfo);
		if (createArrayStatement != null) {
			log.debug("create the array from the statement provided by a user: "
					+ createArrayStatement);
			if (!createArrayStatement.contains(toArray)) {
				throw new IllegalArgumentException(
						"The object to which we have "
								+ "to load the data has a different name "
								+ "than the object specified in the create statement.");
			}
			SciDBHandler localHandler = new SciDBHandler(
					migrationInfo.getConnectionTo());
			localHandler.execute(createArrayStatement);
			localHandler.commit();
			localHandler.close();
			return toArray;
		}
		return null;
	}

	/**
	 * Prepare flat and target arrays in SciDB to load the data.
	 * 
	 * @throws SQLException
	 * @throws MigrationException
	 * @throws UnsupportedTypeException
	 * @throws LocalQueryExecutionException
	 * @throws NoTargetArrayException
	 * 
	 */
	public static SciDBArrays prepareFlatTargetArrays(
			MigrationInfo migrationInfo, ObjectMetaData fromObjectMetaData)
					throws MigrationException, SQLException,
					UnsupportedTypeException, LocalQueryExecutionException {
		SciDBHandler handler = new SciDBHandler(
				migrationInfo.getConnectionTo());
		String toArray = migrationInfo.getObjectTo();
		SciDBArrayMetaData arrayMetaData = null;
		SciDBArray flatArray;
		SciDBArray multiDimArray = null;

		String createdArrayName = createArrayFromUserStatement(migrationInfo);
		if (createdArrayName != null) {
			SciDBArrayMetaData createdArrayMetaData = null;
			try {
				createdArrayMetaData = handler
						.getArrayMetaData(createdArrayName);
			} catch (NoTargetArrayException e) {
				String message = "It should not happen - the migrator could not create "
						+ "a target array using the statment provided by user. "
						+ e.getMessage();
				log.error(message + " " + StackTrace.getFullStackTrace(e));
				throw new MigrationException(message);
			}
			if (MigrationUtils.isFlatArray(createdArrayMetaData)) {
				flatArray = new SciDBArray(createdArrayName, true, false);
				return new SciDBArrays(flatArray, null);
			} else {
				multiDimArray = new SciDBArray(createdArrayName, true, false);
			}
		}
		try {
			arrayMetaData = handler.getArrayMetaData(toArray);
		} catch (NoTargetArrayException e) {
			/*
			 * When only a name of array in SciDB was given, but the array does
			 * not exist in SciDB then we have to create the target array which
			 * by default is flat.
			 */
			createFlatArray(toArray, migrationInfo, fromObjectMetaData);
			flatArray = new SciDBArray(toArray, true, false);
			/* the data should be loaded to the default flat array */
			return new SciDBArrays(flatArray, null);
		}
		handler.close();
		if (MigrationUtils.isFlatArray(arrayMetaData)) {
			return new SciDBArrays(new SciDBArray(toArray, false, false), null);
		}
		/*
		 * the target array is multidimensional so we have to build the
		 * intermediate flat array
		 */
		/*
		 * check if every column from Postgres is mapped to a column/attribute
		 * in SciDB's arrays (the attributes from the flat array can change to
		 * dimensions in the multi-dimensional array, thus we cannot verify the
		 * match of columns in PostgreSQL and dimensions/attributes in SciDB)
		 */
		Map<String, AttributeMetaData> dimensionsMap = arrayMetaData
				.getDimensionsMap();
		Map<String, AttributeMetaData> attributesMap = arrayMetaData
				.getAttributesMap();
		List<AttributeMetaData> attributesOrdered = fromObjectMetaData
				.getAttributesOrdered();
		for (AttributeMetaData attributeMetaData : attributesOrdered) {
			String attributeName = attributeMetaData.getName();
			if (!dimensionsMap.containsKey(attributeName)
					&& !attributesMap.containsKey(attributeName)) {
				throw new MigrationException("The attribute " + attributeName
						+ " from object (table/array): "
						+ migrationInfo.getObjectFrom()
						+ " is not matched with any attribute/dimension "
						+ "in the array in SciDB: " + toArray);
			}
		}
		String newFlatIntermediateArrayName = toArray + "__bigdawg__flat__"
				+ SessionIdentifierGenerator.INSTANCE.nextRandom26CharString();
		createFlatArray(newFlatIntermediateArrayName, migrationInfo,
				fromObjectMetaData);
		flatArray = new SciDBArray(newFlatIntermediateArrayName, true, true);
		/*
		 * check if we already have the multiDimArray - then we have been
		 * provided with the create statement by a user
		 */
		if (multiDimArray != null) {
			multiDimArray = new SciDBArray(toArray, false, false);
		}
		return new SciDBArrays(flatArray, multiDimArray);
	}

}
