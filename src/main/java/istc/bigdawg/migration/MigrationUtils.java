/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
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
import istc.bigdawg.migration.FromSciDBToPostgres.MigrationType;
import istc.bigdawg.migration.datatypes.FromSQLTypesToSciDB;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.scidb.SciDBArrayDimensionsAndAttributesMetaData;
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

	/*
	 * The statement to be used in a database to create an object (table/array,
	 * this is a temporal variable for consumer from a lambda function
	 */
	private static String createStatement = null;

	/**
	 * Check if this is a flat array in SciDB.
	 * 
	 * We check if the attributes: names, positions and types are equivalent.
	 * This is current assumption and can be changed in future.
	 * 
	 * @param scidbArrayMetaData
	 * @param object
	 *            to meta data
	 * @return true if there are any dimensions in the array.
	 * @throws MigrationException
	 * @throws UnsupportedTypeException
	 */
	public static boolean isFlatArray(SciDBArrayMetaData scidbArrayMetaData,
			ObjectMetaData objectMetaData)
					throws MigrationException, UnsupportedTypeException {
		List<AttributeMetaData> objectAttributesOrdered = objectMetaData
				.getAttributesOrdered();
		List<AttributeMetaData> scidbAttributesOrdered = scidbArrayMetaData
				.getAttributesOrdered();
		if (scidbArrayMetaData.getDimensionsOrdered().size() > 1) {
			return false;
		}
		if (objectAttributesOrdered.size() != scidbAttributesOrdered.size()) {
			return false;
		}
		Iterator<AttributeMetaData> fromIter = objectAttributesOrdered
				.iterator();
		Iterator<AttributeMetaData> toIter = scidbAttributesOrdered.iterator();
		while (fromIter.hasNext() && toIter.hasNext()) {
			AttributeMetaData fromAttributeMetaData = fromIter.next();
			AttributeMetaData toAttributeMetaData = toIter.next();
			if (!fromAttributeMetaData.getName()
					.equals(toAttributeMetaData.getName())
					|| fromAttributeMetaData.isNullable() != toAttributeMetaData
							.isNullable()
					|| !toAttributeMetaData.getSqlDataType()
							.equals(fromAttributeMetaData.getSqlDataType())) {
				return false;
			}
		}
		return true;
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
		return areAttributesTheSame(metaDataFrom.getName(), attrOrderedFrom,
				metaDataTo.getName(), attrOrderedTo);
	}

	/**
	 * Check if the given list of attributes/dimensions are the same in respect
	 * of their names.
	 * 
	 * @param objectFrom
	 *            table/array from which we export the data
	 * @param attrOrderedFrom
	 *            the attributes that belong to the objectFrom
	 * @param objectTo
	 *            table/array to which we load the data
	 * @param attrOrderedTo
	 *            the attributes that belong to the object to which we load the
	 *            data
	 * @return true if the attributes are the same with respect to their names
	 * @throws MigrationException
	 */
	private static boolean areAttributesTheSame(String objectFrom,
			List<AttributeMetaData> attrOrderedFrom, String objectTo,
			List<AttributeMetaData> attrOrderedTo) throws MigrationException {
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
							+ objectTo
							+ " is not matched in the same ORDER with "
							+ "attribute in the object: " + objectFrom
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
	 * Check if dimensions and attributes are the same in both objects.
	 */
	public static boolean areDimensionsAndAttributesTheSame(
			ObjectMetaData metaDataFrom, ObjectMetaData metaDataTo)
					throws MigrationException {
		List<AttributeMetaData> fromDimensionsAttributes = new ArrayList<AttributeMetaData>();
		fromDimensionsAttributes.addAll(metaDataFrom.getDimensionsOrdered());
		fromDimensionsAttributes.addAll(metaDataFrom.getAttributesOrdered());
		List<AttributeMetaData> toDimensionsAttributes = new ArrayList<AttributeMetaData>();
		toDimensionsAttributes.addAll(metaDataTo.getDimensionsOrdered());
		toDimensionsAttributes.addAll(metaDataTo.getAttributesOrdered());
		return areAttributesTheSame(metaDataFrom.getName(),
				fromDimensionsAttributes, metaDataTo.getName(),
				toDimensionsAttributes);
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
			String postgresColumnType = attrMetaData.getSqlDataType();
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
		if (arrays == null) {
			return;
		}
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
		if (arrays != null) {
			/**
			 * the migration was successful so only clear the intermediate
			 * arrays
			 */
			MigrationUtils.removeArrays(migrationInfo.getConnectionTo(),
					"clean the intermediate arrays",
					arrays.getIntermediateArrays());
		}
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
		List<AttributeMetaData> fromAttributesOrdered = objectToMetaData
				.getAttributesOrdered();
		for (AttributeMetaData fromattributeMetaData : fromAttributesOrdered) {
			String attributeName = fromattributeMetaData.getName();
			String postgresColumnType = fromattributeMetaData.getSqlDataType();
			String attributeType = FromSQLTypesToSciDB
					.getSciDBTypeFromSQLType(postgresColumnType);
			String attributeNULL = "";
			if (fromattributeMetaData.isNullable()) {
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
	public synchronized static String getUserCreateStatement(
			MigrationInfo migrationInfo) {
		createStatement = null;
		migrationInfo.getMigrationParams().ifPresent(
				params -> params.getCreateStatement().ifPresent(statement -> {
					createStatement = statement;
				}));
		return createStatement;
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
	 * @throws Exception
	 * 
	 * @throws NoTargetArrayException
	 * 
	 */
	public static SciDBArrays prepareFlatTargetArrays(
			MigrationInfo migrationInfo, ObjectMetaData fromObjectMetaData)
					throws Exception {
		String toArray = migrationInfo.getObjectTo();
		SciDBArrayMetaData arrayMetaData = null;
		SciDBArray flatArray;
		SciDBArray multiDimArray = null;

		String createdArrayName = createArrayFromUserStatement(migrationInfo);
		if (createdArrayName != null) {
			SciDBArrayMetaData createdArrayMetaData = null;
			try {
				SciDBHandler handler = new SciDBHandler(
						migrationInfo.getConnectionTo());
				createdArrayMetaData = handler
						.getObjectMetaData(createdArrayName);
				handler.close();
			} catch (NoTargetArrayException e) {
				String message = "It should not happen - the migrator could not create "
						+ "a target array using the statment provided by user. "
						+ e.getMessage();
				log.error(message + " " + StackTrace.getFullStackTrace(e));
				throw new MigrationException(message);
			}
			if (MigrationUtils.isFlatArray(createdArrayMetaData,
					fromObjectMetaData)) {
				flatArray = new SciDBArray(createdArrayName, true, false);
				return new SciDBArrays(flatArray, null);
			} else {
				multiDimArray = new SciDBArray(createdArrayName, true, false);
			}
		}
		SciDBHandler handler = null;
		try {
			handler = new SciDBHandler(migrationInfo.getConnectionTo());
			arrayMetaData = handler.getObjectMetaData(toArray);
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
		} finally {
			if (handler != null) {
				handler.close();
			}
		}
		if (MigrationUtils.isFlatArray(arrayMetaData, fromObjectMetaData)) {
			return new SciDBArrays(new SciDBArray(toArray, false, false), null);
		}
		multiDimArray = new SciDBArray(toArray, false, false);
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
		return new SciDBArrays(flatArray, multiDimArray);
	}

	/**
	 * Decide the migration type (transfer (only the attributes) or (attributes
	 * and dimensions)) for SciDB.
	 * 
	 * @throws MigrationException
	 *             {@link MigrationException}
	 */
	public static MigrationType getMigrationType(MigrationInfo migrationInfo,
			DBHandler toHandler) throws MigrationException {
		SciDBHandler fromHandler = null;
		try {
			fromHandler = new SciDBHandler(migrationInfo.getConnectionFrom());
			SciDBArrayMetaData scidbArrayMetaData = fromHandler
					.getObjectMetaData(migrationInfo.getObjectFrom());
			String toObject = migrationInfo.getObjectTo();
			if (toHandler.existsObject(toObject)) {
				ObjectMetaData objectToMetaData = toHandler
						.getObjectMetaData(migrationInfo.getObjectFrom());
				// can we migrate only the attributes from the SciDB array
				List<AttributeMetaData> scidbAttributesOrdered = scidbArrayMetaData
						.getAttributesOrdered();
				List<AttributeMetaData> toAttributesOrdered = objectToMetaData
						.getAttributesOrdered();
				if (toAttributesOrdered.size() == scidbAttributesOrdered.size()
						&& MigrationUtils.areAttributesTheSame(
								scidbArrayMetaData, objectToMetaData)) {
					return MigrationType.FLAT;
				} /*
					 * check if the dimensions and the attributes in the array
					 * match the columns in the table
					 */
				else {
					/*
					 * verify the dimensions and attributes in the array with
					 * the columns in the table
					 */
					List<AttributeMetaData> scidbDimensionsAttributes = new ArrayList<AttributeMetaData>();
					scidbDimensionsAttributes
							.addAll(scidbArrayMetaData.getDimensionsOrdered());
					scidbDimensionsAttributes
							.addAll(scidbArrayMetaData.getAttributesOrdered());

					if (MigrationUtils.areAttributesTheSame(
							new SciDBArrayDimensionsAndAttributesMetaData(
									scidbArrayMetaData.getArrayName(),
									scidbDimensionsAttributes),
							objectToMetaData)) {
						return MigrationType.FULL;
					} else {
						return MigrationType.FLAT;
					}
				}
			} else {
				return MigrationType.FULL;
			}
		} catch (SQLException ex) {
			String message = "Problem with connection to one of the databases. "
					+ ex.getMessage();
			throw new MigrationException(message);
		} catch (Exception ex) {
			String message = "Problem with checking meta data. "
					+ ex.getMessage();
			throw new MigrationException(message);
		} finally {
			if (fromHandler != null) {
				try {
					fromHandler.close();
				} catch (SQLException e) {
					log.error("Could not close the handler for SciDB. "
							+ e.getMessage() + " "
							+ StackTrace.getFullStackTrace(e), e);
				}
			}
		}
	}
}
