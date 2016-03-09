/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.datatypes.DataTypesFromPostgreSQLToSciDB;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.StackTrace;

/**
 * Implementation of the migration from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToSciDBImplementation
		implements MigrationImplementation {

	/* log */
	private static Logger log = Logger
			.getLogger(FromPostgresToSciDBImplementation.class);

	/* General message about the action in the class. */
	private static String generalMessage = "Data migration from PostgreSQL to SciDB";

	/* General error message when the migration fails in the class. */
	private static String errMessage = generalMessage + " failed! ";

	/*
	 * These are the arrays that were created during migration of data from
	 * PostgreSQL to SciDB. If something fails on the way, then the arrays
	 * should be removed.
	 */
	private Set<String> createdArrays = new HashSet<>();

	private PostgreSQLConnectionInfo connectionFrom;
	private String fromTable;
	private SciDBConnectionInfo connectionTo;
	private String toArray;
	private PostgreSQLTableMetaData postgresqlTableMetaData;

	/* Resources that have to be cleaned at the end of the migration process. */
	private String postgresPipe = null;
	private String scidbPipe = null;
	private ExecutorService executor = null;
	private Connection connectionPostgres = null;

	/**
	 * Initialize the migration from PostgreSQL to SciDB.
	 * 
	 * @param connectionFrom
	 *            the connection to PostgreSQL
	 * @param fromTable
	 *            the name of the table in PostgreSQL to be migrated (from which
	 *            we export the data)
	 * @param connectionTo
	 *            the connection to SciDB database
	 * @param toArray
	 *            the name of the array in SciDB to which we load the data
	 * @throws MigrationException
	 */
	public FromPostgresToSciDBImplementation(
			PostgreSQLConnectionInfo connectionFrom, String fromTable,
			SciDBConnectionInfo connectionTo, String toArray)
					throws MigrationException {
		this.connectionFrom = connectionFrom;
		this.fromTable = fromTable;
		this.connectionTo = connectionTo;
		this.toArray = toArray;
		try {
			this.postgresqlTableMetaData = new PostgreSQLHandler(connectionFrom)
					.getColumnsMetaData(fromTable);
		} catch (SQLException postgresException) {
			MigrationException migrateException = handleException(
					postgresException, "Extraction of meta data on the table: "
							+ fromTable + " in PostgreSQL failed. ");
			throw migrateException;
		}
	}

	/**
	 * Binary migration.
	 * 
	 * @return {@link MigrationResult }
	 * @throws MigrationException
	 */
	public MigrationResult migrateBin() throws MigrationException {
		log.info(generalMessage + " Mode: binary migration.");
		long startTimeMigration = System.currentTimeMillis();
		try {
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromPostgres_" + fromTable);
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toSciDB_" + toArray);

			SciDBArrays arrays = prepareFlatTargetArrays();
			executor = Executors.newFixedThreadPool(3/* 3 */);

			String copyFromCommand = PostgreSQLHandler
					.getExportBinCommand(fromTable);
			CopyFromPostgresExecutor exportExecutor = new CopyFromPostgresExecutor(
					connectionFrom, copyFromCommand, postgresPipe);
			FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(
					postgresPipe, scidbPipe, getSciDBBinFormat(),
					TransformBinExecutor.TYPE.FromPostgresToSciDB);
			FutureTask<Long> transformTask = new FutureTask<Long>(
					transformExecutor);
			executor.submit(transformTask);

			LoadToSciDBExecutor loadExecutor = new LoadToSciDBExecutor(
					connectionTo, arrays, scidbPipe, getSciDBBinFormat());
			FutureTask<String> loadTask = new FutureTask<String>(loadExecutor);
			executor.submit(loadTask);

			long transformationResult = transformTask.get();
			String transformationMessage;
			if (transformationResult != 0) {
				String message = "Check the C++ migrator! It might need to be compiled and checked separately!";
				log.error(message);
				throw new MigrationException(message);
			} else {
				transformationMessage = "Transformation finished successfuly!";
			}
			long extractedRowsCount = exportTask.get();

			String loadMessage = loadTask.get();

			removeFlatArrayIfIntermediate(arrays);
			long endTimeMigration = System.currentTimeMillis();
			String message = "bin migration from PostgreSQL to SciDB execution time: "
					+ (endTimeMigration - startTimeMigration);
			log.info(message);
			System.out.println(message);
			return new MigrationResult(extractedRowsCount, null,
					loadMessage + "No information about number of loaded rows."
							+ " Result of transformation: "
							+ transformationMessage,
					false);
		} catch (SQLException | UnsupportedTypeException | InterruptedException
				| ExecutionException | IOException
				| RunShellException exception) {
			MigrationException migrationException = handleException(exception,
					"Migration in binary Format failed. ");
			throw migrationException;
		} finally {
			cleanResources();
		}
	}

	/**
	 * This is migration from PostgreSQL to SciDB based on CSV format and
	 * carried out in a single thread.
	 * 
	 * 
	 * 
	 * @return MigrationRestult information about the executed migration
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public MigrationResult migrateSingleThreadCSV() throws MigrationException {
		log.info(generalMessage + " Mode: migrateSingleThreadCSV");
		long startTimeMigration = System.currentTimeMillis();
		String delimiter = "|";
		try {
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_fromPostgres_" + fromTable);
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(
					this.getClass().getName() + "_toSciDB_" + toArray);
			executor = Executors.newFixedThreadPool(3);

			CopyFromPostgresExecutor exportExecutor = new CopyFromPostgresExecutor(
					connectionFrom,
					PostgreSQLHandler.getExportCsvCommand(fromTable, delimiter),
					postgresPipe);
			FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
			executor.submit(exportTask);

			String typesPattern = SciDBHandler
					.getTypePatternFromPostgresTypes(postgresqlTableMetaData);
			TransformFromCsvToSciDBExecutor csvSciDBExecutor = new TransformFromCsvToSciDBExecutor(
					typesPattern, postgresPipe, delimiter, scidbPipe,
					connectionTo.getBinPath());
			FutureTask<Integer> csvSciDBTask = new FutureTask<Integer>(
					csvSciDBExecutor);
			executor.submit(csvSciDBTask);

			SciDBArrays arrays = prepareFlatTargetArrays();
			LoadToSciDBExecutor loadExecutor = new LoadToSciDBExecutor(
					connectionTo, arrays, scidbPipe);
			FutureTask<String> loadTask = new FutureTask<String>(loadExecutor);
			executor.submit(loadTask);

			long extractedRowsCount = exportTask.get();
			csvSciDBTask.get();
			String loadMessage = loadTask.get();

			removeFlatArrayIfIntermediate(arrays);
			long endTimeMigration = System.currentTimeMillis();
			String message = "csv migration from PostgreSQL to SciDB execution time: "
					+ (endTimeMigration - startTimeMigration);
			log.info(message);
			return new MigrationResult(extractedRowsCount, null,
					loadMessage + "No information about number of loaded rows.",
					false);
		} catch (SQLException | UnsupportedTypeException | ExecutionException
				| InterruptedException | MigrationException | IOException
				| RunShellException exception) {
			MigrationException migrationException = handleException(exception,
					"Migration in CSV format failed. ");
			throw migrationException;
		} finally {
			cleanResources();
		}

	}

	/**
	 * Clean resources of this instance of the migrator at the end of migration.
	 * 
	 * @throws MigrationException
	 */
	private void cleanResources() throws MigrationException {
		if (postgresPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(postgresPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: "
						+ postgresPipe + " " + e.getMessage());
			}
		}
		if (scidbPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(scidbPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: "
						+ scidbPipe + " " + e.getMessage());
			}
		}
		if (executor != null && !executor.isShutdown()) {
			executor.shutdownNow();
		}
		if (connectionPostgres != null) {
			try {
				connectionPostgres.close();
			} catch (SQLException e) {
				e.printStackTrace();
				String msg = "Could not close connection to PostgreSQL!"
						+ e.getMessage() + " " + generalMessage;
				log.error(msg);
				throw new MigrationException(msg);
			}
		}
	}

	/**
	 * Handler the exception for the migration.
	 * 
	 * @param exception
	 *            the exception that was raised during migration
	 * @return the MigrationException
	 */
	private MigrationException handleException(Exception exception,
			String message) {
		/* this log with stack trace is for UnsupportedTypeException */
		log.error(StackTrace.getFullStackTrace(exception));
		String msg = message + errMessage + exception.getMessage()
				+ " PostgreSQL connection: " + connectionFrom.toString()
				+ " fromTable: " + fromTable + " SciDBConnection: "
				+ connectionTo.toString() + " to array:" + toArray;
		log.error(msg);
		/* try to clean the environment: remove the created arrays */
		for (String array : createdArrays) {
			try {
				removeArray(array);
			} catch (SQLException e1) {
				e1.printStackTrace();
				msg = "Could not remove intermediate arrays from SciDB! " + msg;
				log.error(msg);
				return new MigrationException(msg);
			}
		}
		return new MigrationException(msg);
	}

	/**
	 * Remove the intermediate flat array if it was created. If the
	 * multi-dimensional array was the target one, then the intermediate array
	 * should be deleted (the array was created in this migration process).
	 * 
	 * @param connectionTo
	 *            connection to SciDB
	 * @param arrays
	 *            the information about arrays: flat and multi-dimensional
	 * @throws SQLException
	 */
	private void removeFlatArrayIfIntermediate(SciDBArrays arrays)
			throws SQLException {
		if (arrays.getMultiDimensional() != null) {
			removeArray(arrays.getFlat());
		}
	}

	/**
	 * Remove the given array from SciDB.
	 * 
	 * @param connectionTo
	 * @param arrayName
	 * @throws SQLException
	 */
	private void removeArray(String arrayName) throws SQLException {
		SciDBHandler handler = new SciDBHandler(connectionTo);
		handler.executeStatement("drop array " + arrayName);
		handler.close();
	}

	/**
	 * example types=int32_t,int32_t null,double,double null,string,string null
	 * 
	 * @return scidb bin format for the transformation
	 * 
	 * 
	 * @throws UnsupportedTypeException
	 */
	private String getSciDBBinFormat() throws UnsupportedTypeException {
		StringBuilder binFormatBuffer = new StringBuilder();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String postgresColumnType = postgresColumnMetaData.getDataType();
			String attributeType = DataTypesFromPostgreSQLToSciDB
					.getSciDBTypeFromPostgreSQLType(postgresColumnType);
			String attributeNULL = "";
			if (postgresColumnMetaData.isNullable()) {
				attributeNULL = " null";
			}
			binFormatBuffer.append(attributeType + attributeNULL + ",");
		}
		/* delete the last comma "," */
		binFormatBuffer.deleteCharAt(binFormatBuffer.length() - 1);
		return binFormatBuffer.toString();
	}

	/**
	 * Create a flat array in SciDB from the meta info about the table in
	 * PostgreSQL.
	 * 
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 * @throws MigrationException
	 */
	private void createFlatArray(String arrayName)
			throws SQLException, UnsupportedTypeException, MigrationException {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + arrayName + " <");
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String attributeName = postgresColumnMetaData.getName();
			String postgresColumnType = postgresColumnMetaData.getDataType();
			String attributeType = DataTypesFromPostgreSQLToSciDB
					.getSciDBTypeFromPostgreSQLType(postgresColumnType);
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
		createArrayStringBuf.append("> [i=0:*,1000000,0]");
		SciDBHandler handler = new SciDBHandler(connectionTo);
		handler.executeStatement(createArrayStringBuf.toString());
		handler.commit();
		handler.close();
		createdArrays.add(arrayName);
	}

	/**
	 * Prepare flat and target arrays in SciDB to load the data.
	 * 
	 * @throws SQLException
	 * @throws MigrationException
	 * @throws UnsupportedTypeException
	 * 
	 */
	private SciDBArrays prepareFlatTargetArrays()
			throws MigrationException, SQLException, UnsupportedTypeException {
		SciDBHandler handler = new SciDBHandler(connectionTo);
		SciDBArrayMetaData arrayMetaData = null;
		try {
			arrayMetaData = handler.getArrayMetaData(toArray);
		} catch (NoTargetArrayException e) {
			/*
			 * When only a name of array in SciDB was given, but the array does
			 * not exist in SciDB then we have to create the target array which
			 * by default is flat.
			 */
			createFlatArray(toArray);
			/* the data should be loaded to the default flat array */
			return new SciDBArrays(toArray, null);
		}
		handler.close();
		if (PostgreSQLSciDBMigrationUtils.isFlatArray(arrayMetaData,
				postgresqlTableMetaData)) {
			return new SciDBArrays(toArray, null);
		}
		/*
		 * the target array is multidimensional so we have to build the
		 * intermediate flat array
		 */
		/*
		 * check if every column from Postgres is mapped to a column/attribute
		 * in SciDB's arrays
		 */
		Map<String, SciDBColumnMetaData> dimensionsMap = arrayMetaData
				.getDimensionsMap();
		Map<String, SciDBColumnMetaData> attributesMap = arrayMetaData
				.getAttributesMap();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData
				.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String postgresColumnName = postgresColumnMetaData.getName();
			if (!dimensionsMap.containsKey(postgresColumnName)
					&& !attributesMap.containsKey(postgresColumnName)) {
				throw new MigrationException(
						"The attribute " + postgresColumnName
								+ " from PostgreSQL's table: " + fromTable
								+ " is not matched with any attribute/dimension in the array in SciDB: "
								+ toArray);
			}
		}
		String newFlatIntermediateArray = toArray + "__flat__";
		createFlatArray(newFlatIntermediateArray);
		return new SciDBArrays(newFlatIntermediateArray, toArray);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.MigrationImplementation#migrate()
	 */
	@Override
	public MigrationResult migrate() throws MigrationException {
		return migrateBin();
	}

	/**
	 * @return the connectionFrom
	 */
	public PostgreSQLConnectionInfo getConnectionFrom() {
		return connectionFrom;
	}

	/**
	 * @return the fromTable
	 */
	public String getFromTable() {
		return fromTable;
	}

	/**
	 * @return the connectionTo
	 */
	public SciDBConnectionInfo getConnectionTo() {
		return connectionTo;
	}

	/**
	 * @return the toArray
	 */
	public String getToArray() {
		return toArray;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FromPostgresToSciDBImplementation [connectionFrom="
				+ connectionFrom + ", fromTable=" + fromTable
				+ ", connectionTo=" + connectionTo + ", toArray=" + toArray
				+ "]";
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
				+ ((connectionFrom == null) ? 0 : connectionFrom.hashCode());
		result = prime * result
				+ ((connectionTo == null) ? 0 : connectionTo.hashCode());
		result = prime * result
				+ ((fromTable == null) ? 0 : fromTable.hashCode());
		result = prime * result + ((toArray == null) ? 0 : toArray.hashCode());
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
		FromPostgresToSciDBImplementation other = (FromPostgresToSciDBImplementation) obj;
		if (connectionFrom == null) {
			if (other.connectionFrom != null)
				return false;
		} else if (!connectionFrom.equals(other.connectionFrom))
			return false;
		if (connectionTo == null) {
			if (other.connectionTo != null)
				return false;
		} else if (!connectionTo.equals(other.connectionTo))
			return false;
		if (fromTable == null) {
			if (other.fromTable != null)
				return false;
		} else if (!fromTable.equals(other.fromTable))
			return false;
		if (toArray == null) {
			if (other.toArray != null)
				return false;
		} else if (!toArray.equals(other.toArray))
			return false;
		return true;
	}

}
