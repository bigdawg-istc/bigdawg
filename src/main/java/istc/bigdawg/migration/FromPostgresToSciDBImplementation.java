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
import istc.bigdawg.monitoring.Monitor;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Pipe;
import istc.bigdawg.utils.SessionIdentifierGenerator;
import istc.bigdawg.utils.StackTrace;

/**
 * Implementation of the migration from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 */
public class FromPostgresToSciDBImplementation implements MigrationImplementation {

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToSciDBImplementation.class);

	/* General message about the action in the class. */
	private static String generalMessage = "Data migration from PostgreSQL to SciDB";

	/* General error message when the migration fails in the class. */
	private static String errMessage = generalMessage + " failed! ";

	enum MigrationType {
		FULL /* export dimensions and attributes from SciDB */, FLAT
		/* export only the attributes from SciDB */}

	private MigrationType migrationType;
	/*
	 * These are the arrays that were created during migration of data from
	 * PostgreSQL to SciDB. If something fails on the way, then the arrays
	 * should be removed.
	 */
	private Set<String> createdArrays = new HashSet<>();

	/**
	 * These are intermediate arrays (flat) created for migration and should be
	 * always removed at the end of the migration process.
	 */
	private Set<String> intermediateArrays = new HashSet<>();

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
	public FromPostgresToSciDBImplementation(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			SciDBConnectionInfo connectionTo, String toArray) throws MigrationException {
		this.connectionFrom = connectionFrom;
		this.fromTable = fromTable;
		this.connectionTo = connectionTo;
		this.toArray = toArray;
		try {
			this.postgresqlTableMetaData = new PostgreSQLHandler(connectionFrom).getColumnsMetaData(fromTable);
		} catch (SQLException postgresException) {
			MigrationException migrateException = handleException(postgresException,
					"Extraction of meta data on the table: " + fromTable + " in PostgreSQL failed. ");
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
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_fromPostgres_" + fromTable);
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_toSciDB_" + toArray);

			SciDBArrays arrays = prepareFlatTargetArrays();
			executor = Executors.newFixedThreadPool(3/* 3 */);

			String copyFromCommand = PostgreSQLHandler.getExportBinCommand(fromTable);
			//String copyFromCommand = "copy from " + fromTable + " to " + postgresPipe + " with (format binary, freeze)";
			CopyFromPostgresExecutor exportExecutor = new CopyFromPostgresExecutor(connectionFrom, copyFromCommand,
					postgresPipe);
			FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
			executor.submit(exportTask);

			TransformBinExecutor transformExecutor = new TransformBinExecutor(postgresPipe, scidbPipe,
					getSciDBBinFormat(), TransformBinExecutor.TYPE.FromPostgresToSciDB);
			FutureTask<Long> transformTask = new FutureTask<Long>(transformExecutor);
			executor.submit(transformTask);

			LoadToSciDBExecutor loadExecutor = new LoadToSciDBExecutor(connectionTo, arrays, scidbPipe,
					getSciDBBinFormat());
			FutureTask<String> loadTask = new FutureTask<String>(loadExecutor);
			executor.submit(loadTask);

			long countExtractedElements = exportTask.get();
			long transformationResult = transformTask.get();
			String loadMessage = loadTask.get();

			String transformationMessage;
			if (transformationResult != 0) {
				String message = "Check the C++ migrator! It might need to be compiled and checked separately!";
				log.error(message);
				throw new MigrationException(message);
			} else {
				transformationMessage = "Transformation finished successfuly!";
			}

			/**
			 * the migration was successful so only clear the intermediate
			 * arrays
			 */
			PostgreSQLSciDBMigrationUtils.removeArrays(connectionTo, "clean the intermediate arrays",
					intermediateArrays);
			createdArrays.removeAll(intermediateArrays);

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationStatistics stats = new MigrationStatistics(connectionFrom, connectionTo, fromTable, toArray,
					startTimeMigration, endTimeMigration, null, countExtractedElements, this.getClass().getName());
			Monitor.addMigrationStats(stats);
			log.debug("Migration result,connectionFrom," + connectionFrom.toSimpleString() + ",connectionTo,"
					+ connectionTo.toString() + ",fromTable," + fromTable + ",toArray," + toArray
					+ ",startTimeMigration," + startTimeMigration + ",endTimeMigration," + endTimeMigration
					+ ",countExtractedElements," + countExtractedElements + ",countLoadedElements," + "N/A"
					+ ",durationMsec," + durationMsec + "," + Thread.currentThread().getStackTrace()[1].getMethodName()
					+ "," + migrationType.toString());
			return new MigrationResult(countExtractedElements, null,
					loadMessage + " No information about the number of loaded rows." + " Result of transformation: "
							+ transformationMessage,
					false);
		} catch (SQLException | UnsupportedTypeException | InterruptedException | ExecutionException | IOException
				| RunShellException exception) {
			MigrationException migrationException = handleException(exception, "Migration in binary Format failed. ");
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
			postgresPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_fromPostgres_" + fromTable);
			scidbPipe = Pipe.INSTANCE.createAndGetFullName(this.getClass().getName() + "_toSciDB_" + toArray);
			executor = Executors.newFixedThreadPool(3);

			CopyFromPostgresExecutor exportExecutor = new CopyFromPostgresExecutor(connectionFrom,
					PostgreSQLHandler.getExportCsvCommand(fromTable, delimiter), postgresPipe);
			FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
			executor.submit(exportTask);

			String typesPattern = SciDBHandler.getTypePatternFromPostgresTypes(postgresqlTableMetaData);
			TransformFromCsvToSciDBExecutor csvSciDBExecutor = new TransformFromCsvToSciDBExecutor(typesPattern,
					postgresPipe, delimiter, scidbPipe, connectionTo.getBinPath());
			FutureTask<Integer> csvSciDBTask = new FutureTask<Integer>(csvSciDBExecutor);
			executor.submit(csvSciDBTask);

			SciDBArrays arrays = prepareFlatTargetArrays();
			LoadToSciDBExecutor loadExecutor = new LoadToSciDBExecutor(connectionTo, arrays, scidbPipe);
			FutureTask<String> loadTask = new FutureTask<String>(loadExecutor);
			executor.submit(loadTask);

			long countExtractedElements = exportTask.get();
			csvSciDBTask.get();
			String loadMessage = loadTask.get();

			/**
			 * the migration was successful so only clear the intermediate
			 * arrays
			 */
			PostgreSQLSciDBMigrationUtils.removeArrays(connectionTo, "clean the intermediate arrays",
					intermediateArrays);
			createdArrays.removeAll(intermediateArrays);

			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			MigrationStatistics stats = new MigrationStatistics(connectionFrom, connectionTo, fromTable, toArray,
					startTimeMigration, endTimeMigration, null, countExtractedElements, this.getClass().getName());
			Monitor.addMigrationStats(stats);
			log.debug("Migration result,connectionFrom," + connectionFrom.toSimpleString() + ",connectionTo,"
					+ connectionTo.toString() + ",fromTable," + fromTable + ",toArray," + toArray
					+ ",startTimeMigration," + startTimeMigration + ",endTimeMigration," + endTimeMigration
					+ ",countExtractedElements," + countExtractedElements + ",countLoadedElements," + "N/A"
					+ ",durationMsec," + durationMsec + "," + Thread.currentThread().getStackTrace()[1].getMethodName()
					+ "," + migrationType.toString());
			return new MigrationResult(countExtractedElements, null,
					loadMessage + " No information about number of loaded rows.", false);
		} catch (SQLException | UnsupportedTypeException | ExecutionException | InterruptedException
				| MigrationException | IOException | RunShellException exception) {
			MigrationException migrationException = handleException(exception, "Migration in CSV format failed. ");
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
				throw new MigrationException("Could not remove pipe: " + postgresPipe + " " + e.getMessage());
			}
		}
		if (scidbPipe != null) {
			try {
				Pipe.INSTANCE.deletePipeIfExists(scidbPipe);
			} catch (IOException e) {
				throw new MigrationException("Could not remove pipe: " + scidbPipe + " " + e.getMessage());
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
				String msg = "Could not close connection to PostgreSQL!" + e.getMessage() + " " + generalMessage;
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
	private MigrationException handleException(Exception exception, String message) {
		/* this log with stack trace is for UnsupportedTypeException */
		log.error(StackTrace.getFullStackTrace(exception));
		String msg = message + errMessage + exception.getMessage() + " PostgreSQL connection: "
				+ connectionFrom.toString() + " fromTable: " + fromTable + " SciDBConnection: "
				+ connectionTo.toString() + " to array:" + toArray;
		log.error(msg);
		try {
			/**
			 * there was an exception so the migration failed and the created
			 * arrays should be removed
			 */
			PostgreSQLSciDBMigrationUtils.removeArrays(connectionTo, msg, createdArrays);
		} catch (MigrationException ex) {
			return ex;
		}
		return new MigrationException(msg);
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
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String postgresColumnType = postgresColumnMetaData.getDataType();
			String attributeType = DataTypesFromPostgreSQLToSciDB.getSciDBTypeFromPostgreSQLType(postgresColumnType);
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
	private void createFlatArray(String arrayName) throws SQLException, UnsupportedTypeException, MigrationException {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + arrayName + " <");
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String attributeName = postgresColumnMetaData.getName();
			String postgresColumnType = postgresColumnMetaData.getDataType();
			String attributeType = DataTypesFromPostgreSQLToSciDB.getSciDBTypeFromPostgreSQLType(postgresColumnType);
			String attributeNULL = "";
			if (postgresColumnMetaData.isNullable()) {
				attributeNULL = " NULL";
			}
			createArrayStringBuf.append(attributeName + ":" + attributeType + attributeNULL + ",");
		}
		/* delete the last comma "," */
		createArrayStringBuf.deleteCharAt(createArrayStringBuf.length() - 1);
		/* " r_regionkey:int64,r_name:string,r_comment:string> );" */
		/* this is by default 1 mln cells in a chunk */
		createArrayStringBuf.append("> [_flat_dimension_=0:*,1000000,0]");
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
	private SciDBArrays prepareFlatTargetArrays() throws MigrationException, SQLException, UnsupportedTypeException {
		SciDBHandler handler = new SciDBHandler(connectionTo);
		SciDBArrayMetaData arrayMetaData = null;
		migrationType = MigrationType.FULL;
		try {
			arrayMetaData = handler.getArrayMetaData(toArray);
		} catch (NoTargetArrayException e) {
			/*
			 * When only a name of array in SciDB was given, but the array does
			 * not exist in SciDB then we have to create the target array which
			 * by default is flat.
			 */
			createFlatArray(toArray);
			migrationType = MigrationType.FLAT;
			/* the data should be loaded to the default flat array */
			return new SciDBArrays(toArray, null);
		}
		handler.close();
		if (PostgreSQLSciDBMigrationUtils.isFlatArray(arrayMetaData, postgresqlTableMetaData)) {
			migrationType = MigrationType.FLAT;
			return new SciDBArrays(toArray, null);
		}
		/*
		 * the target array is multidimensional so we have to build the
		 * intermediate flat array
		 */
		/*
		 * check if every column from Postgres is mapped to a column/attribute
		 * in SciDB's arrays (the attributes from the flat array can change to
		 * dimesions in the mulit dimensional array, thus we cannot verify the
		 * match of columns in postgres and dimensions/attributes in scidb)
		 */
		Map<String, SciDBColumnMetaData> dimensionsMap = arrayMetaData.getDimensionsMap();
		Map<String, SciDBColumnMetaData> attributesMap = arrayMetaData.getAttributesMap();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresqlTableMetaData.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String postgresColumnName = postgresColumnMetaData.getName();
			if (!dimensionsMap.containsKey(postgresColumnName) && !attributesMap.containsKey(postgresColumnName)) {
				throw new MigrationException("The attribute " + postgresColumnName + " from PostgreSQL's table: "
						+ fromTable + " is not matched with any attribute/dimension in the array in SciDB: " + toArray);
			}
		}
		String newFlatIntermediateArrayName = toArray + "__bigdawg__flat__"
				+ SessionIdentifierGenerator.INSTANCE.nextRandom26CharString();
		createFlatArray(newFlatIntermediateArrayName);
		return new SciDBArrays(newFlatIntermediateArrayName, toArray);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.migration.MigrationImplementation#migrate()
	 */
	@Override
	public MigrationResult migrate() throws MigrationException {
		/*
		 * the CSV migration is used for debugging and development, if you want
		 * to go much faster then change it to migrateBin() but then the C++
		 * migrator has to be compiled on each machine where bigdawg is running
		 */
		return migrateSingleThreadCSV();
	}

}