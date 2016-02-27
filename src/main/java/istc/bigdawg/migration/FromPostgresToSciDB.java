/**
 * 
 */
package istc.bigdawg.migration;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.NoTargetArrayException;
import istc.bigdawg.exceptions.UnsupportedTypeException;
import istc.bigdawg.migration.datatypes.DataTypesFromPostgreSQLToSciDB;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.StackTrace;
import istc.bigdawg.utils.SystemUtilities;

/**
 * Migrate data from PostgreSQL to SciDB.
 * 
 * @author Adam Dziedzic
 *
 */
public class FromPostgresToSciDB implements FromDatabaseToDatabase {

	/* log */
	private static Logger log = Logger.getLogger(FromPostgresToSciDB.class);

	/* General message about the action in the class. */
	private String generalMessage = "Data migration from PostgreSQL to SciDB";

	/* General error message when the migration fails in the class. */
	private String errMessage = generalMessage + " failed! ";

	/*
	 * These are the arrays that were created during migration of data from
	 * PostgreSQL to SciDB. If something fails on the way, then the arrays
	 * should be removed.
	 */
	private Set<String> createdArrays = new HashSet<>();

	/**
	 * Get the meta data about table in PostgreSQL.
	 * 
	 * @param connectionFrom
	 *            the connection to PostgreSQL from which we migrate the data
	 * @param fromTable
	 *            the table from which we migrate the data
	 * @return the meta data about the table in PostgreSQL
	 * @throws MigrationException
	 *             thrown when Extraction of the attribute types from PostgreSQL
	 *             failed
	 */
	private PostgreSQLTableMetaData getPostgreSQLTableMetaData(PostgreSQLConnectionInfo connectionFrom,
			String fromTable) throws MigrationException {
		PostgreSQLTableMetaData postgresTableMetaData;
		try {
			postgresTableMetaData = new PostgreSQLHandler(connectionFrom).getColumnsMetaData(fromTable);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new MigrationException(
					errMessage + " Extraction of the attribute types from PostgreSQL failed. " + e.getMessage());
		}
		return postgresTableMetaData;
	}

	/**
	 * This is migration from PostgreSQL to SciDB based on CSV format and
	 * carried out in a single thread.
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
	 * 
	 * @return MigrationRestult information about the executed migration
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public MigrationResult migrateSingleThreadCSV(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			SciDBConnectionInfo connectionTo, String toArray) throws MigrationException {
		log.info(generalMessage + " Mode: migrateSingleThreadCSV");
		String csvFilePath = SystemUtilities.getSystemTempDir() + "/bigdawg_" + fromTable + ".csv";
		String delimiter = "|";
		String scidbFilePath = SystemUtilities.getSystemTempDir() + "/bigdawg_" + fromTable + ".scidb";
		ExecutorService executor = null;
		Connection connectionPostgres = null;
		try {

			executor = Executors.newSingleThreadExecutor();

			FileOutputStream output = new FileOutputStream(csvFilePath);
			connectionPostgres = PostgreSQLHandler.getConnection(connectionFrom);
			connectionPostgres.setAutoCommit(false);
			CopyFromPostgresExecutor exportExecutor = new CopyFromPostgresExecutor(connectionPostgres,
					PostgreSQLHandler.getExportCsvCommand(fromTable, delimiter), output);
			FutureTask<Long> exportTask = new FutureTask<Long>(exportExecutor);
			executor.submit(exportTask);
			long extractedRowsCount = exportTask.get();

			PostgreSQLTableMetaData postgresTableMetaData = getPostgreSQLTableMetaData(connectionFrom, fromTable);
			List<PostgreSQLColumnMetaData> columnsMetaData = postgresTableMetaData.getColumnsOrdered();
			String typesPattern = SciDBHandler.getTypePatternFromPostgresTypes(columnsMetaData);
			TransformFromCsvToSciDBExecutor csvSciDBExecutor = new TransformFromCsvToSciDBExecutor(typesPattern,
					csvFilePath, delimiter, scidbFilePath, connectionTo.getBinPath());
			FutureTask<Integer> csvSciDBTask = new FutureTask<Integer>(csvSciDBExecutor);
			executor.submit(csvSciDBTask);
			/* wait for the transformation */
			csvSciDBTask.get();

			// save the disk space
			SystemUtilities.deleteFileIfExists(csvFilePath);

			SciDBArrays arrays = prepareFlatTargetArrays(connectionTo, toArray, fromTable, postgresTableMetaData);
			LoadToSciDBExecutor loadExecutor = new LoadToSciDBExecutor(connectionTo, arrays, scidbFilePath);
			FutureTask<String> loadTask = new FutureTask<String>(loadExecutor);
			executor.submit(loadTask);

			String loadMessage = loadTask.get();

			removeFlatArray(connectionTo, arrays);
			return new MigrationResult(extractedRowsCount, null,
					loadMessage + "No information about number of loaded rows.", false);
		} catch (SQLException | UnsupportedTypeException | ExecutionException | InterruptedException
				| FileNotFoundException | MigrationException e) {
			/* this log with stack trace is for UnsupportedTypeException */
			log.error(StackTrace.getFullStackTrace(e));
			String msg = errMessage + " Final data loading from PostgreSQL to SciDB failed! " + e.getMessage()
					+ " PostgreSQL connection: " + connectionFrom.toString() + " fromTable: " + fromTable
					+ " SciDBConnection: " + connectionTo.toString() + " to array:" + toArray;
			log.error(msg);
			/* try to clean the environment: remove the created arrays */
			for (String array : createdArrays) {
				try {
					removeArray(connectionTo, array);
				} catch (SQLException e1) {
					e1.printStackTrace();
					msg = "Could not remove intermediate arrays from SciDB! " + msg;
					log.error(msg);
					throw new MigrationException(msg);
				}
			}
			throw new MigrationException(msg);
		} finally {
			SystemUtilities.deleteFileIfExists(csvFilePath);
			SystemUtilities.deleteFileIfExists(scidbFilePath);
			if (executor != null && !executor.isShutdown()) {
				executor.shutdownNow();
			}
			if (connectionPostgres != null) {
				try {
					connectionPostgres.close();
				} catch (SQLException e) {
					e.printStackTrace();
					String msg = "Could not close connection to PostgreSQL!" + e.getMessage();
					log.error(msg);
					throw new MigrationException(msg);
				}
			}
		}

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
	private void removeFlatArray(SciDBConnectionInfo connectionTo, SciDBArrays arrays) throws SQLException {
		if (arrays.getMultiDimensional() != null) {
			removeArray(connectionTo, arrays.getFlat());
		}
	}

	/**
	 * Remove the given array from SciDB.
	 * 
	 * @param connectionTo
	 * @param arrayName
	 * @throws SQLException
	 */
	private void removeArray(SciDBConnectionInfo connectionTo, String arrayName) throws SQLException {
		SciDBHandler handler = new SciDBHandler(connectionTo);
		handler.executeStatement("drop array " + arrayName);
		handler.close();
	}

	/**
	 * Check if it is a flat array in SciDB. It also verifies if the mapping
	 * from the SciDB
	 * 
	 * @param postgresTableMetaData
	 * @param scidbArrayMetaData
	 * @param fromTable
	 * @param toArray
	 * @return
	 * @throws MigrationException
	 */
	private boolean isFlatArray(PostgreSQLTableMetaData postgresTableMetaData, SciDBArrayMetaData scidbArrayMetaData,
			String fromTable, String toArray) throws MigrationException {
		List<SciDBColumnMetaData> scidbAttributesOrdered = scidbArrayMetaData.getAttributesOrdered();
		List<SciDBColumnMetaData> scidbDimensionsOrdered = scidbArrayMetaData.getDimensionsOrdered();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresTableMetaData.getColumnsOrdered();
		// check if this is the flat array only
		if (scidbDimensionsOrdered.size() != 1) {
			return false;
		}
		if (scidbAttributesOrdered.size() == postgresColumnsOrdered.size()) {
			/*
			 * check if the flat array attributes are at the same order as
			 * columns in PostgreSQL
			 */
			for (int i = 0; i < scidbAttributesOrdered.size(); ++i) {
				if (!scidbAttributesOrdered.get(i).getColumnName().equals(postgresColumnsOrdered.get(i).getName())) {
					String msg = "The attribute " + postgresColumnsOrdered.get(i).getName()
							+ " from PostgreSQL's table: " + fromTable
							+ " is not matched in the same ORDER with attribute/dimension in the array in SciDB: "
							+ toArray + " (position " + i + " PostgreSQL is for the attribute "
							+ postgresColumnsOrdered.get(i).getName() + " whereas the position " + i
							+ " in the array in SciDB is: " + scidbAttributesOrdered.get(i).getColumnName() + ").";
					log.error(msg);
					throw new MigrationException(msg);
				}
			}
			return true;
		}
		return false;

	}

	/**
	 * When only a name of array in SciDB was given, but the array does not
	 * exist in SciDB then we have to create the target array which be default
	 * is flat.
	 * 
	 * @param connectionTo
	 * @param toArray
	 * @param postgresTableMetaData
	 * @throws SQLException
	 * @throws UnsupportedTypeException
	 */
	private void buildTargetDefaultFlatArray(SciDBConnectionInfo connectionTo, String toArray,
			PostgreSQLTableMetaData postgresTableMetaData) throws SQLException, UnsupportedTypeException {
		StringBuilder createArrayStringBuf = new StringBuilder();
		createArrayStringBuf.append("create array " + toArray + " <");
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresTableMetaData.getColumnsOrdered();
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
		createArrayStringBuf.append("> [i=0:*,1000000,0]");
		SciDBHandler handler = new SciDBHandler(connectionTo);
		handler.executeStatement(createArrayStringBuf.toString());
		handler.commit();
		handler.close();
		createdArrays.add(toArray);
	}

	/**
	 * Prepare flat and target arrays in SciDB to load the data.
	 * 
	 * @param connectionInfo
	 *            connection info about SciDB
	 * @param toArray
	 *            the array in SciDB where we want to load the data
	 * @throws SQLException
	 * @throws MigrationException
	 * @throws UnsupportedTypeException
	 * 
	 */
	private SciDBArrays prepareFlatTargetArrays(SciDBConnectionInfo connectionTo, String toArray, String fromTable,
			PostgreSQLTableMetaData postgresTableMetaData)
					throws MigrationException, SQLException, UnsupportedTypeException {
		SciDBHandler handler = new SciDBHandler(connectionTo);
		SciDBArrayMetaData arrayMetaData = null;
		try {
			arrayMetaData = handler.getArrayMetaData(toArray);
		} catch (NoTargetArrayException e) {
			buildTargetDefaultFlatArray(connectionTo, toArray, postgresTableMetaData);
			/* the data should be loaded to the default flat array */
			return new SciDBArrays(toArray, null);
		}
		handler.close();
		if (isFlatArray(postgresTableMetaData, arrayMetaData, fromTable, toArray)) {
			return new SciDBArrays(toArray, null);
		}
		/*
		 * check if every column from Postgres is mapped to a column/attribute
		 * in SciDB's arrays
		 */
		Map<String, SciDBColumnMetaData> dimensionsMap = arrayMetaData.getDimensionsMap();
		Map<String, SciDBColumnMetaData> attributesMap = arrayMetaData.getAttributesMap();
		List<PostgreSQLColumnMetaData> postgresColumnsOrdered = postgresTableMetaData.getColumnsOrdered();
		for (PostgreSQLColumnMetaData postgresColumnMetaData : postgresColumnsOrdered) {
			String postgresColumnName = postgresColumnMetaData.getName();
			if (!dimensionsMap.containsKey(postgresColumnName) && !attributesMap.containsKey(postgresColumnName)) {
				throw new MigrationException("The attribute " + postgresColumnName + " from PostgreSQL's table: "
						+ fromTable + " is not matched with any attribute/dimension in the array in SciDB: " + toArray);
			}
		}
		String newFlatIntermediateArray = toArray + "__flat__";
		buildTargetDefaultFlatArray(connectionTo, newFlatIntermediateArray, postgresTableMetaData);
		return new SciDBArrays(newFlatIntermediateArray, toArray);
	}

	/**
	 * This is migration from PostgreSQL to SciDB.
	 * 
	 * @param connectionFrom
	 *            the connection to PostgreSQL
	 * @param fromTable
	 *            the name of the table in PostgreSQL to be migrated
	 * @param connectionTo
	 *            the connection to SciDB database
	 * @param arrayTo
	 *            the name of the array in SciDB
	 * 
	 * @see istc.bigdawg.migration.FromDatabaseToDatabase#migrate(istc.bigdawg.query.
	 *      ConnectionInfo, java.lang.String, istc.bigdawg.query.ConnectionInfo,
	 *      java.lang.String)
	 * 
	 * 
	 */
	@Override
	public MigrationResult migrate(ConnectionInfo connectionFrom, String fromTable, ConnectionInfo connectionTo,
			String toArray) throws MigrationException {
		log.debug("General data migration: " + this.getClass().getName());
		if (connectionFrom instanceof PostgreSQLConnectionInfo && connectionTo instanceof SciDBConnectionInfo) {
			try {
				return this.migrateSingleThreadCSV((PostgreSQLConnectionInfo) connectionFrom, fromTable,
						(SciDBConnectionInfo) connectionTo, toArray);
			} catch (MigrationException e) {
				throw new MigrationException(e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws MigrationException
	 */
	public static void main(String[] args) throws MigrationException, IOException {
		LoggerSetup.setLogging();
		FromPostgresToSciDB migrator = new FromPostgresToSciDB();
		PostgreSQLConnectionInfo conFrom = new PostgreSQLConnectionInfo("localhost", "5431", "tpch", "postgres",
				"test");
		String fromTable = "region";
		SciDBConnectionInfo conTo = new SciDBConnectionInfo("localhost", "1239", "scidb", "mypassw",
				"/opt/scidb/14.12/bin/");
		String toArray = "region2";
		// migrator.migrateSingleThreadCSV(conFrom, fromTable, conTo, arrayTo);
		migrator.migrate(conFrom, fromTable, conTo, toArray);
	}

}
