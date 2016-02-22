/**
 * 
 */
package istc.bigdawg.migration;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.RunShellException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.postgresql.PostgreSQLColumnMetaData;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLTableMetaData;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.scidb.SciDBArrayMetaData;
import istc.bigdawg.scidb.SciDBColumnMetaData;
import istc.bigdawg.scidb.SciDBConnectionInfo;
import istc.bigdawg.scidb.SciDBHandler;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.RunShell;
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
	private String generalMessage = "Data migration from Postgres to SciDB";

	/* General error message when the migration fails in the class. */
	private String errMessage = generalMessage + " failed! ";

	/**
	 * Command to copy data from a table in PostgreSQL.
	 * 
	 * @param table
	 *            the name of the table from which we extract data
	 * @param delimiter
	 *            the delimiter for the output CSV file
	 * 
	 * @return the command to extract data from a table in PostgreSQL
	 */
	private String getCopyCommandPostgreSQL(String table, String delimiter) {
		StringBuilder copyFromStringBuf = new StringBuilder();
		copyFromStringBuf.append("COPY ");
		copyFromStringBuf.append(table + " ");
		copyFromStringBuf.append("TO ");
		copyFromStringBuf.append(" STDOUT ");
		copyFromStringBuf.append("with (format csv, delimiter '" + delimiter + "')");
		return copyFromStringBuf.toString();
	}

	/**
	 * Extract data from PostgreSQL to a single CSV file.
	 * 
	 * @param fromTable
	 *            the name of the table to extract the data from
	 * @param connectionFrom
	 *            the information about the connection to PostgreSQL
	 * @return number of rows extracted from PostgreSQL
	 * 
	 * @throws MigrationException
	 */
	private Long extractDataFromPostgreSQLSingleThreadCsv(String fromTable, PostgreSQLConnectionInfo connectionFrom,
			String csvFilePath, String delimiter) throws MigrationException {
		String copyCommandPostgreSQL = getCopyCommandPostgreSQL(fromTable, delimiter);
		Connection conPostgreSQL;
		try {
			conPostgreSQL = PostgreSQLHandler.getConnection(connectionFrom);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new MigrationException(errMessage + "Could not connect to PostgreSQL! " + e.getMessage());
		}
		CopyManager copyManagerPostgreSQL;
		try {
			copyManagerPostgreSQL = new CopyManager((BaseConnection) conPostgreSQL);
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new MigrationException(errMessage + "PostgreSQL Copy Manager creation failed. " + e1.getMessage());
		}
		FileWriter writer = null;
		try {
			writer = new FileWriter(csvFilePath);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MigrationException(errMessage + " Problem with opening a file: " + csvFilePath + " for writing.");
		}
		Long extractedRowsCount;
		try {
			extractedRowsCount = copyManagerPostgreSQL.copyOut(copyCommandPostgreSQL, writer);
			log.debug(generalMessage + " extracted rows from PostgreSQL: " + extractedRowsCount);
		} catch (SQLException | IOException e) {
			e.printStackTrace();
			throw new MigrationException(
					errMessage + " PostgreSQL Copy Manager: extracting data from PostgreSQL failed. " + e.getMessage());
		}
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new MigrationException(
					errMessage + " Problem with closing the file: " + csvFilePath + " " + e.getMessage());
		}
		return extractedRowsCount;
	}

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
	 * Change the format of the file from CSV to the SciDB format.
	 * 
	 * @param postgresTableMetaData
	 *            the meta data about table in PostgreSQL
	 * @param csvFilePath
	 *            the path to csv file exported from PostgreSQL
	 * @param delimiter
	 *            the separator between fields in the csv file
	 * @param scidbFilePath
	 *            the path to the scidb file format to be loaded to SciDB
	 * 
	 * @throws MigrationException
	 *             thrown when conversion from csv to scidb format failed
	 */
	private void fromCsvToSciDB(PostgreSQLTableMetaData postgresTableMetaData, String csvFilePath, String delimiter,
			String scidbFilePath, SciDBConnectionInfo connectionTo) throws MigrationException {
		String typesPattern = SciDBHandler.getTypePatternFromPostgresTypes(postgresTableMetaData.getColumnsOrdered());
		ProcessBuilder csv2scidb = new ProcessBuilder(connectionTo.getBinPath() + "csv2scidb", "-i", csvFilePath, "-o",
				scidbFilePath, "-d", delimiter, "-p", typesPattern);
		log.debug(csv2scidb.command());
		try {
			RunShell.runShell(csv2scidb);
		} catch (RunShellException | InterruptedException | IOException e) {
			e.printStackTrace();
			throw new MigrationException(errMessage + " Conversion from csv to scidb format failed! " + e.getMessage());
		}
		// save the disk space
		SystemUtilities.deleteFileIfExists(csvFilePath);
	}

	/**
	 * This is migration from PostgreSQL to SciDB based on CSV format and
	 * carried out in a single thread.
	 * 
	 * @param connectionFrom
	 *            the connection to PostgreSQL
	 * @param fromTable
	 *            the name of the table in PostgreSQL to be migrated
	 * @param connectionTo
	 *            the connection to SciDB database
	 * @param toArray
	 *            the name of the array in SciDB
	 * 
	 * @return MigrationRestult information about the executed migration
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public MigrationResult migrateSingleThreadCSV(PostgreSQLConnectionInfo connectionFrom, String fromTable,
			SciDBConnectionInfo connectionTo, String toArray) throws MigrationException, SQLException {
		String csvFilePath = SystemUtilities.getSystemTempDir() + "/bigdawg_" + fromTable + ".csv";
		String delimiter = "|";
		String scidbFilePath = SystemUtilities.getSystemTempDir() + "/bigdawg_" + fromTable + ".scidb";
		try {
			log.info(generalMessage);
			long extractedRowsCount = extractDataFromPostgreSQLSingleThreadCsv(fromTable, connectionFrom, csvFilePath,
					delimiter);
			PostgreSQLTableMetaData postgresTableMetaData = getPostgreSQLTableMetaData(connectionFrom, fromTable);
			fromCsvToSciDB(postgresTableMetaData, csvFilePath, delimiter, scidbFilePath, connectionTo);
			SciDBHandler handler = new SciDBHandler(connectionTo);
			handler.setAutoCommit(false);
			prepareFlatTargetArrays(handler, toArray, fromTable, postgresTableMetaData);
			try {
				// we have to do it, why there is a lock on the table?
				handler.close();
				handler = new SciDBHandler(connectionTo);
				loadDataToSciDB(handler, toArray, scidbFilePath);
			} catch (SQLException e) {
				throw new MigrationException(errMessage + " Final data loading to SciDB failed! " + e.getMessage());
			}
			handler.close();
			return new MigrationResult(extractedRowsCount, null, "No information about loaded rows.", false);
		} finally {
			SystemUtilities.deleteFileIfExists(csvFilePath);
			SystemUtilities.deleteFileIfExists(scidbFilePath);
		}

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
					throw new MigrationException("The attribute " + postgresColumnsOrdered.get(i).getName()
							+ " from PostgreSQL's table: " + fromTable
							+ " is not matched in the same ORDER with attribute/dimension in the array in SciDB: "
							+ toArray + " (position " + i + " PostgreSQL is for the attribute "
							+ postgresColumnsOrdered.get(i).getName() + " wheras the position " + i
							+ " in the array in SciDB is: " + scidbAttributesOrdered.get(i).getColumnName() + ").");
				}
			}
			return true;
		}
		return false;

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
	 * 
	 */
	public void prepareFlatTargetArrays(SciDBHandler handler, String toArray, String fromTable,
			PostgreSQLTableMetaData postgresTableMetaData) throws MigrationException, SQLException {
		SciDBArrayMetaData arrayMetaData = handler.getArrayMetaData(toArray);
		if (isFlatArray(postgresTableMetaData, arrayMetaData, fromTable, toArray)) {
			return;
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
		/*
		 * we have to create a flat array and redimension it to the final result
		 */
		/*
		 * AFL% store(redimension(load(test_waveform_flat,'/home/adam/data/
		 * waveform_test.scidb'),test_waveform_),test_waveform_);
		 */

		/* remove the auxiliary flat array if the target was not flat */

	}

	private String loadDataToSciDB(SciDBHandler handler, String arrayTo, String dataFile)
			throws SQLException {
		// InputStream resultInStream =
		// RunShell.executeAQLcommandSciDB(conTo.getHost(), conTo.getPort(),
		// conTo.getBinPath(), "load " + arrayTo + " from '" + dataFile + "'");
		// String resultString = IOUtils.toString(resultInStream,
		// Constants.ENCODING);
		//	log.debug("Load data to SciDB: " + resultString);
		handler.executeStatement("load " + arrayTo + " from '" + dataFile + "'");
		handler.commit();
		return "Data successfuly loaded to SciDB";
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
			} catch (MigrationException | SQLException e) {
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

/*
 * 0 [main] INFO istc.bigdawg.LoggerSetup - Starting application. Logging was
 * configured! 94 [main] INFO istc.bigdawg.migration.FromPostgresToSciDB - Data
 * migration from Postgres to SciDB 110 [main] DEBUG
 * istc.bigdawg.migration.FromPostgresToSciDB - Data migration from Postgres to
 * SciDB extracted rows from PostgreSQL: 5 121 [main] DEBUG
 * istc.bigdawg.postgresql.PostgreSQLHandler - replace double quotes (
 * ") with signle quotes in the query to run it in PostgreSQL: SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema="
 * public" and table_name="region" order by ordinal_position 156 [main] DEBUG
 * istc.bigdawg.migration.FromPostgresToSciDB - [/opt/scidb/14.12/bin/csv2scidb,
 * -i, /tmp/bigdawg_region.csv, -o, /tmp/bigdawg_region.scidb, -d, |, -p, NSS]
 * 548 [main] INFO istc.bigdawg.utils.RunShell - command to be executed in
 * SciDB: load region from /tmp/bigdawg_region.scidb; on host: localhost port:
 * 1239 SciDB bin path: /opt/scidb/14.12/bin/ 673 [main] DEBUG
 * istc.bigdawg.migration.FromPostgresToSciDB - Load data to SciDB: Query was
 * executed successfully
 */
