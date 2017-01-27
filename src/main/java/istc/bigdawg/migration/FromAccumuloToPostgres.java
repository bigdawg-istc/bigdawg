/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.accumulo.AccumuloMigrationParams;

/**
 * @author Adam Dziedzic
 * 8:37:41 PM
 *
 */

import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.utils.ListConncatenator;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromAccumuloToPostgres extends FromDatabaseToDatabase {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3329851490314835580L;

	private static Logger logger = Logger
			.getLogger(FromAccumuloToPostgres.class);

	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private AccumuloInstance accInst = null;

	// parameters
	private int postgreSQLWritebatchSize = 1000;
	private int postgreSQLReaderCharSize = 1000000;
	private char delimiter = '|';

	public FromAccumuloToPostgres() {

	}

	public FromAccumuloToPostgres(AccumuloInstance accInst, Connection con) {
		this.accInst = accInst;
		this.con = con;
	}

	private void cleanPostgreSQLResources() throws SQLException {
		if (rs != null) {
			rs.close();
		}
		if (st != null) {
			st.close();
		}
		if (con != null) {
			con.close();
		}
	}

	/**
	 * Extract table's meta data. This has to be in this class as after reading
	 * the result set meta data, we have to clean the PostgreSQL's resources.
	 * 
	 * @param tableName
	 *            The table for which we want the meta data.
	 * @param con
	 *            Connection to PostgreSQL.
	 * @return result set meta data
	 * @throws SQLException
	 */
	private ResultSetMetaData getMetaData(final String tableName)
			throws SQLException {
		/* We have to get to the meta data after querying the table. */
		String query = "Select * from "
				+ tableName.replace(";", "").replace(" ", "") + " limit 1";
		logger.debug("Query to get meta data: " + query);
		st = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		rs = st.executeQuery();
		if (rs == null) {
			return null;
		}
		return rs.getMetaData();
	}

	public void createNewRowForPostgres(String[] row, StringBuilder sBuilder) {
		// we finished a new row;
		String rowString = ListConncatenator.joinList(row, delimiter, "\n");
		// System.out.println(rowString);
		sBuilder.append(rowString);
	}

	public void flushRowsToPostgreSQL(StringBuilder sBuilder,
			PushbackReader reader, CopyManager cpManager, String postgresTable,
			String copyString) throws IOException, SQLException {
		// System.out.println(sBuilder.toString());
		reader.unread(sBuilder.toString().toCharArray());
		cpManager.copyIn(copyString.toString(), reader);
		sBuilder.delete(0, sBuilder.length());
	}

	@Override
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		if (!(migrationInfo
				.getConnectionFrom() instanceof AccumuloConnectionInfo
				&& migrationInfo
						.getConnectionTo() instanceof PostgreSQLConnectionInfo)) {
			return null;
		}
		AccumuloConnectionInfo conFrom = (AccumuloConnectionInfo) migrationInfo
				.getConnectionFrom();
		PostgreSQLConnectionInfo conTo = (PostgreSQLConnectionInfo) migrationInfo
				.getConnectionTo();
		logger.debug(conTo);
		try {
			this.con = PostgreSQLHandler.getConnection(conTo);
			con.setAutoCommit(false);
			con.setReadOnly(false);
			logger.debug("Check if there exists create table statement for "
					+ "PostgreSQL, if so create it.");
			String createStatement = MigrationUtils
					.getUserCreateStatement(migrationInfo);
			if (createStatement != null) {
				logger.debug(
						"The create table statement to be executed in PostgreSQL: "
								+ createStatement);
				PostgreSQLHandler.createTargetTableSchema(con,
						migrationInfo.getObjectTo(), createStatement);
			}
		} catch (SQLException e) {
			String msg = "Could not connect to PostgreSQL.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
		}

		try {
			this.accInst = AccumuloInstance.getFullInstance(conFrom);
		} catch (AccumuloSecurityException | AccumuloException e) {
			throw new MigrationException("Problem with Accumulo", e);
		}

		/* Extract range for table scanning in Accumulo. */
		Range range = null;
		MigrationParams params = migrationInfo.getMigrationParams()
				.orElse(null);
		if (params != null) {
			if (params instanceof AccumuloMigrationParams) {
				AccumuloMigrationParams accParams = (AccumuloMigrationParams) params;
				range = accParams.getSourceTableRange();
			}
		}

		try {
			return fromAccumuloToPostgres(migrationInfo.getObjectFrom(),
					migrationInfo.getObjectTo(), range);
		} catch (AccumuloException | AccumuloSecurityException
				| AccumuloBigDawgException | TableNotFoundException
				| SQLException | IOException e) {
			String msg = e.getMessage();
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		}
	}

	public MigrationResult fromAccumuloToPostgres(final String accumuloTable,
			final String postgresTable, Range accumuloRange)
					throws AccumuloException, AccumuloSecurityException,
					AccumuloBigDawgException, SQLException,
					TableNotFoundException, IOException, MigrationException {
		logger.debug("Migrate data from Accumulo to Postgres.");
		long startTimeMigration = System.currentTimeMillis();
		/*
		 * count rows in sense of PostgreSQL one PostgreSQL row == many rows
		 * combined from Accumulo)
		 */
		long accumuloCounter = 0; /* Number of rows/tuples from Accumulo. */
		long postgresCounter = 0; /* Number of rows/tuples for PostgreSQL. */
		StringBuilder copyStringBuf = new StringBuilder();
		copyStringBuf.append("COPY ");
		copyStringBuf.append(postgresTable);
		copyStringBuf.append(" FROM STDIN WITH (DELIMITER '");
		copyStringBuf.append(delimiter);
		copyStringBuf.append("')");
		String copyString = copyStringBuf.toString();
		try {
			ResultSetMetaData rsmd = getMetaData(postgresTable);
			if (rsmd == null) {
				String message = "There is no table: " + postgresTable;
				logger.log(Level.INFO, message);
			} else {
				int numOfCol = rsmd.getColumnCount();
				Map<String, Integer> mapNameCol = new HashMap<>();
				for (int i = 0; i < numOfCol; ++i) {
					String columnName = rsmd.getColumnName(i + 1);
					logger.debug("Column name: " + columnName);
					mapNameCol.put(columnName, i);
				}
				StringBuilder sBuilder = new StringBuilder();
				CopyManager cpManager = new CopyManager(
						(BaseConnection) this.con); // ((PGConnection)
				// con).getCopyAPI();
				PushbackReader reader = new PushbackReader(new StringReader(""),
						postgreSQLReaderCharSize);
				// create a new row
				String[] row = new String[numOfCol];
				Text rowId = null;
				Scanner scan = accInst.getConn().createScanner(accumuloTable,
						new Authorizations());
				if (accumuloRange != null) {
					scan.setRange(accumuloRange);
				}
				for (Entry<Key, Value> e : scan) {
					Text thisRowId = e.getKey().getRow();
					// System.out.println(thisRowId);
					// omit first initialization of rowId
					if (rowId != null && !rowId.equals(thisRowId)) {
						++postgresCounter;
						++accumuloCounter;
						createNewRowForPostgres(row, sBuilder);
						if (accumuloCounter % postgreSQLWritebatchSize == 0) {
							flushRowsToPostgreSQL(sBuilder, reader, cpManager,
									postgresTable, copyString);
						}
						row = new String[numOfCol];
					}
					rowId = thisRowId;
					Text colq = e.getKey().getColumnQualifier();
					String value = e.getValue().toString();
					/*
					 * Column number (index) in PostgreSQL starts from 1.
					 * 
					 * All identifiers (including column names) that are not
					 * double-quoted are folded to lower case in PostgreSQL, so
					 * we have to change the column qualifier from Accumulo
					 * (that denotes column name in PostgreSQL to lowercase.
					 */
					Integer colIndex = mapNameCol
							.get(colq.toString().toLowerCase());
					if (colIndex == null) {
						throw new MigrationException(
								"No such column in PostgreSQL: "
										+ colq.toString()
										+ ". Current columns in PostgreSQL table are: "
										+ mapNameCol.toString());
					}
					row[colIndex] = value;
				}
				if (rowId != null) {
					++postgresCounter;
					++accumuloCounter;
					createNewRowForPostgres(row, sBuilder);
					flushRowsToPostgreSQL(sBuilder, reader, cpManager,
							postgresTable, copyString);
				}
				con.commit();
			}
		} finally {
			cleanPostgreSQLResources();
		}

		long endTimeMigration = System.currentTimeMillis();
		long durationMsec = endTimeMigration - startTimeMigration;
		return new MigrationResult(accumuloCounter, postgresCounter,
				startTimeMigration, endTimeMigration, durationMsec);

	}

	public MigrationResult fromAccumuloToPostgres(final String accumuloTable,
			final String postgresTable)
					throws MigrationException, AccumuloException,
					AccumuloSecurityException, AccumuloBigDawgException,
					SQLException, TableNotFoundException, IOException {
		return fromAccumuloToPostgres(accumuloTable, postgresTable, null);
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws TableNotFoundException
	 * @throws AccumuloBigDawgException
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public static void main(String[] args)
			throws IOException, SQLException, AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException,
			TableNotFoundException, MigrationException {
		boolean all = true;
		System.out.print("Command line arguments: ");
		for (String s : args) {
			System.out.print(s + " ");
		}
		String mode = "";
		String scaleFactor = "";
		if (args.length > 1) {
			System.out.println();
			mode = args[0];
			System.out.println("Mode: " + mode);
		}
		if (args.length > 2) {
			scaleFactor = args[1];
			System.out.println("scaleFactor: " + scaleFactor);
		}
		FromAccumuloToPostgres fromAccumuloToPostgres = null;
		try {
			AccumuloInstance instance = AccumuloInstance.getInstance();
			Connection con = PostgreSQLInstance.getConnection();
			fromAccumuloToPostgres = new FromAccumuloToPostgres(instance, con);
		} catch (AccumuloException | AccumuloSecurityException
				| AccumuloBigDawgException e1) {
			e1.printStackTrace();
			return;
		}
		List<String> tables = new ArrayList<String>();
		tables.add("region");
		tables.add("nation");
		tables.add("customer");
		tables.add("part");
		tables.add("supplier");
		tables.add("partsupp");
		tables.add("orders");
		tables.add("lineitem");

		// Remove data from Postgres
		Connection con = PostgreSQLInstance.getConnection();
		con.setAutoCommit(true);
		for (String table : tables) {
			String deleteStatement = "delete from " + table;
			PreparedStatement st = con.prepareStatement(deleteStatement);
			st.execute();
			st.close();
		}

		PrintWriter fullWriter = new PrintWriter(new BufferedWriter(
				new FileWriter("fromAccumuloToPostgresFull.log", true)));
		String fullMessage = "";
		if (mode.equals("fromAccumuloToPostgres") || all) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter("fromAccumuloToPostgres.log", true)));
			long lStartTime = System.nanoTime();
			for (String table : tables) {
				// System.out.println("Table: " + table);
				fromAccumuloToPostgres.fromAccumuloToPostgres(table, table);
			}
			String message = "From Accumulo To Postgres execution time in seconds: "
					+ (System.nanoTime() - lStartTime) / 1000000000L
					+ " :scale factor: " + scaleFactor;
			fullMessage += message;
			System.out.println(message);
			writer.append(message + "\n");
			writer.close();
		}
		long allRowsNumber = 0;
		if (mode.equals("countRowsPostgres") || all) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter("countRowsPostgres.log", true)));
			con = PostgreSQLInstance.getConnection();
			con.setReadOnly(true);
			con.setAutoCommit(false);
			for (String table : tables) {
				String query = "select count(*) from " + table;
				PreparedStatement st = con.prepareStatement(query,
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY);
				ResultSet rs = st.executeQuery();
				String messageString = "";
				long rowNumber = 0;
				if (rs.next()) {
					rowNumber = rs.getLong(1);
				}
				st.close();
				rs.close();
				allRowsNumber += rowNumber;
				messageString = "PostgreSQL number of rows for table " + table
						+ " is: " + rowNumber;
				System.out.println(messageString);
				writer.append(messageString + "\n");
			}
			con.close();
			writer.close();
		}
		fullWriter.append(TestTpchPostgresAccumulo.getFullLog(
				fullMessage + ":Total number of rows:" + allRowsNumber + "\n"));
		fullWriter.close();
	}
}
