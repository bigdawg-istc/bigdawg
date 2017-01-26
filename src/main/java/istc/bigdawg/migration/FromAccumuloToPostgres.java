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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;
import istc.bigdawg.accumulo.AccumuloInstance;

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

	private ResultSet getPostgreSQLResultSet(final String table)
			throws SQLException {
		String query = "Select * from "
				+ table.replace(";", "").replace(" ", "") + " limit 1";
		st = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		return st.executeQuery();
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
		try {
			this.con = PostgreSQLHandler.getConnection(conTo);
			con.setAutoCommit(false);
			con.setReadOnly(false);
		} catch (SQLException e) {
			String msg = "Could not connect to PostgreSQL.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
		}

		try {
			this.accInst = AccumuloInstance.getFullInstance(conFrom);
		} catch (AccumuloSecurityException | AccumuloException e) {
			throw new MigrationException("Problem with Accumulo", e);
		}
		try {
			return fromAccumuloToPostgres(migrationInfo.getObjectFrom(),
					migrationInfo.getObjectTo());
		} catch (AccumuloException | AccumuloSecurityException
				| AccumuloBigDawgException | SQLException
				| TableNotFoundException | IOException e) {
			String msg = "Could not close the destination database connection.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		}
	}

	public MigrationResult fromAccumuloToPostgres(final String accumuloTable,
			final String postgresTable)
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
		// System.out.println(copyString);
		Iterator<Entry<Key, Value>> iter = accInst
				.getTableIterator(accumuloTable);
		try {
			rs = getPostgreSQLResultSet(postgresTable);
			if (rs == null) {
				String message = "No results were fetched for the table: "
						+ postgresTable;
				logger.log(Level.INFO, message);
			} else {
				ResultSetMetaData rsmd = rs.getMetaData();
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
				while (iter.hasNext()) {
					Entry<Key, Value> e = iter.next();
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
					// System.out.println(value);
					// list is numbered from 0 (columns in PostgreSQL numbered
					// from 1)
					Integer colIndex = mapNameCol.get(colq.toString());
					if (colIndex == null) {
						throw new MigrationException(
								"No such column in PostgreSQL: "
										+ colq.toString());
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
