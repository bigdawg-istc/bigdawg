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
import java.util.Iterator;
import java.util.List;
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

import istc.bigdawg.accumulo.AccumuloInstance;

/**
 * @author Adam Dziedzic
 * 8:37:41 PM
 *
 */

import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.migration.TestTpchPostgresAccumulo;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.utils.ListConncatenator;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromAccumuloToPostgres {

	private static Logger lgr = Logger.getLogger(FromAccumuloToPostgres.class);

	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private AccumuloInstance accInst = null;

	// parameters
	private int postgreSQLWritebatchSize = 1000;
	private int postgreSQLReaderCharSize = 1000000;
	private char delimiter = '|';

	public FromAccumuloToPostgres() throws AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException {
		accInst = AccumuloInstance.getInstance();
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
		con = PostgreSQLInstance.getConnection();
		con.setReadOnly(true);
		con.setAutoCommit(false);
		st = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		return st.executeQuery();
	}

	public void fromAccumuloToPostgres(final String accumuloTable,
			final String postgresTable) throws AccumuloException,
					AccumuloSecurityException, AccumuloBigDawgException,
					SQLException, TableNotFoundException, IOException {
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
				// System.out.println(message);
				lgr.log(Level.INFO, message);
				return;
			}
			ResultSetMetaData rsmd = rs.getMetaData();
			int numOfCol = rsmd.getColumnCount();
			con = PostgreSQLInstance.getConnection();
			StringBuilder sBuilder = new StringBuilder();
			CopyManager cpManager = new CopyManager((BaseConnection) con); // ((PGConnection)
																			// con).getCopyAPI();
			PushbackReader reader = new PushbackReader(new StringReader(""),
					postgreSQLReaderCharSize);
			/*
			 * count rows in sense of PostgreSQL one PostgreSQL row == many rows
			 * combined from Accumulo)
			 */
			int fullCounter = 0;
			// create a new row
			String[] row = new String[numOfCol];
			Text rowId = null;
			while (iter.hasNext()) {
				Entry<Key, Value> e = iter.next();
				Text thisRowId = e.getKey().getRow();
				// System.out.println(thisRowId);
				// omit first initialization of rowId
				if (rowId != null && !rowId.equals(thisRowId)) {
					++fullCounter;
					createNewRowForPostgres(row, sBuilder);
					if (fullCounter % postgreSQLWritebatchSize == 0) {
						flushRowsToPostgreSQL(sBuilder, reader, cpManager,
								postgresTable, copyString);
					}
					row = new String[numOfCol];
				}
				rowId = thisRowId;
				Text colq = e.getKey().getColumnQualifier();
				int thisColNum = Integer.valueOf(colq.toString());
				String value = e.getValue().toString();
				// System.out.println(value);
				// list is numbered from 0 (columns in postgresql are numbered
				// from 1)
				row[thisColNum - 1] = value;
			}
			if (rowId != null) {
				createNewRowForPostgres(row, sBuilder);
				flushRowsToPostgreSQL(sBuilder, reader, cpManager,
						postgresTable, copyString);
			}
		} finally {
			cleanPostgreSQLResources();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws TableNotFoundException
	 * @throws AccumuloBigDawgException
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws SQLException
	 */
	public static void main(String[] args) throws IOException, SQLException,
			AccumuloException, AccumuloSecurityException,
			AccumuloBigDawgException, TableNotFoundException {
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
			fromAccumuloToPostgres = new FromAccumuloToPostgres();
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
