/**
 * 
 */
package migration;

/**
 * 
 */

import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.utils.ListConncatenator;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.PushbackReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToAccumulo {

	private static Logger lgr;
	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private AccumuloInstance accInst = null;

	// parameters
	private Long rowIdCounterForAccumuloFromPostgres = 0L;
	private long accumuloBatchWriterMaxMemory = 5 * 1000L;
	private int accumuloBatchWriterMaxWriteThreads = 4;
	private int accumuloBatchWriteSize = 1000;
	private int postgreSQLFetchSize = 20;
	private char delimiter = '|';

	public FromPostgresToAccumulo() throws AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException {
		lgr = Logger.getLogger(FromPostgresToAccumulo.class.getName());
		accInst = AccumuloInstance.getInstance();
	}

	private BatchWriter getAccumuloBatchWriter(final String table)
			throws AccumuloException, AccumuloSecurityException,
			AccumuloBigDawgException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batch-writer for buffering mutations
		config.setMaxMemory(accumuloBatchWriterMaxMemory);
		config.setMaxWriteThreads(accumuloBatchWriterMaxWriteThreads);
		try {
			BatchWriter writer = accInst.getConnector().createBatchWriter(
					table, config);
			return writer;
		} catch (TableNotFoundException e1) {
			e1.printStackTrace();
			throw e1;
		}
	}

	private ResultSet getPostgreSQLResultSet(final String table)
			throws SQLException {
		String query = "Select * from "
				+ table.replace(";", "").replace(" ", "");
		con = PostgreSQLInstance.getConnection();
		con.setReadOnly(true);
		con.setAutoCommit(false);
		st = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		// Turn use of the cursor on.
		st.setFetchSize(postgreSQLFetchSize);
		return st.executeQuery();
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
		reader.unread(sBuilder.toString().toCharArray());
		cpManager.copyIn(copyString.toString(), reader);
		sBuilder.delete(0, sBuilder.length());
	}

	private Text getRowIdAccumuloFromPostgres(List<Integer> primaryColNum)
			throws SQLException {
		if (primaryColNum.size() == 0) {
			++rowIdCounterForAccumuloFromPostgres;
			return new Text(rowIdCounterForAccumuloFromPostgres.toString());
		}
		StringBuilder rowIdInit = new StringBuilder();
		for (int colNum : primaryColNum) {
			if (colNum > 1) {
				rowIdInit.append(":");
			}
			rowIdInit.append(rs.getObject(colNum).toString());
		}
		return new Text(rowIdInit.toString());
	}

	public void fromPostgresToAccumulo(final String postgresTable,
			String accumuloTable) throws SQLException, AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException,
			TableNotFoundException {
		BatchWriter writer = null;
		try {
			accInst.createTable(accumuloTable);
			writer = getAccumuloBatchWriter(accumuloTable);
			List<Integer> primaryColNum = new PostgreSQLHandler().getPrimaryColumns(postgresTable);
			/*
			 * gettting results from Postgres has to be after determining the PK
			 * columns we use the same reference to rs - ResultSet in the whole
			 * class
			 */
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
			long counter = 0;
			while (rs.next()) {
				++counter;
				Text rowId = getRowIdAccumuloFromPostgres(primaryColNum);
				for (int colNum = 1; colNum <= numOfCol; ++colNum) {
					/*
					 * We can have a composite primary key so we have to store
					 * each column separately including primary keys.
					 */
					Mutation mutation = new Mutation(rowId);
					/* colFamily, colQualifier, value */
					mutation.put(new Text(rsmd.getColumnName(colNum)),
							new Text("" + colNum),
							new Value(rs.getObject(colNum).toString()
									.getBytes()));
					writer.addMutation(mutation);
				}
				if (counter % accumuloBatchWriteSize == 0) {
					counter = 0;
					writer.close();
					writer = getAccumuloBatchWriter(accumuloTable);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (writer != null) {
				writer.close();
			}
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
		System.out.println();
		String mode = "";
		String scaleFactor = "";
		if (args.length > 0) {
			mode = args[0];
		} else if (args.length > 1) {
			scaleFactor = args[1];
		}
		System.out.println("Mode: " + mode);

		FromPostgresToAccumulo tpch = null;
		try {
			tpch = new FromPostgresToAccumulo();
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

		if (mode.equals("fromPostgresToAccumulo") || all) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter("fromPostgresToAccumulo.log", true)));
			long lStartTime = System.nanoTime();
			for (String table : tables) {
				tpch.fromPostgresToAccumulo(table, table);
			}
			String message = "From Postgres to Accumulo execution time in seconds: "
					+ (System.nanoTime() - lStartTime)
					/ 1000000000L
					+ " :scale factor: " + scaleFactor;
			System.out.println(message);
			writer.append(message+"\n");
			writer.close();
		}
		if (mode.equals("countRowsAccumulo") || all) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter("countRowsAccumulo.log", true)));
			for (String table : tables) {
				try {
					// AccumuloInstance.getInstance().readAllData(table);
					long rowNumber = AccumuloInstance.getInstance().countRows(
							table);
					String messageString = "row number for table " + table
							+ ": " + rowNumber;
					System.out.println(messageString);
					writer.append(messageString+"\n");
				} catch (TableNotFoundException | AccumuloException
						| AccumuloSecurityException | AccumuloBigDawgException e) {
					e.printStackTrace();
				}
			}
			writer.close();
		}
	}
}

