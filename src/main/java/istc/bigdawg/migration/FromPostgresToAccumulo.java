/**
 * 
 */
package istc.bigdawg.migration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * 
 */

import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.migration.TestTpchPostgresAccumulo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLInstance;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToAccumulo {

	private static Logger lgr = org.apache.log4j.Logger.getLogger(FromPostgresToAccumulo.class);

	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private AccumuloInstance accInst = null;

	// parameters
	private long rowIdCounterForAccumuloFromPostgres = 0L;
	private long accumuloBatchWriterMaxMemory = 50 * 1024 * 1024l;
	private int accumuloBatchWriterMaxWriteThreads = 4;
	private int accumuloBatchWriteSize = 1000;
	private int postgreSQLFetchSize = 50;

	public AccumuloInstance getAccumuloInstance() {
		return accInst;
	}

	public FromPostgresToAccumulo() throws AccumuloException, AccumuloSecurityException, AccumuloBigDawgException {
		accInst = AccumuloInstance.getInstance();
	}

	private BatchWriter getAccumuloBatchWriter(final String table)
			throws AccumuloException, AccumuloSecurityException, AccumuloBigDawgException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batch-writer for buffering mutations
		config.setMaxMemory(accumuloBatchWriterMaxMemory);
		config.setMaxWriteThreads(accumuloBatchWriterMaxWriteThreads);
		try {
			BatchWriter writer = accInst.getConnector().createBatchWriter(table, config);
			return writer;
		} catch (TableNotFoundException e1) {
			e1.printStackTrace();
			throw e1;
		}
	}

	private ResultSet getPostgreSQLResultSet(final String table) throws SQLException {
		String query = "Select * from " + table.replace(";", "").replace(" ", "");
		con = PostgreSQLInstance.getConnection();
		con.setReadOnly(true);
		con.setAutoCommit(false);
		st = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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

	private Text getRowIdAccumuloFromPostgres(List<Integer> primaryColNum) throws SQLException {
		if (primaryColNum.size() == 0) {
			++rowIdCounterForAccumuloFromPostgres;
			return new Text(Long.toString(rowIdCounterForAccumuloFromPostgres));
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

	public void fromPostgresToAccumulo(final String postgresTable, final String accumuloTable) throws SQLException,
			AccumuloException, AccumuloSecurityException, AccumuloBigDawgException, TableNotFoundException {
		BatchWriter writer = null;
		try {
			writer = getAccumuloBatchWriter(accumuloTable);
			List<Integer> primaryColNum = new PostgreSQLHandler().getPrimaryColumns(postgresTable);
			/*
			 * gettting results from Postgres has to be after determining the PK
			 * columns we use the same reference to rs - ResultSet in the whole
			 * class
			 */
			rs = getPostgreSQLResultSet(postgresTable);
			if (rs == null) {
				String message = "No results were fetched for the table: " + postgresTable;
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
				// Text rowId = new Text(Long.toString(counter));
				for (int colNum = 1; colNum <= numOfCol; ++colNum) {
					/*
					 * We can have a composite primary key so we have to store
					 * each column separately including primary keys.
					 */
					Mutation mutation = new Mutation(rowId);
					/* colFamily, colQualifier, value */
					mutation.put(new Text(rsmd.getColumnName(colNum)), new Text("" + colNum),
							new Value(rs.getObject(colNum).toString().getBytes()));
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
	public static void main(String[] args) throws IOException, SQLException, AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException, TableNotFoundException {

		// Set up a simple configuration that logs on the console.
		// BasicConfigurator.configure();
		// lgr.info("Entering FromAccumuloToPostgres.");

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
		}
		if (args.length > 1) {
			scaleFactor = args[1];
		}
		System.out.println("Mode: " + mode);

		FromPostgresToAccumulo tpch = null;
		try {
			tpch = new FromPostgresToAccumulo();
		} catch (AccumuloException | AccumuloSecurityException | AccumuloBigDawgException e1) {
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

		long allRowsNumber = 0;
		if (mode.equals("countRowsPostgres") || all) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("countRowsPostgres.log", true)));
			Connection con = PostgreSQLInstance.getConnection();
			con.setReadOnly(true);
			con.setAutoCommit(false);
			for (String table : tables) {
				String query = "select count(*) from " + table;
				PreparedStatement st = con.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
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
				messageString = "PostgreSQL number of rows for table " + table + " is: " + rowNumber;
				System.out.println(messageString);
				writer.append(messageString + "\n");
			}
			con.close();
			writer.close();
		}

		AccumuloInstance accInst = tpch.getAccumuloInstance();
		for (String tableName : tables) {
			accInst.deleteTable(tableName);
			accInst.createTable(tableName);
		}

		PrintWriter fullWriter = new PrintWriter(
				new BufferedWriter(new FileWriter("fromPostgresToAccumuloFull.log", true)));
		String fullMessage = "";
		if (mode.equals("fromPostgresToAccumulo") || all) {
			PrintWriter writer = new PrintWriter(
					new BufferedWriter(new FileWriter("fromPostgresToAccumulo.log", true)));
			long lStartTime = System.nanoTime();
			for (String table : tables) {
				System.out.println(table);
				tpch.fromPostgresToAccumulo(table, table);
			}
			String message = "From Postgres to Accumulo execution time in seconds: "
					+ (System.nanoTime() - lStartTime) / 1000000000L + " :scale factor: " + scaleFactor;
			fullMessage += message;
			System.out.println(message);
			writer.append(message + "\n");
			writer.close();
		}
//		long alloRows = 0;
//		if (mode.equals("countRowsAccumulo") || all) {
//			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("countRowsAccumulo.log", true)));
//			for (String table : tables) {
//				try {
//					// AccumuloInstance.getInstance().readAllData(table);
//					long rowNumber = accInst.countRows(table);
//					alloRows += rowNumber;
//					String messageString = "Accumulo row number for table " + table + ": " + rowNumber;
//					System.out.println(messageString);
//					writer.append(messageString + "\n");
//				} catch (TableNotFoundException e) {
//					e.printStackTrace();
//				}
//			}
//			writer.close();
//		}
		fullWriter
				.append(TestTpchPostgresAccumulo.getFullLog(fullMessage + ":Total number of rows before loading in Postgres:" + allRowsNumber + "\n"));
		fullWriter.close();
	}
}
