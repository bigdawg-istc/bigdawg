/**
 * 
 */
package istc.bigdawg.accumulo;

import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.utils.ListConncatenator;
import istc.bigdawg.utils.Row;

import java.io.FileNotFoundException;
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
import org.apache.accumulo.core.master.thrift.MasterClientService.AsyncProcessor.initiateFlush;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 * @author Adam Dziedzic
 * 
 */
public class TpchFromPostgresToAccumulo {

	private static Logger lgr;
	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private AccumuloInstance accInst = null;
	private Long rowIdCounterForAccumuloFromPostgres = 0L;

	// parameters
	private int postgreSQLFetchSize = 1000;
	private long AccumuloBatchWriterMaxMemory = 1000000L;
	private int AccumuloBatchWriterMaxWriteThreads = 4;
	private int postgreSQLWritebatchSize = 1000;
	private int postgreSQLReaderCharSize = 1000000;
	private char delimiter;

	public TpchFromPostgresToAccumulo() throws AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException {
		lgr = Logger.getLogger(TpchFromPostgresToAccumulo.class.getName());
		accInst = AccumuloInstance.getInstance();
	}

	public static void sampleFile() throws FileNotFoundException,
			UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("/home/adam/data/sample.csv",
				"UTF-8");
		double d1 = 14.51341;
		double d2 = 1411.134;
		double d3 = 1341.131;
		int i1 = 1001;
		int i2 = 1001;
		int i3 = 5641;
		String s1 = "afdadfafd";
		String s2 = "afdadf 131";
		for (int i = 0; i < 100; ++i) {
			writer.println(d1 + "," + d2 + "," + d3 + "," + i1 + "," + i2 + ","
					+ "," + i3 + "," + s1 + "," + s2); // 64 bytes
		}
		writer.close();
	}

	private BatchWriter getAccumuloBatchWriter(final String table)
			throws AccumuloException, AccumuloSecurityException,
			AccumuloBigDawgException, TableNotFoundException {
		accInst.createTable(table);
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batchwriter for buffering mutations
		config.setMaxMemory(AccumuloBatchWriterMaxMemory);
		config.setMaxWriteThreads(AccumuloBatchWriterMaxWriteThreads);
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
		con.setAutoCommit(false);
		st = con.prepareStatement(query);
		// Turn use of the cursor on.
		st.setFetchSize(postgreSQLFetchSize);
		return st.executeQuery();
	}

	private List<Integer> getPrimaryColumnsPostgreSQL(final String table)
			throws SQLException {
		List<Integer> primaryColNum = new ArrayList<Integer>();
		String query = "SELECT pg_attribute.attnum "
				+ "FROM pg_index, pg_class, pg_attribute, pg_namespace "
				+ "WHERE " + "pg_class.oid = ?::regclass AND "
				+ "indrelid = pg_class.oid AND nspname = 'public' AND "
				+ "pg_class.relnamespace = pg_namespace.oid AND "
				+ "pg_attribute.attrelid = pg_class.oid AND "
				+ "pg_attribute.attnum = any(pg_index.indkey) AND indisprimary";
		// System.out.println(query);
		try {
			con = PostgreSQLInstance.getConnection();
			st = con.prepareStatement(query);
			st.setString(1, table);
			rs = st.executeQuery();
			while (rs.next()) {
				// System.out.println("Primary column number: "+rs.getInt(1));
				primaryColNum.add(new Integer(rs.getInt(1)));
			}
		} finally {
			cleanPostgreSQLResources();
		}
		return primaryColNum;

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
		System.out.println(rowString);
		sBuilder.append(rowString);
	}

	public void flushRowsToPostgreSQL(StringBuilder sBuilder,
			PushbackReader reader, CopyManager cpManager, String postgresTable)
			throws IOException, SQLException {
		StringBuilder copyString = new StringBuilder();
		copyString.append("COPY ");
		copyString.append(postgresTable);
		copyString.append(" FROM STDIN WITH (DELIMITER '|");
		//copyString.append(delimiter);
		copyString.append("')");
		System.out.println(copyString);
		reader.unread(sBuilder.toString().toCharArray());
		cpManager.copyIn(copyString.toString(), reader);
		sBuilder.delete(0, sBuilder.length());
	}

	public void fromAccumuloToPostgres(final String accumuloTable,
			final String postgresTable) throws AccumuloException,
			AccumuloSecurityException, AccumuloBigDawgException, SQLException,
			TableNotFoundException, IOException {
		Iterator<Entry<Key, Value>> iter = accInst
				.getTableIterator(accumuloTable);
		try {
			rs = getPostgreSQLResultSet(postgresTable);
			if (rs == null) {
				String message = "No results were fetched for the table: "
						+ postgresTable;
				System.out.println(message);
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
				System.out.println(thisRowId);
				// omit first initialization of rowId
				if (rowId != null && !rowId.equals(thisRowId)) {
					++fullCounter;
					createNewRowForPostgres(row, sBuilder);
					if (fullCounter % postgreSQLWritebatchSize == 0) {
						flushRowsToPostgreSQL(sBuilder, reader, cpManager, postgresTable);
					}
					row = new String[numOfCol];
				}
				rowId = thisRowId;
				Text colq = e.getKey().getColumnQualifier();
				int thisColNum = Integer.valueOf(colq.toString());
				String value = e.getValue().toString();
				System.out.println(value);
				// list is numbered from 0 (columns in postgresql are numbered
				// from 1)
				row[thisColNum - 1] = value;
			}
			createNewRowForPostgres(row, sBuilder);
			flushRowsToPostgreSQL(sBuilder, reader, cpManager, postgresTable);
		} finally {
			cleanPostgreSQLResources();
		}
	}

	Text getRowIdAccumuloFromPostgres(List<Integer> primaryColNum)
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
		BatchWriter writer = getAccumuloBatchWriter(accumuloTable);
		try {
			List<Integer> primaryColNum = getPrimaryColumnsPostgreSQL(postgresTable);
			/*
			 * gettting results from Postgres has to be after determining the PK
			 * columns we use the same reference to rs - ResultSet in the whole
			 * class
			 */
			rs = getPostgreSQLResultSet(postgresTable);
			if (rs == null) {
				String message = "No results were fetched for the table: "
						+ postgresTable;
				System.out.println(message);
				lgr.log(Level.INFO, message);
				return;
			}
			ResultSetMetaData rsmd = rs.getMetaData();
			int numOfCol = rsmd.getColumnCount();
			while (rs.next()) {
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
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			cleanPostgreSQLResources();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			sampleFile();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		TpchFromPostgresToAccumulo tpch = null;
		try {
			tpch = new TpchFromPostgresToAccumulo();
		} catch (AccumuloException | AccumuloSecurityException
				| AccumuloBigDawgException e1) {
			e1.printStackTrace();
			return;
		}
		List<String> tables = new ArrayList<String>();
		tables.add("region");
		// tables.add("nation");
		// tables.add("customer");
		// tables.add("orders");
		// tables.add("part");
		// tables.add("partsupp");
		// tables.add("supplier");
		// tables.add("lineitem");

		for (String table : tables) {
			try {
				try {
					long lStartTime = System.nanoTime();
					tpch.fromPostgresToAccumulo(table, table);
					String message = "From Postgres to Accumulo execution time in seconds: "
							+ (System.nanoTime() - lStartTime) / 1000000000L;
					System.out.println(message);
					lStartTime = System.nanoTime();
					tpch.fromAccumuloToPostgres(table, table + "fromaccumulo");
					message = "From Accumulo to Postgres execution time in seconds: "
							+ (System.nanoTime() - lStartTime) / 1000000000L;
					System.out.println(message);
				} catch (AccumuloException e) {
					e.printStackTrace();
				} catch (AccumuloSecurityException e) {
					e.printStackTrace();
				} catch (AccumuloBigDawgException e) {
					e.printStackTrace();
				} catch (TableNotFoundException e) {
					e.printStackTrace();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		for (String table : tables) {
			try {
				AccumuloInstance.getInstance().readAllData(table);
				// int rowNumber =
				// AccumuloInstance.getInstance().countRows(table);
				// System.out.println("row number for table " + table +
				// ": "rowNumber);
			} catch (TableNotFoundException | AccumuloException
					| AccumuloSecurityException | AccumuloBigDawgException e) {
				e.printStackTrace();
			}
		}
	}
}
