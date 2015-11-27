/**
 * 
 */
package istc.bigdawg.accumulo;

import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.utils.Row;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
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

/**
 * @author Adam Dziedzic
 * 
 */
public class TpchFromPostgresToAccumulo {

	private static Logger lgr;
	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;

	// parameters
	private int postgreSQLFetchSize = 1000;
	private long AccumuloBatchWriterMaxMemory = 1000000L;
	private int AccumuloBatchWriterMaxWriteThreads = 4;
	private int postgreSQLWritebatchSize = 1000;

	public TpchFromPostgresToAccumulo() {
		lgr = Logger.getLogger(TpchFromPostgresToAccumulo.class.getName());
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
		// prepare accumulo
		AccumuloInstance acc = AccumuloInstance.getInstance();
		acc.createTable(table);
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batchwriter for buffering mutations
		config.setMaxMemory(AccumuloBatchWriterMaxMemory);
		config.setMaxWriteThreads(AccumuloBatchWriterMaxWriteThreads);
		try {
			BatchWriter writer = acc.getConnector().createBatchWriter(table,
					config);
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
		rs = st.executeQuery();
		return rs;
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
		con = PostgreSQLInstance.getConnection();
		st = con.prepareStatement(query);
		st.setString(1, table);
		rs = st.executeQuery();
		while (rs.next()) {
			primaryColNum.add(rs.getInt(1));
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

	public void fromAccumuloToPostgres(final String table)
			throws AccumuloException, AccumuloSecurityException,
			AccumuloBigDawgException, SQLException, TableNotFoundException {
		AccumuloInstance accInst = AccumuloInstance.getInstance();
		Iterator<Entry<Key, Value>> iter = accInst.getTableIterator(table);
		ResultSet rs = getPostgreSQLResultSet(table);
		PreparedStatement insert = null;
		if (rs == null) {
			String message = "No results were fetched for the table: " + table;
			System.out.println(message);
			lgr.log(Level.INFO, message);
			return;
		}
		StringBuilder sqlStatement = new StringBuilder();
		sqlStatement.append("insert into ");
		sqlStatement.append(table + " values(");
		ResultSetMetaData rsmd = rs.getMetaData();
		int NumOfCol = rsmd.getColumnCount();
		List<Class> types = Row.getColumnTypes(rsmd);
		for (int i = 1; i < NumOfCol; ++i) {
			if (i > 1) {
				sqlStatement.append(",");
			}
			sqlStatement.append("?");
		}
		sqlStatement.append(")");
		con = PostgreSQLInstance.getConnection();
		insert = con.prepareStatement(sqlStatement.toString());
		int counter = 0;
		while (iter.hasNext()) {
			++counter;
			for (int i = 0; i < NumOfCol; ++i) {
				Entry<Key, Value> e = iter.next();
				Text colf = e.getKey().getColumnFamily();
				Text colq = e.getKey().getColumnQualifier();
				Value value = e.getValue();
				int thisColNum = Integer.valueOf(colq.toString());
				System.out.println(thisColNum);
				System.out.println(types.get(thisColNum - 1));
				// types list is number from 0, columns in sql from 1
				// insert.setObject(thisColNum, types.get(thisColNum-1).);
			}
			insert.addBatch();
			if (counter % postgreSQLWritebatchSize == 0) {
				insert.executeBatch();
			}
		}
		insert.executeBatch();
	}

	public void fromPostgresToAccumulo(final String table) throws SQLException,
			AccumuloException, AccumuloSecurityException,
			AccumuloBigDawgException, TableNotFoundException {
		BatchWriter writer = getAccumuloBatchWriter(table);
		ResultSet rs = getPostgreSQLResultSet(table);
		if (rs == null) {
			String message = "No results were fetched for the table: " + table;
			System.out.println(message);
			lgr.log(Level.INFO, message);
			return;
		}
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int NumOfCol = rsmd.getColumnCount();
			List<Integer> primaryColNum = getPrimaryColumnsPostgreSQL(table);
			while (rs.next()) {
				StringBuilder rowIdInit = new StringBuilder();
				for (int colNum : primaryColNum) {
					if (colNum > 1) {
						rowIdInit.append(":");
					}
					rowIdInit.append(rs.getObject(colNum).toString());
				}
				Text rowId = new Text(rowIdInit.toString());
				for (int colNum = 1; colNum <= NumOfCol; colNum++) {
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
	 */
	public static void main(String[] args) {
		try {
			sampleFile();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		TpchFromPostgresToAccumulo tpch = new TpchFromPostgresToAccumulo();
		List<String> tables = new ArrayList<String>();
		tables.add("region");
		tables.add("nation");
		//tables.add("customer");
		//tables.add("orders");
		// tables.add("part");
		// tables.add("partsupp");
		// tables.add("supplier");
		// tables.add("lineitem");

		long lStartTime = System.nanoTime();
		for (String table : tables) {
			try {
				try {
					tpch.fromPostgresToAccumulo(table);
					// tpch.fromAccumuloToPostgres(table);
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
		String messageQuery = "PostgreSQL query execution time seconds: "
				+ (System.nanoTime() - lStartTime) / 1000000000L + ",";
		System.out.println(messageQuery);
		for (String table : tables) {
			try {
				int rowNumber = AccumuloInstance.getInstance().countRows(table);
				System.out.println("row number for table "+table+": "+rowNumber);
			} catch (TableNotFoundException | AccumuloException
					| AccumuloSecurityException | AccumuloBigDawgException e) {
				e.printStackTrace();
			}
		}
	}
}
