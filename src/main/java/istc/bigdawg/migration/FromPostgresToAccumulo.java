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
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import istc.bigdawg.accumulo.AccumuloConnectionInfo;

/**
 * 
 */

import istc.bigdawg.accumulo.AccumuloInstance;
import istc.bigdawg.exceptions.AccumuloBigDawgException;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.postgresql.PostgreSQLInstance;
import istc.bigdawg.utils.StackTrace;

/**
 * @author Adam Dziedzic
 * 
 */
public class FromPostgresToAccumulo extends FromDatabaseToDatabase {

	/**
	 * For transfer via network.
	 */
	private static final long serialVersionUID = 1L;

	private static Logger lgr = org.apache.log4j.Logger
			.getLogger(FromPostgresToAccumulo.class);

	private static Logger logger = Logger
			.getLogger(FromPostgresToAccumulo.class);

	private Connection con = null;
	private PreparedStatement st = null;
	private ResultSet rs = null;
	private AccumuloInstance accInst = null;

	// parameters
	private long rowIdCounterForAccumuloFromPostgres = 0L;
	private long accumuloBatchWriterMaxMemory = 50 * 1024 * 1024L;
	private int accumuloBatchWriterMaxWriteThreads = 4;
	private int accumuloBatchWriteSize = 1000;
	private int postgreSQLFetchSize = 50;

	private PostgreSQLConnectionInfo conFrom;
	private AccumuloConnectionInfo conTo;

	public AccumuloInstance getAccumuloInstance() {
		return accInst;
	}

	public FromPostgresToAccumulo() {
		logger.debug("Created migrator from PostgreSQL to Accumulo");
	}

	public FromPostgresToAccumulo(AccumuloInstance accInst) {
		this.accInst = accInst;
	}

	private BatchWriter getAccumuloBatchWriter(final String table)
			throws AccumuloException, AccumuloSecurityException,
			AccumuloBigDawgException, TableNotFoundException {
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batch-writer for buffering mutations
		config.setMaxMemory(accumuloBatchWriterMaxMemory);
		config.setMaxWriteThreads(accumuloBatchWriterMaxWriteThreads);
		try {
			BatchWriter writer = accInst.getConnector().createBatchWriter(table,
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

	private Text getRowIdAccumuloFromPostgres(List<Integer> primaryColNum)
			throws SQLException {
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

	@Override
	public MigrationResult migrate(MigrationInfo migrationInfo)
			throws MigrationException {
		if (!(migrationInfo.getConnectionTo() instanceof AccumuloConnectionInfo
				&& migrationInfo
						.getConnectionFrom() instanceof PostgreSQLConnectionInfo)) {
			return null;
		}
		conTo = (AccumuloConnectionInfo) migrationInfo.getConnectionTo();
		conFrom = (PostgreSQLConnectionInfo) migrationInfo.getConnectionFrom();
		try {
			this.con = PostgreSQLHandler.getConnection(conFrom);
			con.setAutoCommit(false);
			con.setReadOnly(true);
		} catch (SQLException e) {
			String msg = "Could not connect to PostgreSQL.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
		}

		try {
			this.accInst = AccumuloInstance.getFullInstance(conTo);
			accInst.createTableIfNotExists(migrationInfo.getObjectTo());
		} catch (Exception e) {
			throw new MigrationException("Problem with Accumulo", e);
		}
		try {
			return fromPostgresToAccumulo(migrationInfo.getObjectFrom(),
					migrationInfo.getObjectTo());
		} catch (MigrationException e) {
			String msg = "Could not close the destination database connection.";
			logger.error(msg + StackTrace.getFullStackTrace(e), e);
			throw new MigrationException(msg, e);
		}
	}

	public MigrationResult fromPostgresToAccumulo(final String postgresTable,
			final String accumuloTable) throws MigrationException {
		logger.debug("Migrate data from PostgreSQL to Accumulo.");
		long startTimeMigration = System.currentTimeMillis();
		BatchWriter writer = null;
		long fullRowCounter = 0; /* Full counter of rows extracted/loaded. */
		try {
			try {
				writer = getAccumuloBatchWriter(accumuloTable);
			} catch (AccumuloException | AccumuloSecurityException
					| AccumuloBigDawgException | TableNotFoundException exp) {
				String msg = "Could not open Accumulo BatchWriter.";
				logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
				throw new MigrationException(msg, exp);
			}
			List<Integer> primaryColNum;
			primaryColNum = new PostgreSQLHandler(conFrom, con)
					.getPrimaryColumnsNoRecourceCleaning(postgresTable);
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
			} else {
				ResultSetMetaData rsmd = rs.getMetaData();
				int numOfCol = rsmd.getColumnCount();
				long counterLocal = 0; /* Local counter for rows for a batch. */
				while (rs.next()) {
					++counterLocal;
					Text rowId = getRowIdAccumuloFromPostgres(primaryColNum);
					// Text rowId = new Text(Long.toString(counter));
					for (int colNum = 1; colNum <= numOfCol; ++colNum) {
						/*
						 * We can have a composite primary key so we have to
						 * store each column separately including primary keys.
						 */
						Mutation mutation = new Mutation(rowId);
						/* colFamily, colQualifier, value */
						Text colFamily = new Text("" + colNum);
						Text colQual = new Text(rsmd.getColumnName(colNum));
						Value value = new Value(
								rs.getObject(colNum).toString().getBytes());
						mutation.put(colFamily, colQual, value);
						try {
							writer.addMutation(mutation);
						} catch (MutationsRejectedException e) {
							String msg = "Mutation (new data) to Accumulo"
									+ " was rejected with: " + "colFamily: "
									+ colFamily + " colQualifier: " + colQual
									+ " value:" + value;
							logger.error(msg + StackTrace.getFullStackTrace(e),
									e);
							throw new MigrationException(msg, e);
						}
					}
					if (counterLocal % accumuloBatchWriteSize == 0) {
						counterLocal = 0;
						try {
							writer.flush();
							writer.close();
						} catch (MutationsRejectedException exp) {
							String msg = "Could not close BatchWriter to Accumulo.";
							logger.error(
									msg + StackTrace.getFullStackTrace(exp),
									exp);
							throw new MigrationException(msg, exp);
						}
						try {
							writer = getAccumuloBatchWriter(accumuloTable);
						} catch (AccumuloException | AccumuloSecurityException
								| AccumuloBigDawgException
								| TableNotFoundException exp) {
							String msg = "Could not open next Accumulo BatchWriter.";
							logger.error(
									msg + StackTrace.getFullStackTrace(exp),
									exp);
							throw new MigrationException(msg, exp);
						}
					}
				}
			}
			long endTimeMigration = System.currentTimeMillis();
			long durationMsec = endTimeMigration - startTimeMigration;
			return new MigrationResult(fullRowCounter, fullRowCounter,
					startTimeMigration, endTimeMigration, durationMsec);
		} catch (SQLException exp) {
			String msg = "Problem with access to PostgreSQL: "
					+ exp.getMessage();
			logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
			throw new MigrationException(msg, exp);
		} finally {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (MutationsRejectedException exp) {
					String msg = "Could not close BatchWriter to Accumulo.";
					logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
					throw new MigrationException(msg, exp);
				}
			}
			try {
				cleanPostgreSQLResources();
			} catch (SQLException exp) {
				String msg = "Could not clean resources to PostgreSQL.";
				logger.error(msg + StackTrace.getFullStackTrace(exp), exp);
				throw new MigrationException(msg, exp);
			}
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
			AccumuloInstance accInst = AccumuloInstance.getInstance();
			tpch = new FromPostgresToAccumulo(accInst);
		} catch (AccumuloException | AccumuloSecurityException
				| AccumuloBigDawgException e1) {
			e1.printStackTrace();
			System.exit(1);
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
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter("countRowsPostgres.log", true)));
			Connection con = PostgreSQLInstance.getConnection();
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

		AccumuloInstance accInst = tpch.getAccumuloInstance();
		for (String tableName : tables) {
			accInst.deleteTable(tableName);
			accInst.createTable(tableName);
		}

		PrintWriter fullWriter = new PrintWriter(new BufferedWriter(
				new FileWriter("fromPostgresToAccumuloFull.log", true)));
		String fullMessage = "";
		if (mode.equals("fromPostgresToAccumulo") || all) {
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter("fromPostgresToAccumulo.log", true)));
			long lStartTime = System.nanoTime();
			for (String table : tables) {
				logger.debug("Table: " + table);
				try {
					tpch.fromPostgresToAccumulo(table, table);
				} catch (MigrationException e) {
					e.printStackTrace();
				}
			}
			String message = "From Postgres to Accumulo execution time in seconds: "
					+ (System.nanoTime() - lStartTime) / 1000000000L
					+ " :scale factor: " + scaleFactor;
			fullMessage += message;
			System.out.println(message);
			writer.append(message + "\n");
			writer.close();
		}
		// long alloRows = 0;
		// if (mode.equals("countRowsAccumulo") || all) {
		// PrintWriter writer = new PrintWriter(new BufferedWriter(new
		// FileWriter("countRowsAccumulo.log", true)));
		// for (String table : tables) {
		// try {
		// // AccumuloInstance.getInstance().readAllData(table);
		// long rowNumber = accInst.countRows(table);
		// alloRows += rowNumber;
		// String messageString = "Accumulo row number for table " + table + ":
		// " + rowNumber;
		// System.out.println(messageString);
		// writer.append(messageString + "\n");
		// } catch (TableNotFoundException e) {
		// e.printStackTrace();
		// }
		// }
		// writer.close();
		// }
		fullWriter.append(TestTpchPostgresAccumulo.getFullLog(fullMessage
				+ ":Total number of rows before loading in Postgres:"
				+ allRowsNumber + "\n"));
		fullWriter.close();
	}
}
