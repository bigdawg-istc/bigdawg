/**
 * 
 */
package istc.bigdawg.migration;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import istc.bigdawg.LoggerSetup;
import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.executor.ExecutorEngine.LocalQueryExecutionException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;

/**
 * @author Adam Dziedzic
 * 
 *         Atomic migration of tables between instances of PostgreSQL.
 */
public class AtomicMigration {
	/** log */
	private static Logger logger = Logger.getLogger(AtomicMigration.class);

	/** From which PostgreSQL instance to migrate the data. */
	private PostgreSQLConnectionInfo connectionFrom;

	/** From which table to migrate the data. */
	private String fromTable;

	/** To which PostgreSQL instance to migrate the data to. */
	private PostgreSQLConnectionInfo connectionTo;

	/** To which table to migrate the data. */
	private String toTable;

	/** List of column names. */
	private List<String> columnNames;

	/** Convenient handler for interactions with PostgreSQL. */
	private PostgreSQLHandler handler;

	/** For crude migration. */
	private FromPostgresToPostgres migrator;

	private static final String RENAME_TO_OLD = "alter table %s rename to %s_old";
	private static final String RENAME_FROM_OLD = "alter table %s_old rename to %s";
	private static final String TABLE_NEW = "create table %s_new as select * from %s_old limit 0";
	private static final String TABLE_UPDATE = "create table %s_update as select * from %s_old limit 0";
	private static final String TABLE_DELETE = "create table %s_delete as select * from %s_old limit 0";

	private static final String DROP_FUNCTION = "drop function if exists change_%s()";
	private static final String DROP_TRIGGER = "drop trigger if exists %s_crud on %s";

	/** public for tests */
	public static final String DROP_IF_EXISTS = "drop table if exists %s";

	/**
	 * Create an instance of atomic migrator.
	 * 
	 * @throws SQLException
	 */
	public AtomicMigration(PostgreSQLConnectionInfo connectionFrom,
			String fromTable, PostgreSQLConnectionInfo connectionTo,
			String toTable) throws SQLException {
		this.connectionFrom = connectionFrom;
		this.fromTable = fromTable;
		this.connectionTo = connectionTo;
		this.toTable = toTable;
		handler = new PostgreSQLHandler(connectionFrom);
		columnNames = handler.getColumnNames(fromTable);
		migrator = new FromPostgresToPostgres();
	}

	/**
	 * 
	 * @return String representing comma separated columns.
	 */
	private String getCommaSeparatedColumns() {
		return columnNames.stream().collect(Collectors.joining(","));
	}

	/**
	 * 
	 * @return String representing comma separated columns, each column is
	 *         prefixed by "OLD."
	 */
	private String getCommaSeparatedColumnsWithOLD() {
		return columnNames.stream().map(columnName -> "OLD." + columnName)
				.collect(Collectors.joining(","));
	}

	/**
	 * for table region:
	 * 
	 * CREATE OR REPLACE VIEW region AS SELECT * FROM region_new UNION (SELECT *
	 * FROM region_old EXCEPT (SELECT * FROM region_delete UNION SELECT * FROM
	 * region_update));
	 * 
	 * @param table
	 * @return view of the table
	 */
	private String getCreateView(String table) {
		String psqlView = "CREATE OR REPLACE VIEW " + table
				+ " AS SELECT * FROM " + table + "_new UNION (SELECT * FROM "
				+ table + "_old EXCEPT (SELECT * FROM " + table
				+ "_delete UNION SELECT * FROM " + table + "_update))";
		logger.debug(psqlView);
		return psqlView;
	}

	/**
	 * 
	 * @param table
	 * @return trigger for queries
	 */
	private String getTrigger(String table) {
		String psqlTrigger = "CREATE TRIGGER " + table
				+ "_crud INSTEAD OF INSERT OR UPDATE OR DELETE ON " + table
				+ " FOR EACH ROW EXECUTE PROCEDURE change_" + table + "()";
		logger.debug(psqlTrigger);
		return psqlTrigger;
	}

	/**
	 * Get function for PostgreSQL to replace the typical: insert, update,
	 * delete operations.
	 * 
	 * @param table
	 *            The table which is represented as view.
	 * @return
	 */
	private String getFunction(String table) {
		String psqlFunction = "CREATE OR REPLACE FUNCTION change_" + table
				+ "() RETURNS TRIGGER AS $$ " + "BEGIN "
				+ "IF (TG_OP = 'DELETE') THEN " + "DELETE FROM " + table
				+ "_new WHERE (" + getCommaSeparatedColumns() + ") = ("
				+ getCommaSeparatedColumnsWithOLD() + "); " + "INSERT INTO "
				+ table + "_delete VALUES(OLD.*); "
				+ "IF NOT FOUND THEN RETURN NULL; END IF; " + "RETURN OLD;"
				+ "ELSIF (TG_OP = 'UPDATE') THEN INSERT INTO " + table
				+ "_new VALUES(NEW.*); " + "INSERT INTO " + table
				+ "_update VALUES(OLD.*);"
				+ "IF NOT FOUND THEN RETURN NULL; END IF; " + "RETURN NEW; "
				+ "ELSIF (TG_OP = 'INSERT') THEN " + "INSERT INTO " + table
				+ "_new VALUES(NEW.*); RETURN NEW; END IF; "
				+ "END; $$ LANGUAGE plpgsql";
		/* the statement above cannot be logged easily due to '$$' */
		System.out.println(psqlFunction);
		return psqlFunction;
	}

	/**
	 * The final insert into the target table at the desintion database.
	 * 
	 * @param table
	 *            The target table.
	 * @return the insert into query
	 */
	private String getInsertIntoDestination(String table) {
		String insertInto = "INSERT INTO " + table + "_old SELECT * FROM "
				+ table + "_new";
		logger.debug(insertInto);
		return insertInto;
	}

	/**
	 * Delete the updated and deleted rows from the old table.
	 * 
	 * @param table
	 * @return the delete sql statement
	 */
	private String getDeleteForDestination(String table) {
		String deleteInDestination = "DELETE FROM " + table + "_old WHERE ("
				+ getCommaSeparatedColumns() + ") IN (SELECT * FROM " + table
				+ "_delete UNION SELECT * FROM " + table + "_update)";
		logger.debug(deleteInDestination);
		return deleteInDestination;
	}

	/**
	 * Migrate table atomically. Abort if any error.
	 * 
	 * @param table
	 * @throws LocalQueryExecutionException
	 * @throws SQLException
	 * @throws MigrationException
	 */
	public void migrate() throws LocalQueryExecutionException, SQLException,
			MigrationException {
		prepareFrom(connectionFrom, fromTable);
		migrator.migrate(connectionFrom, fromTable + "_old", connectionTo,
				toTable + "_old");
		/* start of the window of time without the table */
		handler.execute("drop view " + fromTable);
		try {
			migrator.migrate(connectionFrom, fromTable + "_new", connectionTo,
					toTable + "_new");
			migrator.migrate(connectionFrom, fromTable + "_delete",
					connectionTo, toTable + "_delete");
			migrator.migrate(connectionFrom, fromTable + "_update",
					connectionTo, toTable + "_update");
			prepareTo(connectionTo, toTable);
		} catch (SQLException e) {
			prepareTo(connectionFrom, fromTable);
		}
		/* the end of the window with no target table */
		cleanFrom(connectionFrom, fromTable);
	}

	/**
	 * Prepare the source node (from node) for the atomic migration: create
	 * view, trigger, function and additional tables.
	 * 
	 * @throws SQLException
	 *             problem with sql statements for PostgreSQL
	 */
	private void prepareFrom(PostgreSQLConnectionInfo conInfo, String table)
			throws SQLException {
		Connection con = PostgreSQLHandler.getConnection(conInfo);
		con.setAutoCommit(false);
		Statement statement = con.createStatement();

		statement.execute(String.format(DROP_IF_EXISTS, table + "_old"));
		statement.execute(String.format(RENAME_TO_OLD, table, table));

		statement.execute(String.format(DROP_IF_EXISTS, table + "_new"));
		statement.execute(String.format(TABLE_NEW, table, table));

		statement.execute(String.format(DROP_IF_EXISTS, table + "_update"));
		statement.execute(String.format(TABLE_UPDATE, table, table));

		statement.execute(String.format(DROP_IF_EXISTS, table + "_delete"));
		statement.execute(String.format(TABLE_DELETE, table, table));

		statement.execute(getCreateView(table));
		statement.execute(getFunction(table));

		statement.execute(
				"drop trigger if exists " + table + "_crud on " + table);
		statement.execute(getTrigger(table));
		con.commit();
		statement.close();
		con.close();
	}

	/**
	 * Prepare the destination database/node (to database) to serve queries for
	 * the table which was migrated atomically.
	 * 
	 * @throws SQLException
	 *             problem with sql statements for PostgreSQL
	 */
	private void prepareTo(PostgreSQLConnectionInfo conInfo, String table)
			throws SQLException {
		Connection con = PostgreSQLHandler.getConnection(conInfo);
		con.setAutoCommit(false);
		Statement statement = con.createStatement();
		statement.execute(getDeleteForDestination(table));
		statement.execute(getInsertIntoDestination(table));
		statement.execute(String.format(RENAME_FROM_OLD, table, table));
		/* remove the additional tables */
		for (String suffix : Arrays.asList("_delete", "_update", "_new")) {
			statement.execute(String.format(DROP_IF_EXISTS, table + suffix));
		}
		statement.close();
		con.commit();
		con.close();
	}

	/**
	 * Drop view, trigger, function and additional tables in the source
	 * database/node.
	 * 
	 * @throws SQLException
	 */
	private void cleanFrom(PostgreSQLConnectionInfo conInfo, String table)
			throws SQLException {
		Connection con = PostgreSQLHandler.getConnection(conInfo);
		con.setAutoCommit(false);
		Statement statement = con.createStatement();
		String dropFunction = String.format(DROP_FUNCTION, table);
		logger.debug("drop function command: " + dropFunction);
		statement.execute(dropFunction);
		String dropTrigger = String.format(DROP_TRIGGER, table, table);
		logger.debug("drop trigger command: " + dropTrigger);
		statement.execute(dropTrigger);
		for (String suffix : Arrays.asList("_delete", "_update", "_new",
				"_old")) {
			statement.execute(String.format(DROP_IF_EXISTS, table + suffix));
		}
		con.commit();
		statement.close();
		con.close();
	}

	public static void main(String[] args) throws LocalQueryExecutionException,
			SQLException, MigrationException {
		LoggerSetup.setLogging();
		PostgreSQLConnectionInfo connectionFrom = new PostgreSQLConnectionInfo(
				"localhost", "5431", "tpch", "pguser", "test");
		PostgreSQLConnectionInfo connectionTo = new PostgreSQLConnectionInfo(
				"localhost", "5430", "tpch", "pguser", "test");
		String table = "orders";

		PostgreSQLHandler handlerFrom = new PostgreSQLHandler(connectionFrom);
		try {
			handlerFrom.execute(String.format(DROP_IF_EXISTS, table));
		} catch (LocalQueryExecutionException e) {
			logger.debug(e.getMessage());
			handlerFrom.execute(String.format(DROP_IF_EXISTS, table));
		}
		handlerFrom.execute("create table " + table + " as select * from "
				+ table + "_backup");

		PostgreSQLHandler handlerTo = new PostgreSQLHandler(connectionTo);
		handlerTo.execute(String.format(DROP_IF_EXISTS, table));

		AtomicMigration atomicMigration = new AtomicMigration(connectionFrom,
				table, connectionTo, table);
		atomicMigration.migrate();
	}

}
