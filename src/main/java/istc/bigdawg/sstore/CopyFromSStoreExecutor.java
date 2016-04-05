package istc.bigdawg.sstore;

import java.sql.SQLException;

import istc.bigdawg.migration.CopyFromPostgresExecutor;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;

public class CopyFromSStoreExecutor extends CopyFromPostgresExecutor {

    public CopyFromSStoreExecutor(PostgreSQLConnectionInfo connectionPostgreSQL, String copyFromString,
	    String outputFile) throws SQLException {
	super(connectionPostgreSQL, copyFromString, outputFile);
	// TODO Auto-generated constructor stub
    }

}
