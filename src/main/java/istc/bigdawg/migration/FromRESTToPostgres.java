package istc.bigdawg.migration;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;
import istc.bigdawg.postgresql.PostgreSQLHandler;
import istc.bigdawg.rest.RESTConnectionInfo;
import istc.bigdawg.utils.StackTrace;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 * Migration from REST into Postgres
 *
 * @author Matthew J. Mucklo
 */
public class FromRESTToPostgres extends FromDatabaseToDatabase {
    private static Logger logger = Logger
            .getLogger(FromRESTToPostgres.class);

    public FromRESTToPostgres() {
        super(new ExportREST(), // @TODO - maybe support other formats?
                LoadPostgres.ofFormat(FileFormat.CSV));
    }

    @Override
    public MigrationResult executeMigrationLocalRemote() throws MigrationException {
        return this.executeMigrationLocally();
    }

    public MigrationResult executeMigrationLocally() throws MigrationException {
        MigrationResult migrationResult = super.executeMigrationLocally();
        try {
            PostgreSQLHandler.getConnection(((PostgreSQLConnectionInfo) this.migrationInfo.getConnectionTo())).close();
        }
        catch (SQLException e) {
            throw new MigrationException(e.toString());
        }
        return migrationResult;
    }

    /**
     * Migrate data from REST to Postgres.
     */
    public MigrationResult migrate(MigrationInfo migrationInfo)	throws MigrationException {
        logger.debug("General data migration: " + this.getClass().getName());
        if (migrationInfo.getConnectionFrom() instanceof RESTConnectionInfo
                && migrationInfo.getConnectionTo() instanceof PostgreSQLConnectionInfo) {
            try {
                this.migrationInfo = migrationInfo;
                return this.dispatch();
            } catch (Exception e) {
                logger.error(StackTrace.getFullStackTrace(e));
                throw new MigrationException(e.getMessage(), e);
            }
        }
        return null;
    }
}
