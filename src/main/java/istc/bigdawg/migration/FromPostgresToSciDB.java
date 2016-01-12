/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import istc.bigdawg.exceptions.MigrationException;
import istc.bigdawg.exceptions.SciDBException;
import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.RunShell;
import jline.internal.Log;

/**
 * @author adam
 *
 */
public class FromPostgresToSciDB implements FromDatabaseToDatabase {

	/**
	 * 
	 */
	public FromPostgresToSciDB() {
		System.out.println("Data migration from Postgres to SciDB");
		try {
			loadDataToSciDB();
		} catch (IOException | InterruptedException | SciDBException e) {
			e.printStackTrace();
		}
	}
	
	public String loadDataToSciDB() throws IOException, InterruptedException, SciDBException {
		InputStream resultInStream = RunShell.runSciDBquery("localhost",
				"load region from '/home/adam/data/tpch/tpch1G/csv/region.sci'");
		String resultString = IOUtils.toString(resultInStream,
				Constants.ENCODING);
		Log.debug("Load data to SciDB: "+resultString);
		return resultString;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * istc.bigdawg.migration.FromDatabaseToDatabase#migrate(istc.bigdawg.query.
	 * ConnectionInfo, java.lang.String, istc.bigdawg.query.ConnectionInfo,
	 * java.lang.String)
	 */
	@Override
	public MigrationResult migrate(ConnectionInfo connectionFrom, String objectFrom, ConnectionInfo connectionTo,
			String objectTo) throws MigrationException {
		// TODO Auto-generated method stub
		return null;
	}
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FromPostgresToSciDB migrator = new FromPostgresToSciDB();

	}

}
