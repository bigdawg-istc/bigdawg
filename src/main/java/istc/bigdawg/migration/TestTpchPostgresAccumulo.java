/**
 * 
 */
package istc.bigdawg.migration;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;

import istc.bigdawg.exceptions.AccumuloBigDawgException;

/**
 * @author Adam Dziedzic
 * 9:24:07 PM
 *
 */
public class TestTpchPostgresAccumulo {

	public static String getFullLog(String message) {
		SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS");
		String currentTime = timeFormatter.format(System.currentTimeMillis());	
		return currentTime+" "+message; 
	}
	
	/**
	 * @param args
	 * @throws TableNotFoundException 
	 * @throws AccumuloBigDawgException 
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws SQLException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, SQLException, AccumuloException, AccumuloSecurityException, AccumuloBigDawgException, TableNotFoundException {
		FromPostgresToAccumulo.main(args);
		FromAccumuloToPostgres.main(args);
	}

}
