/**
 * 
 */
package istc.bigdawg.accumulo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;

/**
 * @author Adam Dziedzic
 * 
 */
public class DataLoader {

	private AccumuloInstance acc;

	public DataLoader(final AccumuloInstance acc) {
		this.acc = acc;
	}

	public int loadFileToTable(final String inputFileName,
			final String delimiter, final String tableName, final AccumuloRowQualifier accQual)
			throws FileNotFoundException, IOException,
			MutationsRejectedException, TableNotFoundException {
		BufferedReader br = new BufferedReader(new FileReader(inputFileName));
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batchwriter for buffering mutations
		config.setMaxMemory(100000L);
		BatchWriter writer = acc.getConnector().createBatchWriter(tableName,
				config);
		int lineCounter=0;
		for (String line; (line = br.readLine()) != null;) {
			String[] tokens = line.split(delimiter);
			AccumuloRowQualifier.AccumuloRow row = accQual.getAccumuloRow(tokens);
			Mutation mutation = new Mutation(row.getRowId());
			mutation.put(row.getColFam(), row.getColQual(), row.getValue());
			writer.addMutation(mutation);
			++lineCounter;
		}
		writer.close();
		br.close();
		return lineCounter;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String instanceName = "adam";
		String zooKeepers = "localhost";
		String userName = "root";
		String pass = "mypassw";
		String delimiter = ";";

		AccumuloRowQualifier accQual = new AccumuloRowQualifier(true, false,
				true, true);
		
		String tableName = "note_events_TedgeDeg";
		String fileName = "/home/adam/Chicago/mimic2_data/accumulo/note_events_TedgeDeg.csv";
		
		String tableName2 = "note_events_TedgeTxt";
		String fileName2 = "/home/adam/Chicago/mimic2_data/accumulo/note_events_TedgeTxt.csv";
		String delimiter2=";;";

		AccumuloInstance acc = null;
		int lineCounter = 0;
		try {
			acc=new AccumuloInstance(instanceName,zooKeepers,userName,pass);
			acc.createTable(tableName);
			acc.createTable(tableName2);
			DataLoader loader = new DataLoader(acc);
			try {
//				lineCounter = loader.loadFileToTable(fileName, delimiter,
//						tableName, accQual);
				lineCounter = loader.loadFileToTable(fileName2, delimiter2,
						tableName2, accQual);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (MutationsRejectedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TableNotFoundException e) {
				e.printStackTrace();
			}
		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		}
		System.out.println(lineCounter + " line(s) loaded. Done.");
	}

}
