/**
 * 
 */
package istc.bigdawg.accumulo;

import istc.bigdawg.exceptions.AccumuloBigDawgException;

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
			final String delimiter, final String tableName,
			final AccumuloRowQualifier accQual) throws FileNotFoundException,
			IOException, MutationsRejectedException, TableNotFoundException {
		System.out.println("Loading file: "+inputFileName+" to table: "+tableName);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileName));
		BatchWriterConfig config = new BatchWriterConfig();
		// bytes available to batchwriter for buffering mutations
		config.setMaxMemory(100000L);
		BatchWriter writer = acc.getConnector().createBatchWriter(tableName,
				config);
		// ColumnVisibility colVis = new ColumnVisibility();
		int lineCounter = 0;
		for (String line; (line = bufferedReader.readLine()) != null;) {
			String[] tokens = line.split(delimiter);
			AccumuloRowQualifier.AccumuloRow row = accQual
					.getAccumuloRow(tokens);
			Mutation mutation = new Mutation(row.getRowId());
			// mutation.put(row.getColFam(), row.getColQual(), colVis,
			// row.getValue());
			// code below for the case without the column visibility
			mutation.put(row.getColFam(), row.getColQual(), row.getValue());
			writer.addMutation(mutation);
			++lineCounter;
		}
		writer.close();
		bufferedReader.close();
		System.out.println("loaded: " + lineCounter + " lines to table "
				+ tableName + " from file" + inputFileName);
		return lineCounter;
	}

	/**
	 * @param args
	 * @throws AccumuloBigDawgException 
	 */
	public static void main(String[] args) throws AccumuloBigDawgException {

		AccumuloRowQualifier accQual = new AccumuloRowQualifier(true, false,
				true, true);

		AccumuloInstance acc = null;
		try {
			// acc=new AccumuloInstance(instanceName,zooKeepers,userName,pass);
			acc = AccumuloInstance.getInstance();

			DataLoader loader = new DataLoader(acc);
			try {
				String tableName = "note_events_TedgeDeg";
				String fileName = "/home/adam/Chicago/mimic2_data/accumulo/note_events_TedgeDeg.csv";
				String delimiter = ";";
				acc.createTable(tableName);
				loader.loadFileToTable(fileName, delimiter, tableName, accQual);

				String tableName2 = "note_events_TedgeTxt";
				String fileName2 = "/home/adam/Chicago/mimic2_data/accumulo/note_events_TedgeTxt.csv";
				String delimiter2 = ";;";
				acc.createTable(tableName2);
				loader.loadFileToTable(fileName2, delimiter2, tableName2,
						accQual);

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
	}

}
