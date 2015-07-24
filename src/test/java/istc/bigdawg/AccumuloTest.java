/**
 * 
 */
package istc.bigdawg;

import istc.bigdawg.accumulo.AccumuloInstance;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.apache.accumulo.core.client.Scanner;

/**
 * @author Adam Dziedzic
 * 
 */
public class AccumuloTest {

	@Test
	public void testMockInstance() {
		Instance instance = new MockInstance();
		//Instance instance = AccumuloInstance.getMiniCluster();
		try {
			Connector conn = instance.getConnector("root",
					new PasswordToken(""));
			TableOperations tabOp = conn.tableOperations();
			try {
				tabOp.create("testTable");
			} catch (TableExistsException e1) {
				System.out.println("Table exception, table creation failed.");
				e1.printStackTrace();
			}
			Text rowID = new Text("row1");
			Text colFam = new Text("myColFam");
			Text colQual = new Text("myColQual");
			ColumnVisibility colVis = new ColumnVisibility("public");
			long timestamp = System.currentTimeMillis();

			Value value = new Value("myValue".getBytes());

			Mutation mutation = new Mutation(rowID);
			mutation.put(colFam, colQual, colVis, timestamp, value);
			// BatchWriterConfig has reasonable defaults
			BatchWriterConfig config = new BatchWriterConfig();
			config.setMaxMemory(10000L); // bytes available to batchwriter for
											// buffering mutations
			BatchWriter writer;
			try {
				writer = conn.createBatchWriter("testTable", config);
				writer.addMutation(mutation);
				writer.close();
			} catch (TableNotFoundException e) {
				System.out
						.println("Problem with creating BatchWrite, table not foun.");
				e.printStackTrace();
			}
			Authorizations auths = new Authorizations("public");
			try {
				Scanner scan = conn.createScanner("testTable", auths);
				scan.fetchColumnFamily(new Text("myColFam"));

				for (Entry<Key, Value> entry : scan) {
					Key keyResultKey = entry.getKey();
					Text rowIdResult = entry.getKey().getRow();
					Text colFamResult = entry.getKey().getColumnFamily();
					Text colKeyResult = entry.getKey().getColumnQualifier();
					Value valueResult = entry.getValue();
					System.out.println("_Key_:" + keyResultKey + " _Row_:"
							+ rowIdResult + " _ColFam_:" + colFamResult
							+ " _ColQual_:" + colKeyResult + " _Value_:"
							+ valueResult);
				}
			} catch (TableNotFoundException e) {
				System.out.println("Table for scanner not found!");
				e.printStackTrace();
			}

		} catch (AccumuloException e) {
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			e.printStackTrace();
		}
	}
}
