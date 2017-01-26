package istc.bigdawg.accumulo;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import istc.bigdawg.LoggerSetupForTests;
import istc.bigdawg.migration.AtomicMigrationTest;

public class AccumuloConnectorTest {

	private static Logger log = Logger.getLogger(AccumuloConnectorTest.class);
	
	private Instance instance;
	
	private ClientConfiguration cc;
    private String username;
    private PasswordToken auth;
    
    private Connector conn;
	
	@Before
	public void setUp() throws Exception {
		
		LoggerSetupForTests.setTestLogging();
		
		cc = ClientConfiguration.loadDefault().withInstance("classdb55").withZkHosts("localhost:2333");//.withInstance("accumulo").withZkHosts("192.168.99.100:2181");//.withZkHosts("zookeeper.docker.local:2181"); // .withZkTimeout(timeout)
//        this.username = "bigdawg";
//        this.auth = new PasswordToken("bigdawg");
		this.username = "AccumuloUser";
        this.auth = new PasswordToken("bJsTTwyiMjbK%1PMEh2hioMK@");
		
        //scan -t bk0802_oTsampleDegree -e S0100
        
		instance = new ZooKeeperInstance(cc);
		conn = instance.getConnector(username, auth);
	}

//	@Test
	public void test() throws TableNotFoundException, MutationsRejectedException {
		String tR = "randomtable";
		
	    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
	            actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
	        expect.put(new Key("v2", "", "v5"), new Value("18".getBytes(StandardCharsets.UTF_8)));
//	        IteratorSetting RPlusIteratorSetting = new DynamicIteratorSetting(DEFAULT_COMBINER_PRIORITY, null)
//	            .append(MathTwoScalar.combinerSetting(1, null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LONG_OR_DOUBLE, false))
//	            .toIteratorSetting();
//		
//	    // first test writing different timestamps. Some can be same.
	    TestUtil.createTestTable(conn, tR);
//	    GraphuloUtil.applyIteratorSoft(RPlusIteratorSetting, conn.tableOperations(), tR);
	    {
	      BatchWriter bw = conn.createBatchWriter(tR, new BatchWriterConfig());
	      Key k = new Key("v2", "", "v5");
	      Value v = new Value("1".getBytes(StandardCharsets.UTF_8));
	      Mutation m;

	      for (int i = 0; i < 3; i++) {
	        m = new Mutation(k.getRowData().toArray());
//	        m.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(),
//	            k.getColumnVisibilityParsed(), 20+((int)(Math.random()*100)), v.get());
	        m.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(), k.getColumnVisibilityParsed(), i+20, v.get());
	        bw.addMutation(m);
//	        if (Math.random() < 0.2)
//	          bw.flush();
	      }
	      bw.flush();
	      bw.close();

	      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
	      scanner.setRanges(Collections.singleton(new Range()));
	      for (Map.Entry<Key, Value> entry : scanner) {
	        actual.put(entry.getKey(), entry.getValue());
	        log.debug("Entry "+actual.size()+": "+entry.getKey()+"    "+entry.getValue());
	      }
	      scanner.close();
	      Assert.assertEquals(expect, actual);
	    }
	}
	
	@Test
	public void test2() throws TableNotFoundException, MutationsRejectedException {
//		String tR = "testtable";
		String tR = "bk0802_oTsampleDegree";
		
	    Map<Key,Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
	    {

	      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
	      scanner.setRanges(Collections.singleton(new Range(null, new Key(new Text("S0100")))));
	      for (Map.Entry<Key, Value> entry : scanner) {
	        actual.put(entry.getKey(), entry.getValue());
	        log.debug("Entry "+actual.size()+": -"+entry.getKey()+"-    -"+entry.getValue());
	      }
	      scanner.close();
	    }
	}

}
