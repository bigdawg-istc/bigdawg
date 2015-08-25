/**
 * 
 */
package istc.bigdawg.accumulo;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.core.Response;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.ObjectMapper;

import istc.bigdawg.BDConstants;
import istc.bigdawg.BDConstants.Shim;
import istc.bigdawg.exceptions.ShellScriptException;
import istc.bigdawg.properties.BigDawgConfigProperties;
import istc.bigdawg.query.DBHandler;
import istc.bigdawg.query.QueryResponseTupleString;
import istc.bigdawg.utils.Constants;
import istc.bigdawg.utils.RunShell;

/**
 * @author adam
 * 
 */
public class AccumuloHandler implements DBHandler {

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#executeQuery(java.lang.String)
	 */
	@Override
	public Response executeQuery(String queryString) {
		String[] params = queryString.split(" ");
		String database = params[0];
		String table = params[1];
		String query = params[2];
		System.out.println("databse: " + database + " table: " + table
				+ " query: " + query);
		try {
			return Response.status(200)
					.entity(executeAccumuloShellScript(database, table, query))
					.build();
		} catch (IOException | InterruptedException | ShellScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private String executeAccumuloShellScript(String database, String table,
			String query) throws IOException, InterruptedException,
			ShellScriptException {
		String accumuloScriptPath = BigDawgConfigProperties.INSTANCE
				.getAccumuloShellScript();
		System.out.println("accumuloScriptPath: " + accumuloScriptPath);
		InputStream scriptResultInStream = RunShell.run(accumuloScriptPath,
				database, table, query);
		String scriptResult = IOUtils.toString(scriptResultInStream,
				Constants.ENCODING);
		System.out.println("Accumulo script result: " + scriptResult);
		QueryResponseTupleString resp = new QueryResponseTupleString("OK", 200,
				scriptResult, 1, 1, AccumuloInstance.schema,
				AccumuloInstance.types, new Timestamp(0));
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(resp).replace("\\", "");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see istc.bigdawg.query.DBHandler#getShim()
	 */
	@Override
	public Shim getShim() {
		return BDConstants.Shim.ACCUMULOTEXT;
	}

	private String executeQueryAccumuloPure(String table)
			throws TableNotFoundException, AccumuloException,
			AccumuloSecurityException, IOException {
		// specify which visibilities we are allowed to see
		// Authorizations auths = new Authorizations("public");
		Authorizations auths = new Authorizations();
		AccumuloInstance accInst = AccumuloInstance.getInstance();
		Connector conn = accInst.getConnector();
		conn.securityOperations().changeUserAuthorizations(
				accInst.getUsername(), auths);
		Scanner scan = conn.createScanner(table, auths);
		scan.setRange(new Range("", null));
		scan.fetchColumnFamily(new Text(""));
		List<List<String>> allRows = new ArrayList<List<String>>();
		for (Entry<Key, Value> entry : scan) {
			// System.out.println(entry.getKey());
			// System.out.println(entry.getValue());
			List<String> oneRow = new ArrayList<String>();
			Text rowIdResult = entry.getKey().getRow();
			Text colFamResult = entry.getKey().getColumnFamily();
			Text colKeyResult = entry.getKey().getColumnQualifier();
			Text visibility = entry.getKey().getColumnVisibility();
			Value valueResult = entry.getValue();
			oneRow.add(rowIdResult.toString());
			oneRow.add(colFamResult.toString());
			oneRow.add(colKeyResult.toString());
			oneRow.add(visibility.toString());
			oneRow.add(valueResult.toString());
			allRows.add(oneRow);
		}
		ObjectMapper mapper = new ObjectMapper();
		String allRowsString = mapper.writeValueAsString(allRows);
		QueryResponseTupleString resp = new QueryResponseTupleString("OK", 200,
				allRowsString, 1, 1, AccumuloInstance.fullSchema,
				AccumuloInstance.fullTypes, new Timestamp(0));
		return mapper.writeValueAsString(resp);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String accumuloScript;
		try {
			accumuloScript = new AccumuloHandler().executeAccumuloShellScript(
					"database", "table", "query");
			System.out.println(accumuloScript);
		} catch (IOException | InterruptedException | ShellScriptException e) {
			e.printStackTrace();
		}

	}

}
