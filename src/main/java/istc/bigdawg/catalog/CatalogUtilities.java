package istc.bigdawg.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.exceptions.BigDawgCatalogException;

public class CatalogUtilities {
	
	public static void checkConnection(Catalog cc) throws BigDawgCatalogException, SQLException {
		if (!cc.isInitiated()) 
			throw new BigDawgCatalogException("[ERROR] BigDAWG: disconnected/uninitialized catalog.");
		if (!cc.isConnected()) {
			CatalogInitiator.connect(cc, cc.getLastURL(), cc.getLastUsername(), cc.getLastPassword());
		}
	}
	
	public static void checkLength(String s1, int len1) throws BigDawgCatalogException {
		if (s1.length() > len1) 
			throw new BigDawgCatalogException("[ERROR] BigDAWG: invalid length for \"" + s1 + "\";");
	}
	
	////////////////////////////////////
	// CATALOG PARSER UTILITIES BELOW //
	////////////////////////////////////
	
	private enum CatalogParsingPhases {unstarted, command, changePhase, view, SQL};
	
	private static StringBuilder sb, temp;
	private static CatalogParsingPhases phase;
	private static List<String> extraction, viewArgs;
	private static int parenlevel;
	
	private static boolean isCatalogSQL(String input) {
		String comp = input.toLowerCase();
		return comp.equals("select") || comp.equals("insert") || comp.equals("update") || comp.equals("delete");
	}
	
	private static void changePhase() {
		String comp = extraction.get(extraction.size() - 1).toString();
		if (isCatalogSQL(comp))
			phase = CatalogParsingPhases.SQL;
		else 
			phase = CatalogParsingPhases.view;
	}
	
	/*
	 * Supported commands:
	 * 1. View -- name, optionally followed by fields
	 * 2. Semicolon separated SQL commands -- SELECT, INSERT, UPDATE, DELETE
	 */
	public static List<String> parseCatalogQuery(String input) throws Exception{
		sb = new StringBuilder(input);
		temp = new StringBuilder();
		extraction = new ArrayList<>();
		viewArgs = new ArrayList<>();
		phase = CatalogParsingPhases.unstarted;
		parenlevel = 0;
		
		while (sb.length() != 0) {
			processSingleToken();
			sb.deleteCharAt(0);
			if (phase == CatalogParsingPhases.changePhase)
				changePhase();
		}
		// post-processing
		if (temp.length() > 0) {
			if (phase == CatalogParsingPhases.view) {
				viewArgs.add(temp.toString());
				extraction.add(String.join(", ", viewArgs));
			} else if (phase == CatalogParsingPhases.SQL) 
				extraction.add(temp.toString());
			else if (phase == CatalogParsingPhases.command) {
				extraction.add(temp.toString());
				extraction.add("");
			}
		}
			
		if (parenlevel != 0)
			throw new Exception("Ill-formed CatalogQuery: "+input+"\n");
		
		return extraction;
	}
	
	/*
	 * Procedures
	 * 1. skip the 'bdcatalog(' section, or unstarted phase;
	 * 2. parse the command
	 * 3. if command is SQL, keep appending until it is semicolon or unquoted-parenthesized end;
	 * 4. if command is not SQL, save each subsequent word to 'viewArgs' until it is semicolon or unquoted-parenthesized end;
	 * 5. after each semi-colon, restore status to 'command'
	 * 6. program terminates when parenthesis level is zero.
	 */
	private static void processSingleToken() throws Exception {
		boolean quotedEh = false;
		char token = sb.charAt(0);
		
		if (quotedEh) {
			switch (token) {
			case '\'':
				quotedEh = !quotedEh;
			case '(':
			case ')':
			case ';':
			case ' ':
			case '\t':
			default:
				temp.append(token);
			}
			return;
		}
		
		// !quotedEh
		switch (token) {
		case ';':
			if (phase == CatalogParsingPhases.view) {
				if (temp.length() > 0) viewArgs.add(temp.toString());
				extraction.add(String.join(", ", viewArgs));
				viewArgs = new ArrayList<>();
			} else if (phase == CatalogParsingPhases.SQL) {
				extraction.add(temp.toString()); // temp should always be > 0
			} else if (phase == CatalogParsingPhases.command) {
				if (temp.length() > 0) extraction.add(temp.toString());
				extraction.add("");
				temp = new StringBuilder();
				break;
			} else
				throw new Exception("Incorrect phase: "+phase.name()+"\n");
			
			temp = new StringBuilder();
			phase = CatalogParsingPhases.command;
			break;
		case ' ':
		case '\t':
			if (temp.length() == 0 && phase != CatalogParsingPhases.SQL)
				break;
			if (phase == CatalogParsingPhases.command) {
				extraction.add(temp.toString());
				temp = new StringBuilder();
				phase = CatalogParsingPhases.changePhase;
			} else if (phase == CatalogParsingPhases.SQL) {
				temp.append(token);
			} else if (phase == CatalogParsingPhases.view) {
				viewArgs.add(temp.toString());
				temp = new StringBuilder();
			} 
			// else it's just unstarted or changePhase
			break;
		case '(':
			parenlevel++;
			if (phase == CatalogParsingPhases.unstarted) 
				phase = CatalogParsingPhases.command;
			break;
		case ')':
			parenlevel--;
			break;
		case '\'':
			quotedEh = !quotedEh;			
		default:
			if (phase != CatalogParsingPhases.unstarted)
				temp.append(token);
			break;
		}
	}
	
	private static String processCatalogResultSet(ResultSet rs) throws Exception {
		StringBuilder sb = new StringBuilder();
		int cCount = rs.getMetaData().getColumnCount();
		
		sb.append(rs.getMetaData().getColumnName(1));
		if (cCount > 1)
			for (int i = 2; i <= cCount ; i++) 
				sb.append('\t').append(rs.getMetaData().getColumnName(i));
		sb.append('\n');
		
		while (rs.next()) {
			sb.append(rs.getObject(1));
			if (cCount > 1)
				for (int i = 2; i <= cCount ; i++) 
					sb.append('\t').append(rs.getObject(i));
			sb.append('\n');
		}
		return sb.toString();
	}
	
	public static String catalogQueryResult (List<String> parsedResult) throws Exception {
		Catalog cc = CatalogInstance.INSTANCE.getCatalog();
		CatalogUtilities.checkConnection(cc);
		StringBuilder result = new StringBuilder();
		String interm;
		ResultSet rs;
		
		for (int i = 0; i < parsedResult.size(); i = i+2) {
			System.out.printf("catalog query: %s %s\n", parsedResult.get(i), parsedResult.get(i+1));
			
			if (isCatalogSQL(parsedResult.get(i))) {
				rs = cc.execRet(String.format("%s %s", parsedResult.get(i), parsedResult.get(i+1)));
			} else {
				String columns = parsedResult.get(i+1).length() > 0 ? parsedResult.get(i+1) : "*";
				String tablename = parsedResult.get(i).toLowerCase().startsWith("catalog.") ? parsedResult.get(i).toLowerCase() : ("catalog."+parsedResult.get(i).toLowerCase());
				rs = cc.execRet(String.format("SELECT %s FROM %s", columns, tablename));
			}
			interm = processCatalogResultSet(rs);
			rs.close();
			System.out.printf("New catalog query result: %s\n", interm);
			if (i > 0) result.append('\n');
			result.append(interm);
		}
		
		return result.toString();
	}
}