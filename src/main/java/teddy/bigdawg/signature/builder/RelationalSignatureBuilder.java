package teddy.bigdawg.signature.builder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import teddy.bigdawg.catalog.Catalog;
import teddy.bigdawg.catalog.CatalogViewer;

public class RelationalSignatureBuilder {
	
	private static Pattern opPattern  	= null;
	private static Pattern objPattern	= null;
	private static Pattern litPattern	= null;
	
	public static void listing() throws Exception {
		
		// reading all the SQL commands
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/PostgresParserTerms.csv"));
		StringBuffer opStringBuffer	  = new StringBuffer();
		StringBuffer objStringBuffer  = new StringBuffer();
		String line 				  = bufferedReader.readLine();
		
		// get raw ops
		opStringBuffer.append("\\b"+line+"\\b");
		line = bufferedReader.readLine();
		do {
			 opStringBuffer.append("|\\b").append(line).append("\\b");
			line = bufferedReader.readLine();
		} while(line != null);
		
		// get tokens, so catalog can filter non-ops
		objStringBuffer.append("(?i)(?!(").append(opStringBuffer).append("|\\bby\\b|\\bas\\b))\\b\\w+\\b");
		
		// finish ops
		opStringBuffer.insert(0, "(?i)(").append(")");
		
		opPattern  = Pattern.compile(opStringBuffer.toString());
		objPattern = Pattern.compile(objStringBuffer.toString());
		litPattern = Pattern.compile("((?<!([a-zA-Z_]{1,10}[0-9.]{0,10}))(([0-9]*[.]?[0-9]+)))|([-][ ]*[0-9]*[.]?[0-9]+)|'[^']*'|(?i)(\\bnull\\b|\\btrue\\b|\\bfalse\\b)");
		bufferedReader.close();
	}
	
	
	
	public static String sig1(String input) throws Exception {
		if (opPattern == null) listing();
		
		StringBuffer stringBuffer	= new StringBuffer();
		Matcher matcher				= opPattern.matcher(input);
		
		try {
			matcher.find();
			stringBuffer.append(input.substring(matcher.start(), matcher.end()));
			while (matcher.find()) {
				stringBuffer.append("\t").append(input.substring(matcher.start(), matcher.end()));
			}
			return stringBuffer.toString();
			
		} catch (IllegalStateException e) {
			return "";
		}
	}
	
	public static String sig2(Catalog cc, String input) throws Exception {
		if (objPattern == null) listing();
		
		StringBuffer stringBuffer	= new StringBuffer();
		Matcher matcher				= objPattern.matcher(input);
		StringBuffer dawgtags  		= new StringBuffer();
		Pattern tagPattern			= Pattern.compile("BIGDAWGTAG_[0-9_]+");
		Matcher tagMatcher			= tagPattern.matcher(input);
		
		try {
			matcher.find();
			stringBuffer.append(input.substring(matcher.start(), matcher.end()));
			while (matcher.find()) {
				stringBuffer.append(",").append(input.substring(matcher.start(), matcher.end()));
			}
			if (tagMatcher.find())
				dawgtags.append(input.substring(tagMatcher.start(), tagMatcher.end()));
			while (tagMatcher.find())
				dawgtags.append("\t"+input.substring(tagMatcher.start(), tagMatcher.end()));
		} catch (IllegalStateException e) {
			return "";
		}
		
		return stringBuffer.append("\t").append(dawgtags).toString();
		/*
		String result = CatalogViewer.getObjectsFromList(cc, stringBuffer.toString());
		if (result.length() == 0) {
			return dawgtags.toString();
		} else {
			return result.concat("\t"+dawgtags.toString());
		}
		*/
	}
	
	public static String sig3(String input) throws Exception {
		if (litPattern == null) listing();
		
		StringBuffer stringBuffer	= new StringBuffer();
		Matcher matcher				= litPattern.matcher(input);
		
		try {
			matcher.find();
			stringBuffer.append(input.substring(matcher.start(), matcher.end()));
			while (matcher.find()) {
				stringBuffer.append("\t").append(input.substring(matcher.start(), matcher.end()));
			}
			return stringBuffer.toString().replace(" ", "");
			
		} catch (IllegalStateException e) {
			return "";
		}
	}

}
