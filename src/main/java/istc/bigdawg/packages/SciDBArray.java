package istc.bigdawg.packages;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.reflect.macros.Aliases;

public class SciDBArray {
	
	private static Pattern lSchemaAttributes = Pattern.compile("^<[-:,.@\\w_ ]+>");
	private static Pattern lDimensions = Pattern.compile("=[*-0-9]+:[*0-9]+,[0-9]+,[0-9]+,?");
	private static Pattern lSchemaDimensions = Pattern.compile("\\[[-:,@*=\\w_ ]+\\]$");
	
	private Map<String, String> attributes;
	private Map<String, String> dimensions;
	private Set<String> schemaAliases;
	private String schemaAlias = null;
	
	
	public SciDBArray (String schema) {
		this();
		
		attributes.putAll(parseAttributes(schema));
		dimensions.putAll(parseDimensions(schema));
	
	}
	
	public SciDBArray() {
		attributes = new LinkedHashMap<>();
		dimensions = new LinkedHashMap<>();
		schemaAliases = new LinkedHashSet<>();
	}
	
	public static Map<String, String> parseAttributes(String wholeSchema) {
		Matcher a = lSchemaAttributes.matcher(wholeSchema);
		if (a.find()) {
			Map<String, String> outputs = new LinkedHashMap<>();
			List<String> attribs = Arrays.asList(wholeSchema.substring(a.start()+1, a.end()-1).split(","));
			
			for (String s : attribs) {
				String[] l = s.split(":");
				outputs.put(l[0], l[1]);
			}
			
			return outputs;
		} else {
			System.out.println ("parseAttribute: Not a valid schema: " + wholeSchema);
			return null;
		}
	}
	
	public static Map<String, String> parseDimensions(String wholeSchema) {
		Matcher di = lSchemaDimensions.matcher(wholeSchema);
		if (di.find()) {
			Map<String, String> outputs = new LinkedHashMap<>();
			
			String dims = wholeSchema.substring(di.start(), di.end());
			dims = dims.substring(1, dims.length()-1);
			Matcher d = lDimensions.matcher(dims);
			
			int lastStop = 0;
			
			while (d.find()) {
				
				int end = d.end();
				if (dims.charAt(d.end()-1) == ',')
					end--;
				
				outputs.put(dims.substring(lastStop, d.start()), 
						dims.substring(d.start()+1, end));
				
				
				lastStop = d.end();
			}
			
			return outputs;
					
		} else {
			System.out.println ("parseAttribute: Not a valid schema: " + wholeSchema);
			return null;
		}
	}
	
	public String getSchemaString() {
		
		if (attributes.isEmpty() || dimensions.isEmpty()) return "";
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("<");
		boolean started = false;
		for (String aName : attributes.keySet()) {
			if (started) 
				sb.append(',').append(aName).append(':').append(attributes.get(aName));
			else {
				sb.append(aName).append(':').append(attributes.get(aName));
				started = true;
			}
		}
		sb.append('>').append('[');
		started = false;
		for (String aName : dimensions.keySet()) {
//			List<String> props = dimensions.get(aName);
			if (started) sb.append(',');
			else started = true;
			sb.append(aName).append('=').append(dimensions.get(aName));
		}
		sb.append(']');
		
		return sb.toString();
		
	}
	
	
	public void fixDimensionStrings() {
		
		
//		if (schemaAliases.isEmpty()) {
//			System.out.println("\n\n\nname: "+this.schemaAlias+"\n\n\n");
//			return;
//		}
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("((, )?(");
		
		boolean started = false;
		for (String s : schemaAliases) {
			if (started) sb.append('|');
			else started = true;
			
			sb.append(s);
			
		}
		
		sb.append("))*$");
		
		Pattern p = Pattern.compile(sb.toString());
		
		
		Map<String, String> substitute = new LinkedHashMap<>();
		
		for (String s : dimensions.keySet()) {
			
			Matcher d = p.matcher(s);
			
			if (d.find()) {
				substitute.put(s.substring(0,d.start()), dimensions.get(s));
				
//				System.out.println("Substitude String:: "+s.substring(0, d.start()));
//				System.out.println("Whole string:: "+s);
			} else {
				System.out.println("Cannot find: "+s);
				break;
			}
		}
		
		dimensions.clear();
		dimensions.putAll(substitute);
		
//		System.out.println("Pattern string: "+sb.toString());
		
	}
	
	
	
	public Map<String, String> getDimensions() throws Exception {
		return dimensions;
	}
	
	public Map<String, String> getAttributes() throws Exception {
		return attributes;
	}
	
	public Set<String> getAllSchemaAliases() {
		return schemaAliases;
	}
	
	public String getAlias() {
		return schemaAlias;
	}
	
	public void setAlias(String alias) {
		schemaAlias = new String (alias);
	}
	
	public void addSchemaAliases(Set<String> aliases) {
		schemaAliases.addAll(aliases)
;	}
}
