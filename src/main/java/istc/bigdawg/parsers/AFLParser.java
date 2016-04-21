package istc.bigdawg.parsers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import istc.bigdawg.extract.logical.SQLTableExpression;
import istc.bigdawg.plan.operators.Operator;
import istc.bigdawg.plan.operators.SeqScan;

public class AFLParser {
	
	private static Pattern opStarter	 	= null;
//	private static Pattern opEndAndBrackets = null;
	
	public static void listing() throws Exception {
		
		// reading all the SQL commands
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/SciDBParserTerms.csv"));
		StringBuffer opStringBuffer	  = new StringBuffer();
		String line 				  = bufferedReader.readLine();
		
		// get raw ops
		opStringBuffer.append("(?i)(\\b"+line+"\\(\\b");
		line = bufferedReader.readLine();
		do {
			 opStringBuffer.append("|\\b").append(line).append("\\(\\b");
			line = bufferedReader.readLine();
		} while (line != null);
		
		
		// finish ops
		opStringBuffer.append("|[,)\\[\\]<>]");
		opStringBuffer.append(")");
		
		opStarter = Pattern.compile(opStringBuffer.toString());
		bufferedReader.close();
		
		// get opEnd
//		opEndAndBrackets = Pattern.compile("[,)\\[\\]<>]");
	}
	
	public static String sig1(String input) throws Exception {
		if (opStarter == null) listing();
		
		StringBuffer stringBuffer	= new StringBuffer();
		Matcher matcher				= opStarter.matcher(input);
		
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
	
	
	public static Operator parsePlanTail(String query) throws Exception {
		
		if (opStarter == null) listing();
		
		Matcher start = opStarter.matcher(query);
		
		String currentOp = null;
		String currentToken = null;
		
		
		Stack<List<String>> rawFunctions = new Stack<>();
		Stack<Operator> children = new Stack<>();
		
		int lastStop = 0;
		boolean ignoreToken = false;
		int aggregateFunctionLevel = 0;
		
		if ((!start.find())||query.substring(start.start(), start.end()).matches("[,)\\[\\]<>]")) 
			throw new Exception("No AFL operator found: "+query);
		
		start.reset();
		
		while (start.find()) {
			
			currentToken = query.substring(start.start(), start.end());
			
			
			switch (currentToken) {
			
			
			case ",":
				
				
				if (ignoreToken) break;
				
				
				if (lastStop != start.start()) {
					System.out.println("--"+query.substring(lastStop, start.start()).trim()+"--comma last stop: "+lastStop+" comma startstar: "+start.start());
					rawFunctions.peek().add(query.substring(lastStop, start.start()).trim());
				} else {
					System.out.println("--"+"comma end"+"--");
					
					// ended with a function
					
					// pop and make TODO
					
					children.push(popAndMake(rawFunctions.pop(), children));
				}
				
				
				
				
				lastStop = start.end();
				break;
				
				
			case ")":
				
				
				// deal with aggregates
				if (aggregateFunctionLevel != 0) {
					aggregateFunctionLevel--;
					if (aggregateFunctionLevel == 0) {
						rawFunctions.peek().add(query.substring(lastStop, start.end()).trim());
						lastStop = start.end();
					}
					break;
				}
				
				
				
				if (lastStop != start.start()) {
					
					// ended with something that's not a function
					System.out.println("--"+query.substring(lastStop, start.start()).trim()+"--");
					rawFunctions.peek().add(query.substring(lastStop, start.start()).trim());
					
				} else {
					System.out.println("--"+"end"+"--");
					
					// ended with a function
					
					
					// pop and make TODO
					
					children.push(popAndMake(rawFunctions.pop(),children));
					
				}
				
					
				ignoreToken = false;
				lastStop = start.end();
				break;
			case "<":
			case "[":
				ignoreToken = true;
				lastStop = start.end();
				break;
			case ">":
				ignoreToken = false;
				break;
			case "]":
				ignoreToken = false;
				
				rawFunctions.peek().add(query.substring(lastStop, start.end()).trim());
				
				lastStop = start.end();
				break;
			case "min(":
			case "max(":
			case "median(":
			case "count(":
			case "avg(":
			case "datetime(":
			// maybe some others functions of aggregates
				ignoreToken = true;
				
				if (aggregateFunctionLevel == 0)
					lastStop = start.end();
				
				aggregateFunctionLevel++;
				
				break;
			default:
				
				currentOp = query.substring(start.start(), start.end()-1);
				System.out.println(query.substring(start.start(), start.end()-1)+"--");
				
				List<String> p = new ArrayList<>();
				p.add(currentOp);
				
				// add placeholder for parent function
				if (!rawFunctions.isEmpty()) {
					rawFunctions.peek().add("__placeHolder");
				}
				
				rawFunctions.push(p);
				
				
				lastStop = start.end();
				break;		
			}
			
		}
		
		
		
		Operator op = null;
		
		return op;
	}
	
	private static Operator popAndMake(List<String> functionArray, Stack<Operator> children) throws Exception {
		
		Operator out = null;
		
		
		switch (functionArray.get(0).toLowerCase()) {
		case "project":
			
			if (functionArray.get(1).equals("__placeHolder")){
				List<Operator> offsprings = new ArrayList<>();
				offsprings.add(children.pop());
				
			} else {
				// add a new seqscan
				SQLTableExpression t = new SQLTableExpression();
				t.setName(functionArray.get(1));
				return new SeqScan(new HashMap<>(), functionArray.subList(2, functionArray.size()), null, t);
			}
			
			
			break;
		case "cross_join":
			break;
		default:
			throw new Exception("Unsupported AFL operators");
		}
		
		
		return out;
	}
}
