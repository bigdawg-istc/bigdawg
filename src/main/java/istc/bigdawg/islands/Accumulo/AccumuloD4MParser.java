package istc.bigdawg.islands.Accumulo;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.exceptions.BigDawgException;

public class AccumuloD4MParser {
	
	static enum State {
		hostname, query, filename
	}
	
	private StringBuilder inputSB;
	StringBuilder tempSB;
	private boolean isParenthesized;
	private boolean isQuoting;
	private State engineState;
	
	public AccumuloD4MParser() {
		inputSB = new StringBuilder();
		tempSB = new StringBuilder();
		isParenthesized = false;
		isQuoting = false;
		engineState = State.hostname;
	}
	
	public List<String> parse(String input) throws BigDawgException {
		List<String> result = new ArrayList<>();
		
		inputSB.append(input);
		
		while (inputSB.length() > 0) {
			switch (engineState) {
			case hostname:
				processSingleToken(result);
				break;
			case query:
				processQuery(result);
				break;
			case filename:
				processSingleToken(result);
				break;
			}
			
			inputSB.deleteCharAt(0);
		}
		result.add(tempSB.toString());
		
		System.out.printf("StringBuilder from AccumuloD4MParser: %s; result: %s\n", inputSB, result);
		return result;
	}
	
	private void processSingleToken(List<String> result) {
		char token = inputSB.charAt(0);
		switch (token) {
		case ' ':
		case '\t':
			break;
		case ',':
			result.add(tempSB.toString());
			tempSB = new StringBuilder();
			if (engineState == State.hostname) engineState = State.query;
			break;
		default:
			tempSB.append(token);
			break;
		}
	}
	
	private void processQuery(List<String> result) {
		char token = inputSB.charAt(0);
		switch (token) {
		case ' ':
		case '\t':
			if (isQuoting) tempSB.append(token);
			break;
		case '\'':
			tempSB.append('^').append('^');
//			tempSB.append('\'');
			isQuoting = (!isQuoting);
			break;
		case '(':
			result.add(tempSB.toString());
			tempSB = new StringBuilder();
//			tempSB.append('"');
			tempSB.append('@');
			tempSB.append(token);
			isParenthesized = true;
			break;
		case ')':
			tempSB.append(token);
//			tempSB.append('"');
			tempSB.append('@');
			isParenthesized = false;
			break;
		case ',':
			if (isParenthesized) {
				tempSB.append(token);
				break;
			}
			result.add(tempSB.toString());
			tempSB = new StringBuilder();
			break;
		default:
			tempSB.append(token);
			break;
		}
	}
}
