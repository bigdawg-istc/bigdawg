package istc.bigdawg.islands.Myria;

import java.util.ArrayList;
import java.util.List;

import istc.bigdawg.exceptions.BigDawgException;

public class MyriaQueryParser {
	
	private StringBuilder inputSB;
	private StringBuilder tempSB;
	private boolean isLookingForOutputToken;
	
	public MyriaQueryParser() {
		inputSB = new StringBuilder();
		tempSB = new StringBuilder();
		isLookingForOutputToken = false;
	}
	
	public List<String> parse(String input) throws BigDawgException {
		List<String> result = new ArrayList<>();
		
		inputSB.append(input);
		while (inputSB.length() > 0) {
			processSingleFragment(result);
			inputSB.deleteCharAt(0);
		}
		
		if (result.size() > 1) {
			inputSB.append(result.get(result.size()-1));
			while (inputSB.length() > 0) {
				processName(result);
				inputSB.deleteCharAt(0);
			}
		}
		System.out.printf("StringBuilder from MyriaQueryParser: %s; result: %s\n", inputSB, result);
		if (result.size() > 1) {
			List<String> newResult = new ArrayList<>();
			StringBuilder sb = new StringBuilder();
			newResult.add(result.get(0));
			newResult.add(result.get(1));
			for (int i = 2; i < result.size() ; i++) sb.append(result.get(i));
			newResult.add(sb.toString());
			return newResult;
		}
		return result;
	}
	
	private void processSingleFragment(List<String> result) {
		char token = inputSB.charAt(0);
		switch (token) {
		case ';':
			tempSB.append(token);
			result.add(tempSB.toString().trim());
			tempSB = new StringBuilder();
			break;
		default:
			tempSB.append(token);
			break;
		}
	}
	
	private void processName(List<String> result) {
		char token = inputSB.charAt(0);
		
		if (!isLookingForOutputToken) 
			if (token == ',') {
				isLookingForOutputToken = true;
				return;
			}
			else return;
		
		switch (token) {
		case ' ':
		case '\t':
			break;
		case ')':
			result.add(1, tempSB.toString());
			break;
		default:
			tempSB.append(token);
			break;
		}
	}
}
