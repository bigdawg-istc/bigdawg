package teddy.bigdawg.packages;

import java.util.ArrayList;

public class QueriesAndPerformanceInformation {
	public ArrayList<ArrayList<String>> qList;
	public ArrayList<Object> pInfo;
	public QueriesAndPerformanceInformation(ArrayList<ArrayList<String>> q, ArrayList<Object> p) {
		qList = q;
		pInfo = p;
	}
	
	public String toString() {
		return "{"+qList.toString()+" | "+pInfo.toString()+"}";
	}
}
