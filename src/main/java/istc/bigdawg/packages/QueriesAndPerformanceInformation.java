package istc.bigdawg.packages;

import java.util.List;

public class QueriesAndPerformanceInformation {
	public List<List<String>> qList;
	public List<Object> pInfo;
	public QueriesAndPerformanceInformation(List<List<String>> q, List<Object> p) {
		qList = q;
		pInfo = p;
	}
	
	public String toString() {
		return "{"+qList.toString()+" | "+pInfo.toString()+"}";
	}
}
