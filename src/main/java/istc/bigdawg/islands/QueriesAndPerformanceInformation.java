package istc.bigdawg.islands;

import java.util.List;

public class QueriesAndPerformanceInformation {
	public List<String> qList;
	public List<Long> pInfo;
	public QueriesAndPerformanceInformation(List<String> q, List<Long> p) {
		qList = q;
		pInfo = p;
	}
	
	public String toString() {
		return "{"+qList.toString()+" | "+pInfo.toString()+"}";
	}
}
