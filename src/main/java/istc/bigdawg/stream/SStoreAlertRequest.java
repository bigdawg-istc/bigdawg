package istc.bigdawg.stream;

import java.util.List;

public class SStoreAlertRequest {

	public List<String> data;
	public String error;
	public String success;
	public List<String> getData() {
		return data;
	}
	public void setData(List<String> data) {
		this.data = data;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public String getSuccess() {
		return success;
	}
	public void setSuccess(String success) {
		this.success = success;
	}
	
	public class Event{
		public int PATIENT_ID;
		public int TS;
		public String SIGNAME;
		public int INTERVAL;
		public String ALERT_MSG;
		public String ACTION_MSG;
	}

}
