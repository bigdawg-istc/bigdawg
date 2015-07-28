package istc.bigdawg.stream;

import istc.bigdawg.exceptions.AlertException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class StreamDAO {

	public class ClientAlert {
		public int clientAlertID;
		public int dbAlertID;
		public boolean push;
		public boolean oneTime;
		public boolean active;
		public List<String> unseenPulls;
		public Date lastPullTime;
		public String pushURL;
		public Date created;

		public ClientAlert(int clientAlertID, int dbAlertID, boolean push,
				boolean oneTime, boolean active, boolean pulled, String pushURL) {
			super();
			this.clientAlertID = clientAlertID;
			this.dbAlertID = dbAlertID;
			this.push = push;
			this.oneTime = oneTime;
			this.active = active;
			this.unseenPulls = new ArrayList<String>();
			this.pushURL = pushURL;
			this.created = new Date();
			this.lastPullTime = null;
		}

	}

	public class DBAlert {
		public int dbAlertID;
		public String stroredProc;
		public boolean oneTime;
		public String receiveURL;
		public Date created;

		public DBAlert(int dbAlertID, String stroredProc, boolean oneTime,
				String receiveURL) {
			super();
			this.dbAlertID = dbAlertID;
			this.stroredProc = stroredProc;
			this.oneTime = oneTime;
			this.receiveURL = receiveURL;
			this.created = new Date();
		}

	}
	public class ClientNotifyEvent {
		public int alertID;
		public int clientAlertID;
		public String body;
	}

	public class AlertEvent {
		public int alertID;
		public int dbAlertID;
		public Date ts;
		public String body;

		public AlertEvent(int alertID, int dbAlertID, String body) {
			super();
			this.alertID = alertID;
			this.dbAlertID = dbAlertID;
			this.body = body;
			this.ts = new Date();
		}

	}
	
	public class PushNotify {
		public String url;
		public String body;
		public PushNotify(String url, String body) {
			super();
			this.url = url;
			this.body = body;
		}		
	}

	public abstract String checkForNewPull(int clientAlertId);

	public abstract DBAlert createOrGetDBAlert(String storedProc,
			boolean oneTime);

	public abstract ClientAlert createClientAlert(int dbAlertId, boolean push,
			boolean oneTime, String pushURL) throws AlertException;

	public abstract List<AlertEvent> addAlertEvent(int dbAlertId, String body);

	public abstract List<PushNotify> updatePullsAndGetPushURLS(
			List<AlertEvent> clientAlertIds) throws AlertException;

	public abstract List<DBAlert> getDBAlerts();

	public abstract List<ClientAlert> getActiveClientAlerts();

	public abstract void reset();

}
