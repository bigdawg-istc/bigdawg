package istc.bigdawg.stream;

import istc.bigdawg.exceptions.AlertException;

import java.util.Date;
import java.util.List;

public abstract class StreamDAO {

	public class ClientAlert {
		public int clientAlertID;
		public int dbAlertID;
		public boolean push;
		public boolean oneTime;
		public boolean active;
		public boolean unseenPull;
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
			this.unseenPull = pulled;
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

	public abstract boolean checkForNewPull(int clientAlertId);

	public abstract DBAlert createOrGetDBAlert(String storedProc,
			boolean oneTime);

	public abstract ClientAlert createClientAlert(int dbAlertId, boolean push,
			boolean oneTime, String pushURL) throws AlertException;

	public abstract List<Integer> addAlertEvent(int dbAlertId, String body);

	public abstract List<String> updatePullsAndGetPushURLS(
			List<Integer> clientAlertIds) throws AlertException;

	public abstract List<DBAlert> getDBAlerts();

	public abstract List<ClientAlert> getActiveClientAlerts();

	public abstract void reset();

}
