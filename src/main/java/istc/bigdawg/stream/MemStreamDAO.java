package istc.bigdawg.stream;

import istc.bigdawg.exceptions.AlertException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author aelmore
 * A dead simple in memory class for storing stream data objects.
 */
public class MemStreamDAO extends StreamDAO {
	List<ClientAlert> clientAlerts;
	List<DBAlert> dbAlerts;
	List<AlertEvent> events;
	AtomicInteger clientCounter = new AtomicInteger();
	AtomicInteger dbCounter = new AtomicInteger();
	AtomicInteger eventCounter = new AtomicInteger();
	
	public MemStreamDAO() {
		clientAlerts = new Vector<>();
		dbAlerts = new Vector<>();
		events = new Vector<>();
		clientCounter = new AtomicInteger();
		dbCounter = new AtomicInteger();
		eventCounter = new AtomicInteger();
	}
	
	
	public void reset(){
		clientAlerts = new Vector<>();
		dbAlerts = new Vector<>();
		events = new Vector<>();
		clientCounter = new AtomicInteger();
		dbCounter = new AtomicInteger();
		eventCounter = new AtomicInteger();
	}
	
	@Override
	public DBAlert createOrGetDBAlert(String storedProc, boolean oneTime) {
		for (DBAlert a : this.dbAlerts) {
			if (a.stroredProc.equalsIgnoreCase(storedProc)){
				return a;
			}
		}
		DBAlert a = new DBAlert(dbCounter.getAndIncrement(),storedProc, oneTime, "null");
		dbAlerts.add(a);
		return a;
	}

	@Override
	public ClientAlert createClientAlert(int dbAlertId, boolean push,
			boolean oneTime, String pushURL) throws AlertException {
		DBAlert dbAlert = null;
		for (DBAlert a : this.dbAlerts) {
			if (a.dbAlertID == dbAlertId){
				dbAlert = a;
			}
		}
		if (dbAlert == null){
			throw new AlertException("Missing DB alert " + dbAlertId);
		}
		for (ClientAlert a : this.clientAlerts){
			if ((a.pushURL != null && a.pushURL.equalsIgnoreCase(pushURL)) && a.active){
				a.push = push;
				a.oneTime = oneTime;
				return a;
			}
		}
		ClientAlert alert = new ClientAlert(clientCounter.getAndIncrement(), dbAlertId, push, oneTime, true, false, pushURL);
		clientAlerts.add(alert);
		return alert;
	}

	@Override
	public List<Integer> addAlertEvent(int dbAlertId, String body) {
		AlertEvent event = new AlertEvent(eventCounter.getAndIncrement(), dbAlertId, body);
		events.add(event);
		List<Integer> clientsToNotify = new ArrayList<>();
		for (ClientAlert a : this.clientAlerts){
			if (a.dbAlertID == dbAlertId && a.active){
				//found a match
				clientsToNotify.add(a.clientAlertID);
				
			}
		}
		return clientsToNotify;
	}

	@Override
	public List<String> updatePullsAndGetPushURLS(List<Integer> clientAlertIds) {
		List<String> urls = new ArrayList<>();

		for (ClientAlert a : this.clientAlerts){
			if (clientAlertIds.contains(a.clientAlertID)){
				//found a match
				if (a.push){
					urls.add(a.pushURL);
					if (a.oneTime){
						a.active = false;
					}
				} else{
					//pull
					a.unseenPull = true;
					a.lastPullTime = new Date();
					
				}
				
			}
		}
		return urls;
	}

	@Override
	public boolean checkForNewPull(int clientAlertId) {
		for (ClientAlert a : this.clientAlerts){
			if (a.clientAlertID==clientAlertId && a.active && !a.push && a.unseenPull){
				//disable if onetime
				if (a.oneTime)
					a.active = false;
				//We have seen it
				a.unseenPull = false;
				return true;
			}
		}
		return false;
	}


	@Override
	public List<DBAlert> getDBAlerts() {
		return dbAlerts;
	}


	@Override
	public List<ClientAlert> getActiveClientAlerts() {
		List<ClientAlert> cas = new ArrayList<>();
		for (ClientAlert a: this.clientAlerts){
			if (a.active){
				cas.add(a);
			}
		}
		return cas;
	}
	
	

}
