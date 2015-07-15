package istc.bigdawg.stream;

import java.sql.Date;
import java.util.List;

public abstract class StreamDAO {
  public class ClientAlert {
    public int clientAlertID;
    public int dbAlertID;
    public boolean push;
    public boolean oneTime;
    public boolean active;
    public boolean pulled ;
    public Date lastPullTime;
    public String pushURL;
    public Date created;
  }

  public class DBAlert {
    public int dbAlertID;
    public String stroredProc;
    public boolean oneTime;
    public String receiveURL;
    public Date created;
  }

  public class AlertEvent {
    public int alertID;
    public int dbAlertID;
    public Date ts;
    public String body;
  }
  
  public abstract DBAlert createOrGetDBAlert(String storedProc, boolean oneTime);
  public abstract ClientAlert createClientAlert(int dbAlertId, boolean push, boolean oneTime, String pushURL);
  public abstract List<Integer> addAlertEvent(int dbAlertId, String body);
  public abstract List<String> updatePullsAndGetPushURLS(List<Integer> clientAlertIds);

}
