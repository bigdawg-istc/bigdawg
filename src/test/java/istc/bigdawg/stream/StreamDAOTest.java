package istc.bigdawg.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import istc.bigdawg.Main;
import istc.bigdawg.exceptions.AlertException;
import istc.bigdawg.stream.StreamDAO.ClientAlert;
import istc.bigdawg.stream.StreamDAO.DBAlert;

import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.commons.lang3.StringUtils;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StreamDAOTest {
    private HttpServer server;
    private WebTarget target;
    private StreamDAO dao;
    
    @Before
    public void setUp() throws Exception {
        // start the server
        server = Main.startServer();
        // create the client
        Client c = ClientBuilder.newClient();

        // uncomment the following line if you want to enable
        // support for JSON in the client (you also have to uncomment
        // dependency on jersey-media-json module in pom.xml and Main.startServer())
        // --
        // c.configuration().enable(new org.glassfish.jersey.media.json.JsonJaxbFeature());

        target = c.target(Main.BASE_URI);
        dao = Main.getStreamDAO();
    }
    
    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testAddDBAlert() throws AlertException {
    	dao.reset();
    	DBAlert dba = dao.createOrGetDBAlert("stored1", false);
    	//New ID
    	assertEquals(0, dba.dbAlertID);
    	DBAlert dba2 = dao.createOrGetDBAlert("stored1", false);
    	//Same ID
    	assertEquals(0, dba2.dbAlertID);
    	DBAlert dba3 = dao.createOrGetDBAlert("stored2", false);
    	//New ID
    	assertEquals(1, dba3.dbAlertID);
    	

    }
    
    @Test
    public void testOneTimeAlert() throws AlertException {
    	dao.reset();
    	//Create db alert events
    	DBAlert dba = dao.createOrGetDBAlert("stored1", false);
    	assertEquals(0, dba.dbAlertID);
    	DBAlert dba3 = dao.createOrGetDBAlert("stored2", false);
    	assertEquals(1, dba3.dbAlertID);
    	//Create client alers
    	ClientAlert caOneTimePull = dao.createClientAlert(0, false, true, null);
    	ClientAlert caOneTimePush = dao.createClientAlert(0, true, true, "pushURL1");
    	ClientAlert caOneTimePush2 = dao.createClientAlert(1, true, true, "pushURL2"); // no alert
    	//Make sure we have one tme allerts
    	assertEquals(0,caOneTimePull.clientAlertID);
    	assertEquals(1,caOneTimePush.clientAlertID);
    	assertEquals(0,caOneTimePush.dbAlertID);
    	assertEquals(2,caOneTimePush2.clientAlertID);
    	assertEquals(1,caOneTimePush2.dbAlertID);
    	//Add alert
    	List<Integer> alertsToUpdate = dao.addAlertEvent(0, "");
    	System.out.println(StringUtils.join(alertsToUpdate, ','));

    	//check that we have matched alerts
    	assertEquals(2, alertsToUpdate.size());
    	//update alerts
    	List<String> urls = dao.updatePullsAndGetPushURLS(alertsToUpdate);
    	System.out.println(StringUtils.join(urls, ','));
    	assertEquals(1, urls.size());
    	assertEquals("pushURL1", urls.get(0));
    	
    	//we havent pulled yet so one should still be active
    	alertsToUpdate = dao.addAlertEvent(0, "");
    	assertEquals(1, alertsToUpdate.size());
    	
    	//check that this fires no alerts
    	dao.checkForNewPull(caOneTimePull.clientAlertID);
    	alertsToUpdate = dao.addAlertEvent(0, "");
    	assertEquals(0, alertsToUpdate.size());
    	
    	//We should have one id left
    	assertEquals(1, dao.getActiveClientAlerts().size());
    	alertsToUpdate = dao.addAlertEvent(1, "");
    	urls = dao.updatePullsAndGetPushURLS(alertsToUpdate);
    	assertEquals(0, dao.getActiveClientAlerts().size());
    	assertEquals(1, urls.size());
    	assertEquals("pushURL2", urls.get(0));
    	
    }
  
    
    @Test
    public void testMultiTimePull() throws AlertException {
    	dao.reset();
    	//Create db alert events
    	DBAlert dba = dao.createOrGetDBAlert("stored1", false);
    	ClientAlert pull = dao.createClientAlert(dba.dbAlertID, false, false, null);
    	ClientAlert pull2 = dao.createClientAlert(dba.dbAlertID, false, false, null);
    	assertFalse(dao.checkForNewPull(pull.clientAlertID));
    	assertFalse(dao.checkForNewPull(pull2.clientAlertID));

    	List<Integer> clientAlertIds = dao.addAlertEvent(dba.dbAlertID, "");
    	dao.updatePullsAndGetPushURLS(clientAlertIds);
    	assertTrue(dao.checkForNewPull(pull.clientAlertID));
    	assertFalse(dao.checkForNewPull(pull.clientAlertID));
    	assertTrue(dao.checkForNewPull(pull2.clientAlertID));
    	
    	
    }
    
    @Test
    public void testMultiTimeAlert() throws AlertException {
    	dao.reset();
    	//Create db alert events
    	DBAlert dba = dao.createOrGetDBAlert("stored1", false);
    	assertEquals(0, dba.dbAlertID);
    	DBAlert dba3 = dao.createOrGetDBAlert("stored2", false);
    	assertEquals(1, dba3.dbAlertID);
    	//Create client alers
    	ClientAlert caOneTimePull = dao.createClientAlert(0, false, false, null);
    	ClientAlert caOneTimePush = dao.createClientAlert(0, true, false, "pushURL1");
    	ClientAlert caOneTimePush2 = dao.createClientAlert(1, true, false, "pushURL2"); // no alert
    	//Make sure we have one tme allerts
    	assertEquals(0,caOneTimePull.clientAlertID);
    	assertEquals(1,caOneTimePush.clientAlertID);
    	assertEquals(0,caOneTimePush.dbAlertID);
    	assertEquals(2,caOneTimePush2.clientAlertID);
    	assertEquals(1,caOneTimePush2.dbAlertID);
    	//Add alert
    	List<Integer> alertsToUpdate = dao.addAlertEvent(0, "");
    	System.out.println(StringUtils.join(alertsToUpdate, ','));

    	//check that we have matched alerts
    	assertEquals(2, alertsToUpdate.size());
    	//update alerts
    	List<String> urls = dao.updatePullsAndGetPushURLS(alertsToUpdate);
    	System.out.println(StringUtils.join(urls, ','));
    	assertEquals(1, urls.size());
    	assertEquals("pushURL1", urls.get(0));
    	
    	alertsToUpdate = dao.addAlertEvent(0, "");
    	assertEquals(2, alertsToUpdate.size());
    	
    	dao.checkForNewPull(caOneTimePull.clientAlertID);
    	alertsToUpdate = dao.addAlertEvent(0, "");
    	assertEquals(2, alertsToUpdate.size());
    	
    	assertEquals(3, dao.getActiveClientAlerts().size());
    	alertsToUpdate = dao.addAlertEvent(1, "");
    	urls = dao.updatePullsAndGetPushURLS(alertsToUpdate);
    	assertEquals(3, dao.getActiveClientAlerts().size());
    	assertEquals(1, urls.size());
    	assertEquals("pushURL2", urls.get(0));
    	
    }
    
}
