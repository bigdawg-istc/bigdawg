package istc.bigdawg.stream;

import static org.junit.Assert.assertEquals;
import istc.bigdawg.Main;
import istc.bigdawg.stream.StreamDAO.DBAlert;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

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
    public void testAddDBAlert() {
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

}
