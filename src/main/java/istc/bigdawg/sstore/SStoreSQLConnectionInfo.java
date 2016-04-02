package istc.bigdawg.sstore;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collection;

import org.apache.commons.lang3.tuple.Pair;

import istc.bigdawg.query.ConnectionInfo;
import istc.bigdawg.query.DBHandler;

public class SStoreSQLConnectionInfo implements ConnectionInfo {
    
    /**
     * 
     */
    private static final long serialVersionUID = 2L;
    private String host;
    private String port;
    private String database;
    private String user;
    private String password;
    
    public SStoreSQLConnectionInfo(String host, String port, String database,
            String user, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String getUrl() {
        return "jdbc:voltdb://" + getHost() + ":" + getPort();
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getPort() {
        return port;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDatabase() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCleanupQuery(Collection<String> objects) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long[] computeHistogram(String object, String attribute, double start, double end, int numBuckets) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Pair<Number, Number> getMinMax(String object, String attribute) throws SQLException, ParseException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DBHandler getHandler() {
        return new SStoreSQLHandler(this);
    }
    
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        String NEW_LINE = System.getProperty("line.separator");

        result.append(this.getClass().getName() + " Object {" + NEW_LINE);
        result.append(" Host: " + this.getHost() + NEW_LINE);
        result.append(" Port: " + this.getPort() + NEW_LINE);
        result.append(" Database: " + this.getDatabase() + NEW_LINE);
        result.append(" User: " + this.getUser() + NEW_LINE);
        result.append(" Password: This is a secret!" + NEW_LINE);
        result.append("}");

        return result.toString();
    }
    
    public String toSimpleString() {
        StringBuilder result = new StringBuilder();

        result.append(this.getClass().getName() + " Object {");
        result.append(" Host: " + this.getHost());
        result.append(" Port: " + this.getPort());
        result.append(" Database: " + this.getDatabase());
        result.append(" User: " + this.getUser());
        result.append(" Password: This is a secret!");
        result.append("}");

        return result.toString();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((database == null) ? 0 : database.hashCode());
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((port == null) ? 0 : port.hashCode());
        result = prime * result + ((user == null) ? 0 : user.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof SStoreSQLConnectionInfo))
            return false;
        SStoreSQLConnectionInfo other = (SStoreSQLConnectionInfo) obj;
        if (database == null) {
            if (other.database != null)
                return false;
        } else if (!database.equals(other.database))
            return false;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (port == null) {
            if (other.port != null)
                return false;
        } else if (!port.equals(other.port))
            return false;
        if (user == null) {
            if (other.user != null)
                return false;
        } else if (!user.equals(other.user))
            return false;
        return true;
    }

}
