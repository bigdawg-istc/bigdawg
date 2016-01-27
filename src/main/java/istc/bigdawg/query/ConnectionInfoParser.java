package istc.bigdawg.query;

import istc.bigdawg.postgresql.PostgreSQLConnectionInfo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chenp on 1/19/2016.
 */
public class ConnectionInfoParser {

    public static String connectionInfoToString (ConnectionInfo info) {
        StringBuilder result = new StringBuilder();
        result.append(String.format("CONNECTIONTYPE:%s", info.getClass().getName()));
        result.append(String.format("HOST:%s", info.getHost()));
        result.append(String.format("DATABASE:%s", info.getDatabase()));
        result.append(String.format("PASSWORD:%s", info.getPassword()));
        result.append(String.format("PORT:%s", info.getPort()));
        result.append(String.format("USER:%s", info.getUser()));
        return result.toString();
    }

    public static ConnectionInfo stringToConnectionInfo (String info) {
        Pattern connectionType = Pattern.compile("(?<=CONNECTIONTYPE:)(?s).*(?=HOST:)");
        Pattern host = Pattern.compile("(?<=HOST:)(?s).*(?=DATABASE:)");
        Pattern database = Pattern.compile("(?<=DATABASE:)(?s).*(?=PASSWORD:)");
        Pattern password = Pattern.compile("(?<=PASSWORD:)(?s).*(?=PORT:)");
        Pattern port = Pattern.compile("(?<=PORT:)(?s).*(?=USER:)");
        Pattern user = Pattern.compile("(?<=USER:)(?s).*");

        String engineConnectionType = "";
        Matcher m = connectionType.matcher(info);
        if (m.find()) {
            engineConnectionType = m.group();
        }
        String engineHost = "";
        m = host.matcher(info);
        if (m.find()) {
            engineHost = m.group();
        }
        String engineDatabase = "";
        m = database.matcher(info);
        if (m.find()) {
            engineDatabase = m.group();
        }
        String enginePassword = "";
        m = password.matcher(info);
        if (m.find()) {
            enginePassword = m.group();
        }
        String enginePort = "";
        m = port.matcher(info);
        if (m.find()) {
            enginePort = m.group();
        }
        String engineUser = "";
        m = user.matcher(info);
        if (m.find()) {
            engineUser = m.group();
        }

        ConnectionInfo connectionInfo = null;
        // TODO implement for other ConnectionInfo classes
        if (engineConnectionType.contains("PostgreSQLConnectionInfo")){
            connectionInfo = new PostgreSQLConnectionInfo(engineHost, enginePort, engineDatabase, engineUser, enginePassword);
        }
        return connectionInfo;
    }
}
