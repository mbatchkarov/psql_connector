package uk.co.casmconsulting;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mmb28 on 18/07/2016.
 */
public class Params {
    public String db, table, user, host;

    public Params() throws IOException {
        Properties prop = new Properties();
        try (InputStream in = ConnectAndRun.class.getResourceAsStream("/connection");) {
            prop.load(in);
        }
        db = prop.getProperty("db");
        table = prop.getProperty("table");
        user = prop.getProperty("user");
        host = prop.getProperty("host");
    }

    @Override
    public String toString() {
        return String.format("database %s, table %s", db, table);
    }
}
