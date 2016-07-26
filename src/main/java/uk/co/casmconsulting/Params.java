package uk.co.casmconsulting;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mmb28 on 18/07/2016.
 */
public class Params {
    public String db, table, user, host;

    private Params(String db, String table, String user, String host) {
        this.db = db;
        this.table = table;
        this.user = user;
        this.host = host;
    }

    public static Params fromFile() throws IOException {
        Properties prop = new Properties();
        try (InputStream in = ConnectAndRun.class.getResourceAsStream("/connection");) {
            prop.load(in);
        }
        return new Params(prop.getProperty("db"), prop.getProperty("table"),
                          prop.getProperty("user"), prop.getProperty("host"));
    }

    @Override
    public String toString() {
        return String.format("database %s, table %s", db, table);
    }
}
