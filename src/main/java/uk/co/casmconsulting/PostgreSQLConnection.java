package uk.co.casmconsulting;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Copied from Datum
 */
public final class PostgreSQLConnection {
    private String host = "localhost";
    private String user;
    private String password;
    private Integer port = Integer.valueOf(5432);
    private String database;

    public PostgreSQLConnection() {
    }

    public String getHost() {
        return this.host;
    }

    public PostgreSQLConnection setHost(String host) {
        this.host = host;
        return this;
    }

    public String getUser() {
        return this.user;
    }

    public PostgreSQLConnection setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return this.password;
    }

    public PostgreSQLConnection setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getUrlFormat() {
        return "jdbc:postgresql://%s:%d/%s";
    }

    public int getPort() {
        return this.port.intValue();
    }

    public PostgreSQLConnection setPort(int port) {
        this.port = Integer.valueOf(port);
        return this;
    }

    public String getDatabase() {
        return this.database;
    }

    public PostgreSQLConnection setDatabase(String database) {
        this.database = database;
        return this;
    }

    public URL buildUrl() throws MalformedURLException {
        return new URL(this.buildUrlString());
    }

    public String buildUrlString() {
        return String.format(this.getUrlFormat(), new Object[]{this.getHost(), Integer.valueOf(this.getPort()), this.getDatabase()});
    }

    public Connection buildConnection() throws SQLException {
        return DriverManager.getConnection(this.buildUrlString(), this.getUser(), this.getPassword());
    }
}
