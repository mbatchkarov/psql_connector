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
    private int port = 5432;
    private String database;

    public PostgreSQLConnection() {
    }

    public PostgreSQLConnection setHost(String host) {
        this.host = host;
        return this;
    }

    public PostgreSQLConnection setUser(String user) {
        this.user = user;
        return this;
    }

    public PostgreSQLConnection setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getUrlFormat() {
        return "jdbc:postgresql://%s:%d/%s";
    }

    public PostgreSQLConnection setPort(int port) {
        this.port = port;
        return this;
    }

    public PostgreSQLConnection setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String buildUrlString() {
        return String.format(getUrlFormat(), host, port, database);
    }

    public Connection buildConnection() throws SQLException {
        return DriverManager.getConnection(buildUrlString(), user, password);
    }
}
