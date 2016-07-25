package uk.co.casmconsulting;

import org.postgresql.util.PSQLException;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by mmb28 on 14/07/2016.
 */
public class ConnectAndRun {

    public static Connection getPostgresConnection() throws SQLException {
        PostgreSQLConnection params = new PostgreSQLConnection().setUser("postgres").setDatabase("postgres");
        return params.buildConnection();
    }

    public static void initPostgresForeignTable(Connection conn, String DB_NAME, String DB_TABLE) throws SQLException {
        System.out.println("Preparing your local database");

        Statement stmt = conn.createStatement();
        String sql = "SELECT current_database() AS db";
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            String localDB = rs.getString("db");
            System.out.println(String.format("Creating local copy of remote DB '%s.%s' in local DB '%s'",
                    DB_NAME, DB_TABLE, localDB));
        }
        createPgExtension(stmt, "dblink");
        createPgExtension(stmt, "postgres_fdw");


        // prepared statements cant be used to parameterise table names
        String connectDblinkSql = "SELECT dblink_connect('hostaddr=127.0.0.1 port=4321 dbname=%s user=postgres');";
        stmt.execute(String.format(connectDblinkSql, DB_NAME));

        stmt.execute("DROP SERVER IF EXISTS server CASCADE;");

        String createServerSql = "CREATE SERVER server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.01', port '4321', dbname '%s');";
        stmt.execute(String.format(createServerSql, DB_NAME));

        setUserMapping(stmt);
        stmt.execute("DROP FOREIGN TABLE IF EXISTS foreign_table;");

        String createForeignTableSql = "CREATE FOREIGN TABLE foreign_table (id serial NOT NULL, data text) SERVER server OPTIONS (schema_name 'public', table_name '%s');";
        stmt.execute(String.format(createForeignTableSql, DB_TABLE));
        stmt.execute("DROP TABLE IF EXISTS local_table;");
        stmt.execute("CREATE TABLE local_table AS SELECT * FROM foreign_table;");


        rs = stmt.executeQuery("SELECT COUNT(*) AS count FROM local_table");
        while (rs.next()) {
            int count = rs.getInt("count");
            System.out.println(String.format("Fetched a total of %d entries", count));
        }
    }

    private static void createPgExtension(Statement stmt, String extName) throws SQLException {
        try {
            stmt.execute(String.format("CREATE EXTENSION %s;", extName));
        } catch (PSQLException ex) {
            // the extension only needs to be added once per db instance, then will complain. ignore them complaints
            if (!ex.getMessage().endsWith("already exists")) {
                throw ex;
            }
        }
    }


    public static void updateForeignTable(Connection conn) throws SQLException {
        if (conn == null){
            System.out.println("Not connected to a database. Check for error messages above.");
            return;
        }
        System.out.println("Fetching new data from remote table");
        Statement stmt = conn.createStatement();
        setUserMapping(stmt);
        String updateSql = "INSERT INTO local_table SELECT * FROM foreign_table WHERE  id > (SELECT max(id) FROM local_table) ORDER BY id ASC;";
        int newRowCount = stmt.executeUpdate(updateSql);
        System.out.println(String.format("Fetched %d new rows", newRowCount));
    }

    private static void setUserMapping(Statement stmt) throws SQLException {
        stmt.execute("DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER server;");
        stmt.execute("CREATE USER MAPPING FOR CURRENT_USER SERVER server OPTIONS (user 'postgres');");
    }
}
