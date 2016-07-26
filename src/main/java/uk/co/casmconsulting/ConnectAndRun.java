package uk.co.casmconsulting;

import org.postgresql.util.PSQLException;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by mmb28 on 14/07/2016.
 */
public class ConnectAndRun {

    public static Connection getRemotePostgresConnection(String db) throws SQLException {
        PostgreSQLConnection params = new PostgreSQLConnection().setUser("postgres").setDatabase(db).setPort(4321);
        return params.buildConnection();
    }

    public static Connection getLocalPostgresConnection() throws SQLException {
        PostgreSQLConnection params = new PostgreSQLConnection().setUser("postgres").setDatabase("postgres");
        return params.buildConnection();
    }

    public static void initPostgresForeignTable(Connection localConn, Connection remoteConn, String remoteTable, String remoteDb) throws SQLException {
        System.out.println("Preparing your local database");
        Statement localStatement = localConn.createStatement();

        String sql = "SELECT current_database() AS db";
        ResultSet rs = localStatement.executeQuery(sql);
        while (rs.next()) {
            String localDB = rs.getString("db");
            System.out.println(String.format("Creating local copy of remote DB '%s.%s' in local DB '%s'",
                                             remoteTable, remoteDb, localDB));
        }
        createPgExtension(localStatement, "dblink");
        createPgExtension(localStatement, "postgres_fdw");


        // prepared statements cant be used to parameterise table names
        String connectDblinkSql = "SELECT dblink_connect('hostaddr=127.0.0.1 port=4321 dbname=%s user=postgres');";
        localStatement.execute(String.format(connectDblinkSql, remoteTable));

        localStatement.execute("DROP SERVER IF EXISTS server CASCADE;");

        String createServerSql = "CREATE SERVER server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.01', port '4321', dbname '%s');";
        localStatement.execute(String.format(createServerSql, remoteTable));

        setUserMapping(localStatement);
        localStatement.execute("DROP FOREIGN TABLE IF EXISTS foreign_table;");


        String blah = getRemoteTableColumnDescriptions(remoteConn, remoteTable);

        // column definition needs to be dynamically generated. Explain how this works and why
        // PG9.5 (Jan 2016) supports IMPORT FOREIGN SCHEMA
        // Otherwise execute this on the remote machine (start tunnel, then connect to database, then run this):
        // select table_name, column_name, ordinal_position, data_type from information_schema.columns where table_name = 'unix_users';
        String createForeignTableSql = "CREATE FOREIGN TABLE foreign_table (%s) SERVER server OPTIONS (schema_name 'public', table_name '%s');";
        // todo this does not seem to create the columns. is it something to do with my use of double quotes around column names
        String format = String.format(createForeignTableSql, blah, remoteDb);
        System.out.println(format);
        boolean execute = localStatement.execute(format);
        localStatement.execute("DROP TABLE IF EXISTS local_table;");
        localStatement.execute("CREATE TABLE local_table AS SELECT * FROM foreign_table;");


        rs = localStatement.executeQuery("SELECT COUNT(*) AS count FROM local_table");
        while (rs.next()) {
            int count = rs.getInt("count");
            System.out.println(String.format("Fetched a total of %d entries", count));
        }
    }

    private static String getRemoteTableColumnDescriptions(Connection remoteConn, String remoteTable) throws SQLException {
        Statement remoteStatement = remoteConn.createStatement();
        StringBuilder sb = new StringBuilder().append("id serial NOT NULL");
        String selectColumnsSql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND column_name != 'id' ORDER BY ordinal_position";
        ResultSet rs = remoteStatement.executeQuery(String.format(selectColumnsSql, remoteTable));
        while (rs.next()) {
            sb.append(String.format(", \"%s\" %s",
                                    rs.getString("column_name"), rs.getString("data_type")));
        }
        return sb.toString();
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
        if (conn == null) {
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
        //todo this could be more secure. the local user should be mapped to a dedicated remote PG user,
        // which can only read databases owned by their corresponding M52 role
        stmt.execute("DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER server;");
        stmt.execute("CREATE USER MAPPING FOR CURRENT_USER SERVER server OPTIONS (user 'postgres');");
    }
}
