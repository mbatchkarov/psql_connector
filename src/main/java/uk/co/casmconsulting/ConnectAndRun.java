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
        PostgreSQLConnection params = new PostgreSQLConnection().setUser("postgres")
                .setDatabase(db).setPort(4321).setHost("127.0.0.1");
        return params.buildConnection();
    }

    public static Connection getLocalPostgresConnection() throws SQLException {
        PostgreSQLConnection params = new PostgreSQLConnection().setUser("postgres").setDatabase("postgres");
        return params.buildConnection();
    }

    public static void initPostgresForeignTable(Connection localConn, String remoteDb, String remoteTable) throws SQLException {
        String remoteTableDefinition = getRemoteTableColumnDescription(remoteDb, remoteTable);

        System.out.println("Preparing your local database");
        Statement localStatement = localConn.createStatement();

        String sql = "SELECT current_database() AS db";
        ResultSet rs = localStatement.executeQuery(sql);
        while (rs.next()) {
            String localDB = rs.getString("db");
            System.out.println(String.format("Creating local copy of remote DB '%s.%s' in local DB '%s'",
                                             remoteDb, remoteTable, localDB));
        }
        createPgExtension(localStatement, "dblink");
        createPgExtension(localStatement, "postgres_fdw");


        // prepared statements cant be used to parameterise table names
        String connectDblinkSql = "SELECT dblink_connect('hostaddr=127.0.0.1 port=4321 dbname=%s user=postgres');";
        localStatement.execute(String.format(connectDblinkSql, remoteDb));

        localStatement.execute("DROP SERVER IF EXISTS server CASCADE;");

        String createServerSql = "CREATE SERVER server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.01', port '4321', dbname '%s');";
        localStatement.execute(String.format(createServerSql, remoteDb));

        setUserMapping(localStatement);
        localStatement.execute("DROP FOREIGN TABLE IF EXISTS foreign_table;");


        String createForeignTableSql = "CREATE FOREIGN TABLE foreign_table (%s) SERVER server OPTIONS (schema_name 'public', table_name '%s');";
        String format = String.format(createForeignTableSql, remoteTableDefinition, remoteTable);
        localStatement.execute(format);
        localStatement.execute("DROP TABLE IF EXISTS local_table;");
        localStatement.execute("CREATE TABLE local_table AS SELECT * FROM foreign_table;");


        rs = localStatement.executeQuery("SELECT COUNT(*) AS count FROM local_table");
        while (rs.next()) {
            int count = rs.getInt("count");
            System.out.println(String.format("Fetched a total of %d entries", count));
        }
    }

    /**
     * To create a foreign table, we need to specify its column names and types, e.g.
     * CREATE FOREIGN TABLE name (id integer, quantity integer, description text)
     * This column definition needs to be dynamically generated.
     * To do this, execute this on the remote machine (start tunnel, then connect to database, then run this):
     * SELECT table_name, column_name, ordinal_position, data_type FROM information_schema.columns FROM table_name = 'name';
     *
     * Postgres 9.5 (Jan 2016) supports IMPORT FOREIGN SCHEMA, which will simplify this greatly
     *
     * @param remoteDb
     * @param remoteTable
     * @return
     * @throws SQLException
     */
    public static String getRemoteTableColumnDescription(String remoteDb, String remoteTable) throws SQLException {
        System.out.println("Getting remote table definition");
        Connection remoteConn = getRemotePostgresConnection(remoteDb);
        Statement remoteStatement = remoteConn.createStatement();
        StringBuilder sb = new StringBuilder().append("id serial NOT NULL");
        String selectColumnsSql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND column_name != 'id' ORDER BY ordinal_position";
        ResultSet rs = remoteStatement.executeQuery(String.format(selectColumnsSql, remoteTable));
        int columnCount = 0;
        while (rs.next()) {
            sb.append(String.format(", \"%s\" %s",
                                    rs.getString("column_name"), rs.getString("data_type")));
            columnCount++;
        }
        if (columnCount == 0) {
            String reason = "Could not find any columns in the remote table (possibly with the exception of id)";
            System.out.println(reason);
            throw new SQLException(reason);
        }
        remoteConn.close();
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
