
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by charles on 07/11/17.
 */
public class SQLiteJDBCDriverConnection {

    public static void connect() {
        Connection conn = null;
        try {
            // db parameters
            String url = "jdbc:sqlite:comp3208-2017-train.sqlite";
            String urldb = "jdbc:sqlite:test.sqlite";
            String url2 = "jdbc:sqlite:MyData.sqlite";
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            conn = DriverManager.getConnection(urldb);
            conn = DriverManager.getConnection(url2);

            System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }

    public static void createNewTable() {
        // SQLite connection string
        String url = "jdbc:sqlite:test.sqlite";

        // SQL statement for creating a new table
        String sqltable1 = "CREATE TABLE IF NOT EXISTS train (\n"
                + "	User_id integer NOT NULL,\n"
                + "	Item_id integer NOT NULL,\n"
                + "	Ratings integer NOT NULL\n"
                + ");";


        System.out.println("testing 1 2 3");

        String sqltable2 = "CREATE TABLE IF NOT EXISTS average (\n"
                + "	User_id integer NOT NULL,\n"
                + "	Average_Ratings float(8) NOT NULL\n"
                + ");";

        System.out.println("testing 4 5 6");
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sqltable1);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sqltable2);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        connect();
        createNewTable();
    }
}

