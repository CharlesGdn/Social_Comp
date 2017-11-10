
import java.sql.*;


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

            

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.err.println(ex.getMessage());
            }
        }
    }

    public static void createNewTable() {
        // SQLite connection string
        String url = "jdbc:sqlite:test.sqlite";

        // SQL statement for creating a new table
        String sqltable1 = "CREATE TABLE IF NOT EXISTS ratings (\n"
                + "	User_id integer NOT NULL,\n"
                + "	Item_id integer NOT NULL,\n"
                + "	Ratings integer NOT NULL\n"
                + ");";


        System.out.println("testing 1 2 3");

        String sqltable2 = "CREATE TABLE IF NOT EXISTS averages (\n"
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

    public static void query(int n, int i, int j) {
        String url = "jdbc:sqlite:test.sqlite";

        String sqlItemI = "SELECT Ratings FROM ratings WHERE User_id = " + n + " AND Item_id = " + i;
        String sqlItemJ = "SELECT Ratings FROM ratings WHERE User_id = " + n + " AND Item_id = " + j;
        String sqlAvg = "SELECT Average_Ratings FROM averages WHERE User_id = " + n;

        String sqlTop = "SELECT SUM (*) FROM ( " +
                    "SELECT * FROM (" +
                        "(" + sqlItemI + " - " + sqlAvg + ") * (" + sqlItemJ + " - " + sqlAvg + "))" +
	                ")" +
            ")";

        String sqlSqrI = "SELECT SUM (squares) FROM (" +
                    "SELECT rating AS squares FROM (" +
                        "SQUARE(" + sqlItemI + " - " + sqlAvg + ")" +
                    ")" +
            ")";

        String sqlSqrJ = "SELECT SUM (squares) FROM (" +
                    "SELECT rating AS squares FROM (" +
                        "SQUARE(" + sqlItemJ + " - " + sqlAvg + ")" +
                    ")" +
            ")";

        String sqlBottom = "SQRT(" + sqlSqrI + ") * SQRT(" + sqlSqrJ + ")";

        String sqlSim = sqlTop + " / " + sqlBottom;

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // query the table
            ResultSet results = stmt.executeQuery(sqlSim);
            System.out.println("Executed successfully");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        connect();
        createNewTable();
        query(1, 2, 3);
    }
}

