
import javafx.util.Pair;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


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
        String sqltable1 = "CREATE TABLE IF NOT EXISTS ratings ("
                + "User_id integer NOT NULL, "
                + "Item_id integer NOT NULL, "
                + "Ratings integer NOT NULL"
                + ");";

        String sqltable2 = "CREATE TABLE IF NOT EXISTS averages (\n"
                + "	User_id integer NOT NULL,\n"
                + "	Average_Ratings float(8) NOT NULL\n"
                + ");";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute("DROP TABLE IF EXISTS ratings");
            stmt.execute(sqltable1);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute("DROP TABLE IF EXISTS averages");
            stmt.execute(sqltable2);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void query() {
        String url = "jdbc:sqlite:test.sqlite";

        Map<Pair<Integer, Integer>, Float> simMatrix = new HashMap<Pair<Integer, Integer>, Float>();

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            for(int i=1; i<4; i++) {
                for(int j=1; j<=i; j++) {

                    float topSum = 0;
                    float iSquareSum = 0;
                    float jSquareSum = 0;

                    for(int n=1; n<3; n++) {
                        topSum += stmt.executeQuery("SELECT ratingI * ratingJ FROM "
                                + "(SELECT ratings.Ratings - averages.Average_Ratings AS ratingI FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + i + " AND averages.User_id = " + n + "), "
                                + "(SELECT ratings.Ratings - averages.Average_Ratings AS ratingJ FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + j + " AND averages.User_id = " + n + ")").getFloat(1);

                        iSquareSum += (float) Math.pow((stmt.executeQuery("SELECT ratings.Ratings - averages.Average_Ratings FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + i + " AND averages.User_id = " + n).getDouble(1)), 2);
                        jSquareSum += (float) Math.pow((stmt.executeQuery("SELECT ratings.Ratings - averages.Average_Ratings FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + j + " AND averages.User_id = " + n).getDouble(1)), 2);
                    }

                    float bottom = (float) (Math.sqrt((double) iSquareSum) * (Math.sqrt((double) jSquareSum)));
                    float similarity = topSum / bottom;

                    if(Double.isNaN(similarity)) {
                        simMatrix.put(new Pair(i, j), (float) 0);
                    } else { simMatrix.put(new Pair(i, j), similarity); }
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        for(Pair<Integer, Integer> key : simMatrix.keySet()) {
            System.out.println("Sim(" + key.getKey() + "," + key.getValue() + ") = " + simMatrix.get(key));
        }
    }

    public static void populateDB() {
        String url = "jdbc:sqlite:test.sqlite";

        ArrayList<String> queries = new ArrayList<String>();
        ArrayList<String> averages = new ArrayList<String>();

        queries.add("INSERT INTO ratings VALUES (1, 1, 7)");
        queries.add("INSERT INTO ratings VALUES (1, 2, 4)");
        queries.add("INSERT INTO ratings VALUES (1, 3, 9)");

        queries.add("INSERT INTO ratings VALUES (2, 1, 6)");
        queries.add("INSERT INTO ratings VALUES (2, 2, 9)");
        queries.add("INSERT INTO ratings VALUES (2, 3, 8)");

        averages.add("INSERT INTO averages VALUES (1, 6.6666667)");
        averages.add("INSERT INTO averages VALUES (2, 7.6666667)");

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            for(String query : queries) {
                stmt.execute(query);
            }

            for(String avg : averages) {
                stmt.execute((avg));
            }
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
        populateDB(); //populates the DB with generic data; used for debugging
        query();
    }
}

