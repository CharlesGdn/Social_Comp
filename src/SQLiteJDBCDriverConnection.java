
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

    public static Map calculateSimilarityMatrix() {
        String url = "jdbc:sqlite:test.sqlite";

        Map<Pair<Integer, Integer>, Float> simMatrix = new HashMap<>();

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            //Loop through every item
            for(int i=stmt.executeQuery("SELECT MIN (Item_id) FROM ratings").getInt(1); i<=stmt.executeQuery("SELECT MAX (Item_id) FROM ratings").getInt(1); i++) {
                for(int j=stmt.executeQuery("SELECT MIN (Item_id) FROM ratings").getInt(1); j<=i; j++) {

                    float topSum = 0;       //Because the top of the formula is a sum
                    float iSquareSum = 0;   //Because the bottom of the formula sums all (Item 'i' ratings)^2
                    float jSquareSum = 0;   //Because the bottom of the formula sums all (Item 'j' ratings)^2

                    //Work out the sums
                    for(int n=stmt.executeQuery("SELECT MIN (User_id) FROM ratings").getInt(1); n<=stmt.executeQuery("SELECT MAX (User_id) FROM ratings").getInt(1); n++) {
                        topSum += stmt.executeQuery("SELECT ratingI * ratingJ FROM "
                                + "(SELECT ratings.Ratings - averages.Average_Ratings AS ratingI FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + i + " AND averages.User_id = " + n + "), "
                                + "(SELECT ratings.Ratings - averages.Average_Ratings AS ratingJ FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + j + " AND averages.User_id = " + n + ")").getFloat(1);

                        iSquareSum += (float) Math.pow((stmt.executeQuery("SELECT ratings.Ratings - averages.Average_Ratings FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + i + " AND averages.User_id = " + n).getDouble(1)), 2);
                        jSquareSum += (float) Math.pow((stmt.executeQuery("SELECT ratings.Ratings - averages.Average_Ratings FROM ratings, averages WHERE ratings.User_id = " + n + " AND ratings.Item_id = " + j + " AND averages.User_id = " + n).getDouble(1)), 2);
                    }

                    //Work out the bottom of the formula and then the similarity using the sums
                    float bottom = (float) (Math.sqrt((double) iSquareSum) * (Math.sqrt((double) jSquareSum)));
                    float similarity = topSum / bottom;

                    //Save the similarity or 0 in the matrix
                    if(Double.isNaN(similarity)) {
                        simMatrix.put(new Pair(i, j), (float) 0);
                    } else { simMatrix.put(new Pair(i, j), similarity); }
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return simMatrix;
    }

    public static void calculatePredictions(Map simMatrix) {
        String url = "jdbc:sqlite:test.sqlite";

        //{user_id -> item_id} -> prediction
        Map<Pair<Integer, Integer>, Float> predictions = new HashMap<>();

        try (Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement()) {

            //Loop through every item for every user in turn
            for(int n=stmt.executeQuery("SELECT MIN (User_id) FROM ratings").getInt(1); n<=stmt.executeQuery("SELECT MAX (User_id) FROM ratings").getInt(1); n++) {
                for(int i=stmt.executeQuery("SELECT MIN (Item_id) FROM ratings").getInt(1); i<=stmt.executeQuery("SELECT MAX (Item_id) FROM ratings").getInt(1); i++) {

                    float topSum = 0;
                    float bottomSum = 0;

                    for (int j = stmt.executeQuery("SELECT MIN (Item_id) FROM ratings").getInt(1); j <= i; j++) {
                        topSum += (((float) simMatrix.get(new Pair(i, j))) * stmt.executeQuery("SELECT Ratings from ratings WHERE User_id = " + n + " AND Item_id = " + j).getFloat(1));
                        bottomSum += (float) simMatrix.get(new Pair(i, j));
                    }

                    float prediction = topSum/bottomSum;

                    if(Double.isNaN(prediction)) {
                        predictions.put(new Pair(n, i), (float) 0);
                    } else { predictions.put(new Pair(n, i), topSum/bottomSum); }
                }
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
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
        calculatePredictions(calculateSimilarityMatrix());
    }
}

