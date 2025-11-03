package com.iitj.bigdata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Performs SQL DDL and SELECT queries on an Amazon Redshift database hosted on AWS.
 */
public class AmazonRedshift {
    /**
     * Number of INSERT statements after which a commit is performed during data load.
     */
    private static final int INSERT_BATCH_SIZE = 5000;
    /**
     * Connection to database
     */
    private Connection con;

    /**
     * Connection configuration loaded from config.properties
     */
    private String url;
    private String uid;
    private String pw;

    /**
     * Constructor - loads database configuration from config.properties
     */
    public AmazonRedshift() {
        loadConfig();
    }

    /**
     * Loads database configuration from config.properties file.
     */
    private void loadConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.err.println("Unable to find config.properties");
                throw new RuntimeException("config.properties not found in resources");
            }
            props.load(input);
            
            url = props.getProperty("redshift.url");
            uid = props.getProperty("redshift.username");
            pw = props.getProperty("redshift.password");
            
            System.out.println("Configuration loaded successfully.");
        } catch (IOException e) {
            System.err.println("Error loading config.properties: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    /**
     * Main method is only used for convenience. Use JUnit test file to verify your answer.
     *
     * @param args none expected
     * @throws SQLException if a database error occurs
     */
    public static void main(String[] args) throws SQLException {
        AmazonRedshift q = new AmazonRedshift();
        q.connect();
        q.drop();
        q.create();
        q.insert();
        
        System.out.println("\n=== Running Query 1 ===");
        ResultSet rs1 = q.query1();
        System.out.println(resultSetToString(rs1, 10));
        
        System.out.println("\n=== Running Query 2 ===");
        ResultSet rs2 = q.query2();
        System.out.println(resultSetToString(rs2, 10));
        
        System.out.println("\n=== Running Query 3 ===");
        ResultSet rs3 = q.query3();
        System.out.println(resultSetToString(rs3, 10));
        
        q.close();
    }

    /**
     * Makes a connection to the database and returns connection to caller.
     *
     * @return connection
     * @throws SQLException if an error occurs
     */
    public Connection connect() throws SQLException {
        System.out.println("Connecting to database.");
        
        try {
            // Load the Redshift JDBC driver
            Class.forName("com.amazon.redshift.jdbc42.Driver");
            
            // Establish connection
            con = DriverManager.getConnection(url, uid, pw);
            
            if (con != null) {
                System.out.println("Successfully connected to Redshift database!");
            }
        } catch (ClassNotFoundException e) {
            System.err.println("Redshift JDBC Driver not found.");
            e.printStackTrace();
            throw new SQLException("Driver not found", e);
        }
        
        return con;
    }

    /**
     * Closes connection to database.
     */
    public void close() {
        System.out.println("Closing database connection.");
        
        try {
            if (con != null && !con.isClosed()) {
                con.close();
                System.out.println("Database connection closed successfully.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing connection: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Drops all tables from the dev schema.
     */
    public void drop() {
        System.out.println("Dropping all the tables");
        
        String[] tables = {"lineitem", "orders", "partsupp", "part", "customer", "supplier", "nation", "region"};
        
        try (Statement stmt = con.createStatement()) {
            for (String table : tables) {
                try {
                    String dropSQL = "DROP TABLE IF EXISTS " + table + " CASCADE";
                    stmt.execute(dropSQL);
                    System.out.println("Dropped table: " + table);
                } catch (SQLException e) {
                    System.err.println("Warning: Could not drop table " + table + ": " + e.getMessage());
                }
            }
            System.out.println("All tables dropped successfully.");
        } catch (SQLException e) {
            System.err.println("Error dropping tables: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Creates the database dev schema and all TPC-H tables.
     *
     * @throws SQLException if an error occurs
     */
    public void create() throws SQLException {
        System.out.println("Creating Tables");
        
        try (Statement stmt = con.createStatement()) {
            // Print current schema
            ResultSet rs = stmt.executeQuery("SELECT current_schema()");
            if (rs.next()) {
                System.out.println("Current schema: " + rs.getString(1));
            }
            rs.close();
            
            // Read and execute the TPC-H create script from classpath resources
            String createScript = readResource("data/tpch_create.sql");
            
            // Split by semicolon and execute each statement
            String[] statements = createScript.split(";");
            for (String sql : statements) {
                sql = sql.trim();
                if (!sql.isEmpty()) {
                    stmt.execute(sql);
                }
            }
            
            System.out.println("All tables created successfully.");
        } catch (IOException e) {
            System.err.println("Error reading create script: " + e.getMessage());
            e.printStackTrace();
            throw new SQLException("Failed to read create script", e);
        }
    }

    /**
     * Loads TPC-H data into the database using multi-row INSERT statements.
     *
     * @throws SQLException if an error occurs
     */
    public void insert() throws SQLException {
        System.out.println("Loading TPC-H Data");
        
        String[] dataFiles = {
            "region.sql", "nation.sql", "customer.sql", "supplier.sql",
            "part.sql", "partsupp.sql", "orders.sql", "lineitem.sql"
        };
        
        // Disable autoCommit for batch processing
        con.setAutoCommit(false);
        
        try (Statement stmt = con.createStatement()) {
            for (String file : dataFiles) {
                System.out.println("Loading data from: " + file);
                String insertScript = readResource("data/" + file);
                
                // Parse individual INSERT statements and batch them into multi-row inserts
                String[] inserts = insertScript.split(";");
                List<String> valueClauses = new ArrayList<>();
                String tableName = null;
                int totalCount = 0;
                
                for (String sql : inserts) {
                    sql = sql.trim();
                    if (!sql.isEmpty() && sql.toLowerCase().startsWith("insert")) {
                        // Extract table name and VALUES clause
                        // Format: INSERT INTO tablename VALUES(...)
                        int valuesIdx = sql.toLowerCase().indexOf("values");
                        if (valuesIdx > 0) {
                            if (tableName == null) {
                                // Extract table name from first insert
                                String insertPart = sql.substring(0, valuesIdx).trim();
                                tableName = insertPart.replaceAll("(?i)insert\\s+into\\s+", "").trim();
                            }
                            
                            // Extract the VALUES(...) part
                            String valuesClause = sql.substring(valuesIdx + 6).trim(); // skip "values"
                            valueClauses.add(valuesClause);
                            
                            // Execute multi-row insert when we reach batch size
                            if (valueClauses.size() >= INSERT_BATCH_SIZE) {
                                executeMultiRowInsert(stmt, tableName, valueClauses);
                                totalCount += valueClauses.size();
                                valueClauses.clear();
                                con.commit();
                                System.out.println("  Inserted " + totalCount + " records...");
                            }
                        }
                    }
                }
                
                // Insert any remaining rows
                if (!valueClauses.isEmpty()) {
                    executeMultiRowInsert(stmt, tableName, valueClauses);
                    totalCount += valueClauses.size();
                    valueClauses.clear();
                }
                
                con.commit();
                System.out.println("  Completed loading " + file + " (" + totalCount + " records)");
            }
            
            System.out.println("All data loaded successfully.");
        } catch (IOException e) {
            con.rollback();
            System.err.println("Error reading data file: " + e.getMessage());
            e.printStackTrace();
            throw new SQLException("Failed to load data", e);
        } catch (SQLException e) {
            con.rollback();
            throw e;
        } finally {
            // Re-enable autoCommit
            con.setAutoCommit(true);
        }
    }

    /**
     * Executes a multi-row INSERT statement.
     *
     * @param stmt Statement object
     * @param tableName target table name
     * @param valueClauses list of VALUES(...) clauses
     * @throws SQLException if execution fails
     */
    private void executeMultiRowInsert(Statement stmt, String tableName, List<String> valueClauses) 
            throws SQLException {
        if (valueClauses.isEmpty()) {
            return;
        }
        
        // Build multi-row insert: INSERT INTO table VALUES (...), (...), (...)
        StringBuilder multiRowInsert = new StringBuilder();
        multiRowInsert.append("INSERT INTO ").append(tableName).append(" VALUES ");
        
        for (int i = 0; i < valueClauses.size(); i++) {
            if (i > 0) {
                multiRowInsert.append(", ");
            }
            multiRowInsert.append(valueClauses.get(i));
        }
        
        stmt.execute(multiRowInsert.toString());
    }

    /**
     * Reads a classpath resource into a String.
     *
     * @param resourcePath path within classpath (e.g., "data/region.sql")
     * @return file content
     * @throws IOException if the resource is not found or cannot be read
     */
    private String readResource(String resourcePath) throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IOException("Resource not found on classpath: " + resourcePath);
            }
            return new String(in.readAllBytes());
        }
    }

    /**
     * Query returns the most recent top 10 orders with the total sale and the date of the order
     * for customers in America.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query1() throws SQLException {
        System.out.println("Executing query #1.");
        
        String sql = 
            "SELECT o.o_orderkey, " +
            "       o.o_orderdate, " +
            "       SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_sale " +
            "FROM orders o " +
            "JOIN lineitem l ON o.o_orderkey = l.l_orderkey " +
            "JOIN customer c ON o.o_custkey = c.c_custkey " +
            "JOIN nation n ON c.c_nationkey = n.n_nationkey " +
            "JOIN region r ON n.n_regionkey = r.r_regionkey " +
            "WHERE r.r_name = 'AMERICA' " +
            "GROUP BY o.o_orderkey, o.o_orderdate " +
            "ORDER BY o.o_orderdate DESC " +
            "LIMIT 10";
        
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * Query returns the customer key and the total price a customer spent in descending order,
     * for all urgent orders that are not failed for all customers who are outside Europe
     * and belong to the largest market segment.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query2() throws SQLException {
        System.out.println("Executing query #2.");
        
        String sql = 
            "WITH largest_segment AS ( " +
            "    SELECT c_mktsegment " +
            "    FROM customer " +
            "    GROUP BY c_mktsegment " +
            "    ORDER BY COUNT(*) DESC " +
            "    LIMIT 1 " +
            ") " +
            "SELECT c.c_custkey, " +
            "       SUM(o.o_totalprice) AS total_spent " +
            "FROM customer c " +
            "JOIN orders o ON c.c_custkey = o.o_custkey " +
            "JOIN nation n ON c.c_nationkey = n.n_nationkey " +
            "JOIN region r ON n.n_regionkey = r.r_regionkey " +
            "WHERE o.o_orderpriority = '1-URGENT' " +
            "  AND o.o_orderstatus != 'F' " +
            "  AND r.r_name != 'EUROPE' " +
            "  AND c.c_mktsegment = (SELECT c_mktsegment FROM largest_segment) " +
            "GROUP BY c.c_custkey " +
            "ORDER BY total_spent DESC";
        
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * Query returns a count of all the line items that were ordered within the six years
     * starting on April 1st, 1997, grouped by order priority in ascending order.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query3() throws SQLException {
        System.out.println("Executing query #3.");
        
        String sql = 
            "SELECT o.o_orderpriority, " +
            "       COUNT(l.l_orderkey) AS lineitem_count " +
            "FROM orders o " +
            "JOIN lineitem l ON o.o_orderkey = l.l_orderkey " +
            "WHERE o.o_orderdate >= DATE '1997-04-01' " +
            "  AND o.o_orderdate < DATE '2003-04-01' " +
            "GROUP BY o.o_orderpriority " +
            "ORDER BY o.o_orderpriority ASC";
        
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    /*
     * Helper methods - Do not change anything below here.
     */

    /**
     * Reads a file and returns its content as a string.
     *
     * @param filename the file to read
     * @return file content
     * @throws IOException if file reading fails
     */
    private String readFile(String filename) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filename)));
    }

    /**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined but only the first few are put into a string.
     *
     * @param rst     ResultSet
     * @param maxrows maximum number of rows to display
     * @return String form of results
     * @throws SQLException if a database error occurs
     */
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();

        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');

        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    /**
     * Converts ResultSetMetaData into a string.
     *
     * @param meta ResultSetMetaData
     * @return string form of metadata
     * @throws SQLException if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + ", " +
                meta.getColumnType(1) + "-" + meta.getColumnTypeName(1) + ", " +
                meta.getColumnDisplaySize(1) + ", " + meta.getPrecision(1) + ", " +
                meta.getScale(1) + ")");

        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + ", " +
                    meta.getColumnType(j) + "-" + meta.getColumnTypeName(j) + ", " +
                    meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j) + ", " +
                    meta.getScale(j) + ")");
        return buf.toString();
    }
}
