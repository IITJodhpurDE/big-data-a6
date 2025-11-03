package com.iitj.bigdata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * JUnit test cases for AmazonRedshift class.
 */
public class AmazonRedshiftTest {
    
    private AmazonRedshift redshift;
    
    @Before
    public void setUp() throws SQLException {
        redshift = new AmazonRedshift();
        redshift.connect();
    }
    
    @After
    public void tearDown() {
        if (redshift != null) {
            redshift.close();
        }
    }
    
    @Test
    public void testConnect() throws SQLException {
        Connection con = redshift.connect();
        assertNotNull("Connection should not be null", con);
        assertFalse("Connection should be open", con.isClosed());
    }
    
    @Test
    public void testQuery1() throws SQLException {
        ResultSet rs = redshift.query1();
        assertNotNull("ResultSet should not be null", rs);
        
        // Verify we have results
        assertTrue("Should have at least one result", rs.next());
        
        // Verify columns exist
        assertNotNull("Order key should exist", rs.getObject("o_orderkey"));
        assertNotNull("Order date should exist", rs.getObject("o_orderdate"));
        assertNotNull("Total sale should exist", rs.getObject("total_sale"));
        
        System.out.println("Query 1 Results:");
        System.out.println(AmazonRedshift.resultSetToString(rs, 10));
    }
    
    @Test
    public void testQuery2() throws SQLException {
        ResultSet rs = redshift.query2();
        assertNotNull("ResultSet should not be null", rs);
        
        // Check if we have results
        if (rs.next()) {
            assertNotNull("Customer key should exist", rs.getObject("c_custkey"));
            assertNotNull("Total spent should exist", rs.getObject("total_spent"));
            
            System.out.println("Query 2 Results:");
            System.out.println(AmazonRedshift.resultSetToString(rs, 10));
        } else {
            System.out.println("Query 2: No results found (this may be expected based on data)");
        }
    }
    
    @Test
    public void testQuery3() throws SQLException {
        ResultSet rs = redshift.query3();
        assertNotNull("ResultSet should not be null", rs);
        
        // Verify we have results
        assertTrue("Should have at least one result", rs.next());
        
        // Verify columns exist
        assertNotNull("Order priority should exist", rs.getObject("o_orderpriority"));
        assertNotNull("Lineitem count should exist", rs.getObject("lineitem_count"));
        
        System.out.println("Query 3 Results:");
        System.out.println(AmazonRedshift.resultSetToString(rs, 10));
    }
}
