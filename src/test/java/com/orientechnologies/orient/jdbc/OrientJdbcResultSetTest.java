package com.orientechnologies.orient.jdbc;

import java.sql.*;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class OrientJdbcResultSetTest extends OrientJdbcBaseTest {

    @Test
    public void shouldMapReturnTypes() throws Exception {

        assertFalse(conn.isClosed());

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT stringKey, intKey, text, length, date FROM Item");

        ResultSetMetaData metaData = rs.getMetaData();

        assertNotNull(metaData);
    }

    @Test
    public void findColumnsReturnsCorrectColumnIndex() throws Throwable {
      Statement statement = conn.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT uuid, title, content FROM Article");

      assertColumnLabelIndexInResultSet(resultSet, "uuid", 1);
    }

    @Test
    public void findColumnsReturnsCorrectColumnIndexForLastColumn() throws Throwable {
      Statement statement = conn.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT uuid, title, content FROM Article");

      assertColumnLabelIndexInResultSet(resultSet, "content", 3);
    }

    @Test
    public void intColumnCanBeReadAsString() throws Throwable{
      Statement statement = conn.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT stringKey, intKey FROM Item WHERE intKey = 1");
      assertTrue(resultSet.next());
      assertEquals("1", resultSet.getString(1));
      assertEquals("1", resultSet.getString(2));
    }

    @Test
    public void nullColumnDataIsReturnedAsNullFromGetString() throws Throwable {
      Statement statement = conn.createStatement();
      statement.execute("INSERT INTO Article (uuid, title) VALUES (22345, NULL)");

      ResultSet resultSet = statement.executeQuery("SELECT title FROM Article WHERE uuid = 22345");
      assertTrue(resultSet.next());
      assertNull(resultSet.getString(1));
//      assertTrue(resultSet.wasNull());
    }

    @Test
    public void requestedColumnsWithNullValueIsFoundFromResultSet() throws Throwable {
      Statement statement = conn.createStatement();
      statement.execute("INSERT INTO Article (uuid, title) VALUES (22346, NULL)");

      ResultSet resultSet = statement.executeQuery("SELECT title, uuid FROM Article WHERE uuid = 22346");
      assertEquals(resultSet.findColumn("title"), 1);
      assertEquals(resultSet.findColumn("uuid"), 2);
    }

    @Test
    public void columnsWithNullValuesAreReported2() throws Throwable {
      Statement statement = conn.createStatement();
      statement.execute("INSERT INTO Article (uuid, title, content) VALUES (22347, NULL, 'teh content')");
      statement.execute("INSERT INTO Article (uuid, title) VALUES (22348, 'jeejee')");

      ResultSet resultSet = statement.executeQuery("SELECT title, uuid FROM Article WHERE uuid = 22347 OR uuid = 22348");
      int titleColumn = resultSet.findColumn("title");
      int uuidColumn = resultSet.findColumn("uuid");

      assertTrue(resultSet.next());
      assertEquals("22347", resultSet.getString(uuidColumn));
      assertNull(resultSet.getString(titleColumn));
      assertTrue(resultSet.next());
      assertEquals("22348", resultSet.getString(uuidColumn));
      assertEquals("jeejee", resultSet.getString(titleColumn));
    }

    private void assertColumnLabelIndexInResultSet(ResultSet resultSet, String columnLabel, int expectedIndex) throws SQLException {
      int actualIndex = resultSet.findColumn(columnLabel);
      assertEquals("Column '" + columnLabel + "' index is wrong, ", expectedIndex, actualIndex);
    }

}
