package com.orientechnologies.orient.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

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
    
    private void assertColumnLabelIndexInResultSet(ResultSet resultSet, String columnLabel, int expectedIndex) throws SQLException {
      int actualIndex = resultSet.findColumn(columnLabel);
      assertEquals("Column '" + columnLabel + "' index is wrong, ", expectedIndex, actualIndex);
    }

}
