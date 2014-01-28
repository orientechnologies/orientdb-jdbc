package com.orientechnologies.orient.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.OConstants;

public class OrientJdbcDatabaseMetaDataTest extends OrientJdbcBaseTest {

  private DatabaseMetaData metaData;

  @Before
  public void setup() throws SQLException {
    metaData = conn.getMetaData();

  }

  @Test
  public void verifyDriverAndDatabaseVersions() throws SQLException {

    assertEquals("memory:test", metaData.getURL());
    assertEquals("admin", metaData.getUserName());
    assertEquals("OrientDB", metaData.getDatabaseProductName());
    assertEquals(OConstants.ORIENT_VERSION, metaData.getDatabaseProductVersion());
    assertEquals(1, metaData.getDatabaseMajorVersion());
    assertEquals(6, metaData.getDatabaseMinorVersion());

    assertEquals("OrientDB JDBC Driver", metaData.getDriverName());
    assertEquals("OrientDB 1.6 JDBC Driver", metaData.getDriverVersion());
    assertEquals(1, metaData.getDriverMajorVersion());
    assertEquals(6, metaData.getDriverMinorVersion());

  }

  @Test
  public void shouldRetrievePrimaryKeysMetadata() throws SQLException {

    ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "Item");
    assertTrue(primaryKeys.next());
    assertEquals("intKey", primaryKeys.getString(4));
    assertEquals("Item.intKey", primaryKeys.getString(6));
    assertEquals(1, primaryKeys.getInt(5));

    assertTrue(primaryKeys.next());
    assertEquals("stringKey", primaryKeys.getString("COLUMN_NAME"));
    assertEquals("Item.stringKey", primaryKeys.getString("PK_NAME"));
    assertEquals(1, primaryKeys.getInt("KEY_SEQ"));

  }

  @Test
  public void shouldRetrieveTableTypes() throws SQLException {

    ResultSet tableTypes = metaData.getTableTypes();
    assertTrue(tableTypes.next());
    assertEquals("TABLE", tableTypes.getString(1));

    assertFalse(tableTypes.next());

  }

  @Test
  public void getSingleFieldByName() throws SQLException {
    ResultSet rs = conn.createStatement().executeQuery("select from OUser");
    if (rs != null) {
      ResultSetMetaData metaData = rs.getMetaData();
      int cc = metaData.getColumnCount();
      Set<String> colset = new HashSet<String>();
      List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>(cc);
      for (int i = 1; i <= cc; i++) {
        String name = metaData.getColumnLabel(i);
        if (colset.contains(name))
          continue;
        colset.add(name);
        Map<String, Object> field = new HashMap<String, Object>();
        field.put("name", name);

        try {
          String catalog = metaData.getCatalogName(i);
          String schema = metaData.getSchemaName(i);
          String table = metaData.getTableName(i);
          ResultSet rsmc = conn.getMetaData().getColumns(catalog, schema, table, name);
          while (rsmc.next()) {
            field.put("description", rsmc.getString("REMARKS"));
            break;
          }
        } catch (SQLException se) {
          se.printStackTrace();
        }
        columns.add(field);
      }

      for (Map<String, Object> c : columns) {
        System.out.println(c);
      }
    }
  }
  
  @Test 
  public void getFieldsWithWildcardAtBeginning() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("%Key", "stringKey", "intKey");
  }

  @Test 
  public void getFieldsWithWildcardAtEnd() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("t%", "time", "text", "title", "tags");
  }

  @Test 
  public void getFieldsWithWildcardAtCenter() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("%th%", "author", "length");
  }

  @Test 
  public void getFieldsWithSingleCharacterWildcardAtCenter() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("aut_or", "author");
  }

  @Test 
  public void getFieldsWithSingleCharacterWildcardAtEnd() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("autho_", "author");
  }

  @Test 
  public void getFieldsWithSingleCharacterWildcardAtBeginning() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("_uthor", "author");
  }

  @Test 
  public void getFieldsWithMultipleSingleCharacterWildcards() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern("t___", "time", "text", "tags");
  }

  @Test 
  public void getFieldsWithNullRetunsAllFields() throws Throwable {
    assertFieldsInItemClassByColumnNamePattern(null, "stringKey", "intKey", "date", "time", "text","length", "published", "title","author", "tags");
  }

  @Test
  public void searchFieldsFromMultipleClasses() throws Throwable {
    Collection<String> expectedTableNames = new ArrayList<String>();
    expectedTableNames.add("Article");
    expectedTableNames.add("Author");
    ResultSet columns = conn.getMetaData().getColumns(null, null, null, "uuid");
    while (columns.next()) {
      String tableName = columns.getString("TABLE_NAME");
      assertTrue("Returned table '" + tableName + "' is not in expected tables", expectedTableNames.remove(tableName));
    }
    assertEquals(0, expectedTableNames.size());
  }

  @Test
  public void searchFieldsFromClassesByTableNameWildcard() throws Throwable {
    Collection<String> expectedTableNames = new ArrayList<String>();
    expectedTableNames.add("Article");

    ResultSet columns = conn.getMetaData().getColumns(null, null, "A%", "title");
    while (columns.next()) {
      String tableName = columns.getString("TABLE_NAME");
      assertTrue("Returned table '" + tableName + "' is not in expected tables", expectedTableNames.remove(tableName));
    }
    assertEquals(0, expectedTableNames.size());
  }
  
  @Test 
  public void tablesCanBeSearchedWithWildcard() throws Throwable {
    Collection<String> expectedTableNames = new ArrayList<String>();
    expectedTableNames.add("Article");
    expectedTableNames.add("Author");
    
    ResultSet columns = conn.getMetaData().getTables(null, null, "A%", null);
    while (columns.next()) {
      String tableName = columns.getString("TABLE_NAME");
      assertTrue("Returned table '" + tableName + "' is not in expected tables", expectedTableNames.remove(tableName));
    }
  }
  
  @Test 
  public void tablescanBeSearchedWithType() throws Throwable {
    Collection<String> expectedTableNames = new ArrayList<String>();
    expectedTableNames.add("Article");
    expectedTableNames.add("Author");
    expectedTableNames.add("Item");
    expectedTableNames.add("V");
    expectedTableNames.add("E");
    
    ResultSet columns = conn.getMetaData().getTables(null, null, null, new String[] { "TABLE" } );
    while (columns.next()) {
      String tableName = columns.getString("TABLE_NAME");
      assertTrue("Returned table '" + tableName + "' is not in expected tables", expectedTableNames.remove(tableName));
    }
  }

  private void assertFieldsInItemClassByColumnNamePattern(String columnNamePattern, String... expectedColumnNames) throws Throwable {
    Collection<String> expectedColumns = new ArrayList<String>(Arrays.asList(expectedColumnNames));

    DatabaseMetaData metadata = conn.getMetaData();
    ResultSet columns = metadata.getColumns(null, null, "Item", columnNamePattern);
    while (columns.next()) {
      String columnName = columns.getString("COLUMN_NAME");
      assertTrue("Returned column '" + columnName + "' is not in expected columns", expectedColumns.remove(columnName));
    }
    assertEquals(0, expectedColumns.size());
  }
}
