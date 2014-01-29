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

  @Test 
  public void getTablesReturnsColumnsInOrderSpecifiedInInterface() throws Throwable {
    ResultSet tables = conn.getMetaData().getTables(null, null, null, new String[] { "TABLE" } );
    assertColumnLabelIndexInResultSet(tables, "TABLE_CAT", 1);
    assertColumnLabelIndexInResultSet(tables, "TABLE_SCHEM", 2);
    assertColumnLabelIndexInResultSet(tables, "TABLE_NAME", 3);
    assertColumnLabelIndexInResultSet(tables, "TABLE_TYPE", 4);
    assertColumnLabelIndexInResultSet(tables, "REMARKS", 5);
    assertColumnLabelIndexInResultSet(tables, "TYPE_CAT", 6);
    assertColumnLabelIndexInResultSet(tables, "TYPE_SCHEM", 7);
    assertColumnLabelIndexInResultSet(tables, "TYPE_NAME", 8);
    assertColumnLabelIndexInResultSet(tables, "SELF_REFERENCING_COL_NAME", 9);
    assertColumnLabelIndexInResultSet(tables, "REF_GENERATION", 10);
  }
  
  @Test
  public void getColumnsReturnsColumnsInOrderSpecifiedInInterface() throws Throwable {
    ResultSet columns = conn.getMetaData().getColumns(null, null, "Item", null);
    assertColumnLabelIndexInResultSet(columns, "TABLE_CAT", 1);
    assertColumnLabelIndexInResultSet(columns, "TABLE_SCHEM", 2);
    assertColumnLabelIndexInResultSet(columns, "TABLE_NAME", 3);
    assertColumnLabelIndexInResultSet(columns, "COLUMN_NAME", 4);
    assertColumnLabelIndexInResultSet(columns, "DATA_TYPE", 5);
    assertColumnLabelIndexInResultSet(columns, "TYPE_NAME", 6);
    assertColumnLabelIndexInResultSet(columns, "COLUMN_SIZE", 7);
    assertColumnLabelIndexInResultSet(columns, "BUFFER_LENGTH", 8);
    assertColumnLabelIndexInResultSet(columns, "DECIMAL_DIGITS", 9);
    assertColumnLabelIndexInResultSet(columns, "NUM_PREC_RADIX", 10);
    assertColumnLabelIndexInResultSet(columns, "NULLABLE", 11);
    assertColumnLabelIndexInResultSet(columns, "REMARKS", 12);
    assertColumnLabelIndexInResultSet(columns, "COLUMN_DEF", 13);
    assertColumnLabelIndexInResultSet(columns, "SQL_DATA_TYPE", 14);
    assertColumnLabelIndexInResultSet(columns, "SQL_DATETIME_SUB", 15);
    assertColumnLabelIndexInResultSet(columns, "CHAR_OCTET_LENGTH", 16);
    assertColumnLabelIndexInResultSet(columns, "ORDINAL_POSITION", 17);
    assertColumnLabelIndexInResultSet(columns, "IS_NULLABLE", 18);
    assertColumnLabelIndexInResultSet(columns, "SCOPE_CATALOG", 19);
    assertColumnLabelIndexInResultSet(columns, "SCOPE_SCHEMA", 20);
    assertColumnLabelIndexInResultSet(columns, "SCOPE_TABLE", 21);
    assertColumnLabelIndexInResultSet(columns, "SOURCE_DATA_TYPE", 22);
    assertColumnLabelIndexInResultSet(columns, "IS_AUTOINCREMENT", 23);
  }
  
  @Test
  public void getColumnsNullableValueIsCorrectOnNotNullField() throws Throwable {
    ResultSet columns = conn.getMetaData().getColumns(null, null, "Article", "uuid");
    assertTrue("Expected to find 'uuid' field from Article-class", columns.next());
    assertEquals(DatabaseMetaData.attributeNoNulls, columns.getInt("NULLABLE"));
  }

  @Test
  public void getColumnsNullableValueIsCorrectOnNullableField() throws Throwable {
    ResultSet columns = conn.getMetaData().getColumns(null, null, "Article", "title");
    assertTrue("Expected to find 'title' field from Article-class", columns.next());
    assertEquals(DatabaseMetaData.attributeNullable, columns.getInt("NULLABLE"));
  }

  @Test
  public void getColumnsOrdinalPositionStaysTheSameOnMultipleQueries() throws Throwable {
    ResultSet columns = conn.getMetaData().getColumns(null, null, "Item", "intKey");
    int ordinalPositionFromDirectFetch = columns.getInt("ORDINAL_POSITION");
    
    int ordinalPositionWhenFetchingAll = -1;
    columns = conn.getMetaData().getColumns(null, null, null, null);
    while (columns.next()) {
      if ("intKey".equals(columns.getString("COLUMN_NAME"))) {
        ordinalPositionWhenFetchingAll = columns.getInt("ORDINAL_POSITION");
        break;
      }
    }
    assertEquals(ordinalPositionFromDirectFetch, ordinalPositionWhenFetchingAll);
  }
  
  private void assertColumnLabelIndexInResultSet(ResultSet resultSet, String columnLabel, int expectedIndex) throws SQLException {
    int actualIndex = resultSet.findColumn(columnLabel);
    assertEquals("Column '" + columnLabel + "' index is wrong, ", expectedIndex, actualIndex);
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
