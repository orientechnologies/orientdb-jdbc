package com.orientechnologies.orient.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

public class OrientJdbcPreparedStatementTest extends OrientJdbcBaseTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("SELECT * FROM Item WHERE stringKey = ? OR intKey = ?");
    assertNotNull(stmt);
    stmt.close();
    assertTrue(stmt.isClosed());

  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("");
    assertThat(stmt.execute(""), is(false));

    assertThat(stmt.getResultSet(), is(nullValue()));
    assertThat(stmt.getMoreResults(), is(false));
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {
    PreparedStatement stmt = conn.prepareStatement("select 1");
    assertThat(stmt.execute(), is(true));
    assertNotNull(stmt.getResultSet());
    ResultSet resultSet = stmt.getResultSet();
    resultSet.first();
    int one = resultSet.getInt("1");
    assertThat(one, is(1));
    assertThat(stmt.getMoreResults(), is(false));

  }

  @Test
  public void shouldExecutePreparedStatement() throws Exception {
    PreparedStatement stmt = conn.prepareStatement("SELECT  " + "FROM Item " + "WHERE stringKey = ? OR intKey = ?");
    assertNotNull(stmt);

    stmt.setString(1, "1");
    stmt.setInt(2, 1);

    ResultSet rs = stmt.executeQuery();
    assertTrue(rs.next());

    assertThat(rs.getInt("@version"), equalTo(0));

    assertThat(rs.getString("@class"), equalTo("Item"));

    assertThat(rs.getString("stringKey"), equalTo("1"));
    assertThat(rs.getInt("intKey"), equalTo(1));

    assertThat(rs.getDate("date").toString(), equalTo(new java.sql.Date(System.currentTimeMillis()).toString()));
    assertThat(rs.getDate("time").toString(), equalTo(new java.sql.Date(System.currentTimeMillis()).toString()));

    stmt.close();
    assertTrue(stmt.isClosed());

  }
  
  @Test
  public void executePutsParametersToQuery() throws SQLException {
    PreparedStatement statement = conn.prepareStatement("SELECT  " + "FROM Item " + "WHERE stringKey = ? OR intKey = ?");

    statement.setString(1, "1");
    statement.setInt(2, 1);

    assertTrue(statement.execute());
    ResultSet resultSet = statement.getResultSet();

    assertThat(resultSet.getString("stringKey"), equalTo("1"));
    assertThat(resultSet.getInt("intKey"), equalTo(1));
  }
  
  @Test
  public void withInsertQueryExecuteReturnsFalseAndUpdateCountCanBeQueried() throws Throwable {
    PreparedStatement statement = conn.prepareStatement("INSERT INTO Article (uuid, title) VALUES (22341, 'TitleFor22341)");
    assertFalse(statement.execute());
    assertEquals(1, statement.getUpdateCount());
  }

  @Test
  public void updateCountOnMultipleUpdatesIsReportedCorrectly() throws Throwable {
    PreparedStatement statement = conn.prepareStatement("UPDATE Item SET published = true;");
    assertFalse(statement.execute());

    assertEquals(20, statement.getUpdateCount());
  }
}
