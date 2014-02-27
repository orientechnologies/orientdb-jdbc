package com.orientechnologies.orient.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.*;

public class OrientJdbcStatementTest extends OrientJdbcBaseTest {

	@Test
	public void shouldCreateStatement() throws Exception {
		Statement stmt = conn.createStatement();
		assertNotNull(stmt);
		stmt.close();
		assertTrue(stmt.isClosed());

	}

	@Test
	public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
		Statement stmt = conn.createStatement();
		assertThat(stmt.execute(""), is(false));
		assertThat(stmt.getResultSet(), is(nullValue()));
		assertTrue(!stmt.getMoreResults());
	}

	@Test
	public void shouldExectuteSelectOne() throws SQLException {

		Statement st = conn.createStatement();
		assertThat(st.execute("select 1"), is(true));
    ResultSet resultSet = st.getResultSet();
    assertNotNull(resultSet);
		resultSet.first();
		int one = resultSet.getInt("1");
		assertThat(one, is(1));
		assertThat(st.getMoreResults(), is(false));

	}

  @Test
  public void getResultSetReturnsNullOnConsecutiveCalls() throws SQLException {
    Statement st = conn.createStatement();
    assertThat(st.execute("select 1"), is(true));
    assertNotNull(st.getResultSet());
    assertEquals(null, st.getResultSet());
  }

  @Test
  public void getUpdateCountReturnsMinusOneOnSelectQueries() throws SQLException {
    Statement st = conn.createStatement();
    assertThat(st.execute("select 1"), is(true));
    assertEquals(-1, st.getUpdateCount());
  }

  @Test
  public void updateCountIsReportedCorrectly() throws Exception {
    Statement statement = conn.createStatement();
    assertFalse(statement.execute("UPDATE Item SET published = true;"));

    assertEquals(20, statement.getUpdateCount());
  }

  @Test
  public void updateCountIsMinusOneOnConsecutiveCalls() throws Exception {
    Statement statement = conn.createStatement();
    assertFalse(statement.execute("UPDATE Item SET published = true;"));

    statement.getUpdateCount();
    assertEquals(-1, statement.getUpdateCount());
  }

  @Test
  public void getResultSetReturnsNullOnUpdates() throws Exception {
    Statement statement = conn.createStatement();
    assertFalse(statement.execute("UPDATE Item SET published = true;"));

    assertEquals(null,statement.getResultSet());
  }
}
