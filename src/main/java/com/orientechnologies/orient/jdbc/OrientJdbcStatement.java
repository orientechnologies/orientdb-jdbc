/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 * Copyright 2011 TXT e-solutions SpA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.jdbc;

import static java.util.Collections.emptyList;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.*;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSelect;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/**
 * @author Roberto Franchini (CELI Srl - franchini@celi.it)
 * @author Salvatore Piccione (TXT e-solutions SpA - salvo.picci@gmail.com)
 */
public class OrientJdbcStatement implements Statement {

  protected final OrientJdbcConnection connection;
  protected final OGraphDatabase       database;

  // protected OCommandSQL query;
  protected OCommandRequest            query;
  protected List<ODocument>            documents;
  protected boolean                    closed;

  private ResultSet                    resultSet;
  private int                          updateCount;

  protected List<String>               batches;

  protected int                        resultSetType;
  protected int                        resultSetConcurrency;
  protected int                        resultSetHoldability;

  public OrientJdbcStatement(final OrientJdbcConnection iConnection) {
    this(iConnection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  /**
   * @param iConnection
   * @param resultSetType
   * @param resultSetConcurrency
   * @throws SQLException
   */
  public OrientJdbcStatement(OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency) throws SQLException {
    this(iConnection, resultSetType, resultSetConcurrency, resultSetType);
  }

  /**
   * @param iConnection
   * @param resultSetType
   * @param resultSetConcurrency
   * @param resultSetHoldability
   */
  public OrientJdbcStatement(OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    this.connection = iConnection;
    this.database = iConnection.getDatabase();
    ODatabaseRecordThreadLocal.INSTANCE.set(database);
    documents = emptyList();
    batches = new ArrayList<String>();
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    setResult();
  }

  public boolean execute(final String sql) throws SQLException {
    return this.execute(sql, new Object[0]);
  }

  protected boolean execute(final String sql, Object... args) throws SQLException {
    if ("".equals(sql)) {
      setResult();
      return false;
    }

    query = new OCommandSQL(sql);
    try {

      Object rawResult = database.command(query).execute(args);
      if (rawResult instanceof List<?>) {
        documents = (List<ODocument>) rawResult;
      } else {
        Integer updateCount = (rawResult instanceof Integer) ? (Integer) rawResult : 1;
        setResult(updateCount);
        return false;
      }

    } catch (OQueryParsingException e) {
      throw new SQLSyntaxErrorException("Error on parsing the query", e);
    }

    List<String> searchedColumns;
    Map<String,Object> projections = new OCommandExecutorSQLSelect().parse(query).getProjections();
    if (projections != null && !projections.isEmpty()) {
      searchedColumns = new ArrayList<String>(projections.keySet());
    } else {
      Set<String> fields = new HashSet<String>();
      for (ODocument document : documents) {
        fields.addAll(Arrays.asList(document.fieldNames()));
      }
      searchedColumns = new ArrayList<String>(fields);
    }



    setResult(new OrientJdbcResultSet(this, searchedColumns, documents, resultSetType, resultSetConcurrency, resultSetHoldability));
    return true;

  }

  public ResultSet executeQuery(final String sql) throws SQLException {
    if (execute(sql))
      return getResultSet();
    else
      return null;
  }

  public int executeUpdate(final String sql) throws SQLException {
    query = new OCommandSQL(sql);
    Object rawResult = database.command(query).execute();

    if (rawResult instanceof ODocument)
      return 1;
    else if (rawResult instanceof Integer)
      return (Integer) rawResult;
    else
      return 0;
  }

  public int executeUpdate(final String sql, int autoGeneratedKeys) throws SQLException {

    return 0;
  }

  public int executeUpdate(final String sql, int[] columnIndexes) throws SQLException {

    return 0;
  }

  public int executeUpdate(final String sql, String[] columnNames) throws SQLException {

    return 0;
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public void close() throws SQLException {
    query = null;
    closed = true;
  }

  public boolean execute(final String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  public boolean execute(final String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  public boolean execute(final String sql, String[] columnNames) throws SQLException {
    return false;
  }

  public void addBatch(final String sql) throws SQLException {
    batches.add(sql);
  }

  public void cancel() throws SQLException {
  }

  public void clearBatch() throws SQLException {
    batches.clear();
  }

  public void clearWarnings() throws SQLException {
  }

  public int[] executeBatch() throws SQLException {
    int[] results = new int[batches.size()];
    int i = 0;
    for (String sql : batches) {
      results[i++] = executeUpdate(sql);
    }
    return results;
  }

  public int getFetchDirection() throws SQLException {

    return 0;
  }

  public int getFetchSize() throws SQLException {

    return 0;
  }

  public ResultSet getGeneratedKeys() throws SQLException {

    return null;
  }

  public int getMaxFieldSize() throws SQLException {

    return 0;
  }

  public int getMaxRows() throws SQLException {

    return 0;
  }

  public boolean getMoreResults() throws SQLException {

    return false;
  }

  public boolean getMoreResults(final int current) throws SQLException {

    return false;
  }

  public int getQueryTimeout() throws SQLException {

    return 0;
  }

  public ResultSet getResultSet() throws SQLException {
    try {
      return this.resultSet;
    } finally {
      setResult();
    }
  }

  public int getResultSetConcurrency() throws SQLException {

    return getResultSet().getConcurrency();
  }

  public int getResultSetHoldability() throws SQLException {

    return getResultSet().getHoldability();
  }

  public int getResultSetType() throws SQLException {

    return getResultSet().getType();
  }

  public int getUpdateCount() throws SQLException {
    if (isClosed())
      throw new SQLException("Statement already closed");

    try {
      return this.updateCount;
    } finally {
      setResult();
    }
  }

  public SQLWarning getWarnings() throws SQLException {

    return null;
  }

  public boolean isClosed() throws SQLException {

    return query == null;
  }

  public boolean isPoolable() throws SQLException {

    return false;
  }

  public void setCursorName(final String name) throws SQLException {

  }

  public void setEscapeProcessing(final boolean enable) throws SQLException {

  }

  public void setFetchDirection(final int direction) throws SQLException {

  }

  public void setFetchSize(final int rows) throws SQLException {

  }

  public void setMaxFieldSize(final int max) throws SQLException {

  }

  public void setMaxRows(final int max) throws SQLException {

  }

  public void setPoolable(final boolean poolable) throws SQLException {

  }

  public void setQueryTimeout(final int seconds) throws SQLException {

  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO This should check is this instance is a wrapper for the given
    // class
    try {
      // the following if-then structure makes sense if the query can be a
      // subclass of OCommandSQL.
      if (this.query == null)
        // if the query instance is null, we use the class OCommandSQL
        return OCommandSQL.class.isAssignableFrom(iface);
      else
        return this.query.getClass().isAssignableFrom(iface);
    } catch (NullPointerException e) {
      throw new SQLException(e);
    }
    // return false;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO This should return the actual query object: OCommandSQL, OQuery,
    // etc...
    try {
      return iface.cast(query);
    } catch (ClassCastException e) {
      throw new SQLException(e);
    }
    // return null;
  }

  public void closeOnCompletion() throws SQLException {

  }

  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  public void setResult(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.updateCount = -1;
  }

  public void setResult(int updateCount) {
    this.updateCount = updateCount;
    this.resultSet = null;
  }

  public void setResult() {
    this.resultSet = null;
    this.updateCount = -1;
  }
}
