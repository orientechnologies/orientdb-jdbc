/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import com.orientechnologies.common.log.OLogManager;

public class OrientJdbcDriver implements java.sql.Driver {

	private static final int MAJOR_VERSION = 0;
	private static final int MINOR_VERSION = 1;

	static {
		try {
			java.sql.DriverManager.registerDriver(new OrientJdbcDriver());
		} catch (SQLException e) {
			OLogManager.instance().error(null,
					"Error while registering the JDBC Driver");
		}
	}

	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith("orient:");
	}

	public Connection connect(String url, Properties info) throws SQLException {
		return new OrientJdbcConnection(url, info);
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		return null;
	}

	public boolean jdbcCompliant() {
		return true;
	}

	public int getMajorVersion() {
		return MAJOR_VERSION;
	}

	public int getMinorVersion() {
		return MINOR_VERSION;
	}
}