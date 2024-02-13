/*

Copyright (c) 2018-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.config.db;

import static org.lockss.config.db.SqlConstants.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.lockss.db.*;
import org.lockss.log.L4JLogger;

/**
 * The ConfigDbManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class ConfigDbManagerSql extends DbManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  // Query to create the table for recording plugins.
  private static final String CREATE_PLUGIN_TABLE_QUERY = "create table "
      + PLUGIN_TABLE + " ("
      + PLUGIN_SEQ_COLUMN + " --BigintSerialPk--,"
      + PLUGIN_ID_COLUMN + " varchar(" + MAX_PLUGIN_ID_COLUMN + ") not null"
      + ")";

  // Query to create the table for recording archival units.
  private static final String CREATE_ARCHIVAL_UNIT_TABLE_QUERY = "create table "
      + ARCHIVAL_UNIT_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " --BigintSerialPk--,"
      + PLUGIN_SEQ_COLUMN + " bigint not null references " + PLUGIN_TABLE
      + " (" + PLUGIN_SEQ_COLUMN + ") on delete cascade,"
      + ARCHIVAL_UNIT_KEY_COLUMN + " varchar(" + MAX_ARCHIVAL_UNIT_KEY_COLUMN
      + ") not null,"
      + CREATION_TIME_COLUMN + " bigint not null,"
      + LAST_UPDATE_TIME_COLUMN + " bigint not null"
      + ")";

  // Query to create the table for recording archival units configurations.
  private static final String CREATE_ARCHIVAL_UNIT_CONFIG_TABLE_QUERY =
      "create table "
      + ARCHIVAL_UNIT_CONFIG_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + CONFIG_KEY_COLUMN + " varchar(" + MAX_CONFIG_KEY_COLUMN + ") not null,"
      + CONFIG_VALUE_COLUMN + " varchar(" + MAX_CONFIG_VALUE_COLUMN
      + ") not null"
      + ")";

  // The SQL code used to create the necessary version 1 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_1_TABLE_CREATE_QUERIES =
    new LinkedHashMap<String, String>() {{
      put(PLUGIN_TABLE, CREATE_PLUGIN_TABLE_QUERY);
      put(ARCHIVAL_UNIT_TABLE, CREATE_ARCHIVAL_UNIT_TABLE_QUERY);
      put(ARCHIVAL_UNIT_CONFIG_TABLE, CREATE_ARCHIVAL_UNIT_CONFIG_TABLE_QUERY);
    }};

  // SQL statements that create the necessary version 1 indices.
  private static final String[] VERSION_1_INDEX_CREATE_QUERIES = new String[] {
      "create unique index idx1_" + PLUGIN_TABLE + " on " + PLUGIN_TABLE
      + "(" + PLUGIN_ID_COLUMN + ")",
      "create index idx1_" + ARCHIVAL_UNIT_TABLE + " on " + ARCHIVAL_UNIT_TABLE
      + "(" + ARCHIVAL_UNIT_KEY_COLUMN + ")",
      "create index idx2_" + ARCHIVAL_UNIT_TABLE + " on " + ARCHIVAL_UNIT_TABLE
      + "(" + PLUGIN_SEQ_COLUMN + ")",
      "create unique index idx1_" + ARCHIVAL_UNIT_CONFIG_TABLE + " on "
      + ARCHIVAL_UNIT_CONFIG_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN + ","
      + CONFIG_KEY_COLUMN + ")"
  };

  // SQL statements that create the necessary version 1 indices for MySQL.
  private static final String[] VERSION_1_INDEX_CREATE_MYSQL_QUERIES =
      new String[] {
	  // TODO: Make the index unique when MySQL is fixed.
	  "create index idx1_" + PLUGIN_TABLE + " on " + PLUGIN_TABLE
	  + "(" + PLUGIN_ID_COLUMN + "(255))",
	  "create index idx1_" + ARCHIVAL_UNIT_TABLE + " on "
	  + ARCHIVAL_UNIT_TABLE + "(" + ARCHIVAL_UNIT_KEY_COLUMN + "(255))",
	  "create index idx2_" + ARCHIVAL_UNIT_TABLE + " on "
	  + ARCHIVAL_UNIT_TABLE + "(" + PLUGIN_SEQ_COLUMN + ")",
	  "create unique index idx1_" + ARCHIVAL_UNIT_CONFIG_TABLE + " on "
	  + ARCHIVAL_UNIT_CONFIG_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN + ","
	  + CONFIG_KEY_COLUMN + ")"
  };

  // Query to create the table for recording archival units state properties.
  private static final String CREATE_ARCHIVAL_UNIT_STATE_TABLE_QUERY =
      "create table "
      + ARCHIVAL_UNIT_STATE_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + STATE_STRING_COLUMN + " varchar(" + MAX_STATE_STRING_COLUMN
      + ") not null"
      + ")";

  // The SQL code used to create the necessary version 2 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_2_TABLE_CREATE_QUERIES =
    new LinkedHashMap<String, String>() {{
      put(ARCHIVAL_UNIT_STATE_TABLE, CREATE_ARCHIVAL_UNIT_STATE_TABLE_QUERY);
    }};

  // Query to create the table for recording archival units agreements.
  private static final String CREATE_ARCHIVAL_UNIT_AGREEMENTS_TABLE_QUERY =
      "create table "
      + ARCHIVAL_UNIT_AGREEMENTS_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + AGREEMENTS_STRING_COLUMN + " varchar(" + MAX_AGREEMENTS_STRING_COLUMN
      + ") not null"
      + ")";

  // The SQL code used to create the necessary version 3 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_3_TABLE_CREATE_QUERIES =
    new LinkedHashMap<String, String>() {{
      put(ARCHIVAL_UNIT_AGREEMENTS_TABLE,
	  CREATE_ARCHIVAL_UNIT_AGREEMENTS_TABLE_QUERY);
    }};

  // SQL statements that create the necessary version 3 indices.
  private static final String[] VERSION_3_INDEX_CREATE_QUERIES = new String[] {
      "create unique index idx1_" + ARCHIVAL_UNIT_STATE_TABLE + " on "
	  + ARCHIVAL_UNIT_STATE_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN + ")",
      "create unique index idx1_" + ARCHIVAL_UNIT_AGREEMENTS_TABLE + " on "
	  + ARCHIVAL_UNIT_AGREEMENTS_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
	  + ")"
  };

  // Query to create the table for recording archival units suspect URL
  // versions.
  private static final String
  CREATE_ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE_QUERY = "create table "
      + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + SUSPECT_URL_VERSIONS_STRING_COLUMN + " varchar("
      + MAX_SUSPECT_URL_VERSIONS_STRING_COLUMN + ") not null"
      + ")";

  // Query to create the table for recording archival units no AU peer sets.
  private static final String CREATE_ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE_QUERY =
      "create table "
      + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + NO_AU_PEER_SET_STRING_COLUMN + " varchar("
      + MAX_NO_AU_PEER_SET_STRING_COLUMN + ") not null"
      + ")";

  // The SQL code used to create the necessary version 4 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_4_TABLE_CREATE_QUERIES =
    new LinkedHashMap<String, String>() {{
      put(ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE,
	  CREATE_ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE_QUERY);
      put(ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE,
	  CREATE_ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE_QUERY);
    }};

  // SQL statements that create the necessary version 4 indices.
  private static final String[] VERSION_4_INDEX_CREATE_QUERIES = new String[] {
      "create unique index idx1_" + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE
      + " on " + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE + "("
	  + ARCHIVAL_UNIT_SEQ_COLUMN + ")",
      "create unique index idx1_" + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE + " on "
	  + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
	  + ")"
  };

  /**
   * Constructor.
   * 
   * @param dataSource
   *          A DataSource with the datasource that provides the connection.
   * @param dataSourceClassName
   *          A String with the data source class name.
   * @param dataSourceUser
   *          A String with the data source user name.
   * @param maxRetryCount
   *          An int with the maximum number of retries to be attempted.
   * @param retryDelay
   *          A long with the number of milliseconds to wait between consecutive
   *          retries.
   * @param fetchSize
   *          An int with the SQL statement fetch size.
   */
  ConfigDbManagerSql(DbManager dbMgr,
                     DataSource dataSource, String dataSourceClassName,
                     String dataSourceUser,
                     int maxRetryCount, long retryDelay, int fetchSize)
      {
        super(dbMgr, dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
	retryDelay, fetchSize);
  }

  /**
   * Sets up the database to version 1.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred setting up the database.
   */
  void setUpDatabaseVersion1(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_1_TABLE_CREATE_QUERIES);

    // Create the necessary indices.
    if (isTypeMysql()) {
      executeDdlQueries(conn, VERSION_1_INDEX_CREATE_MYSQL_QUERIES);
    } else {
      executeDdlQueries(conn, VERSION_1_INDEX_CREATE_QUERIES);
    }

    log.debug2("Done");
  }

  /**
   * Updates the database from version 1 to version 2.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void updateDatabaseFrom1To2(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_2_TABLE_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 2 to version 3.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void updateDatabaseFrom2To3(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_3_TABLE_CREATE_QUERIES);

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_3_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 3 to version 4.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void updateDatabaseFrom3To4(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_4_TABLE_CREATE_QUERIES);

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_4_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }
}
