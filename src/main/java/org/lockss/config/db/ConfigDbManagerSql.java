/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.lockss.db.DbManagerSql;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;

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

  // Query to create the table for recording different archival units state
  // access type values.
  private static final String CREATE_STATE_ACCESS_TYPE_TABLE_QUERY =
      "create table "
      + STATE_ACCESS_TYPE_TABLE + " ("
      + STATE_ACCESS_TYPE_SEQ_COLUMN + " --BigintSerialPk--,"
      + STATE_ACCESS_TYPE_COLUMN + " varchar(" + MAX_STATE_ACCESS_TYPE_COLUMN
      + ")"
      + ")";

  // Query to create the table for recording different archival units state
  // substance existence values.
  private static final String CREATE_STATE_HAS_SUBSTANCE_TABLE_QUERY =
      "create table "
      + STATE_HAS_SUBSTANCE_TABLE + " ("
      + STATE_HAS_SUBSTANCE_SEQ_COLUMN + " --BigintSerialPk--,"
      + STATE_HAS_SUBSTANCE_COLUMN + " varchar("
      + MAX_STATE_HAS_SUBSTANCE_COLUMN + ")"
      + ")";

  // Query to create the table for recording different archival units state
  // last crawl result message values.
  private static final String CREATE_STATE_LAST_CRAWL_RESULT_MSG_TABLE_QUERY =
      "create table "
      + STATE_LAST_CRAWL_RESULT_MSG_TABLE + " ("
      + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN + " --BigintSerialPk--,"
      + STATE_LAST_CRAWL_RESULT_MSG_COLUMN + " varchar("
      + MAX_STATE_LAST_CRAWL_RESULT_MSG_COLUMN + ")"
      + ")";

  // Query to create the table for recording different archival units state
  // CDN stem values.
  private static final String CREATE_STATE_CDN_STEM_TABLE_QUERY =
      "create table "
      + STATE_CDN_STEM_TABLE + " ("
      + STATE_CDN_STEM_SEQ_COLUMN + " --BigintSerialPk--,"
      + STATE_CDN_STEM_COLUMN + " varchar(" + MAX_STATE_CDN_STEM_COLUMN + ")"
      + ")";

  // Query to create the table for recording archival units state CDN stem
  // values.
  private static final String CREATE_ARCHIVAL_UNIT_CDN_STEM_TABLE_QUERY =
      "create table "
      + ARCHIVAL_UNIT_CDN_STEM_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + STATE_CDN_STEM_SEQ_COLUMN + " bigint not null references "
      + STATE_CDN_STEM_TABLE + " (" + STATE_CDN_STEM_SEQ_COLUMN
      + ") on delete cascade,"
      + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN + " smallint not null"
      + ")";

  // Query to create the table for recording archival units state properties.
  private static final String CREATE_ARCHIVAL_UNIT_STATE_TABLE_QUERY =
      "create table "
      + ARCHIVAL_UNIT_STATE_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + LAST_CRAWL_TIME_COLUMN + " bigint not null,"
      + LAST_CRAWL_ATTEMPT_COLUMN + " bigint not null,"
      + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN + " bigint not null references "
      + STATE_LAST_CRAWL_RESULT_MSG_TABLE
      + " (" + STATE_LAST_CRAWL_RESULT_MSG_SEQ_COLUMN + ") on delete cascade,"
      + LAST_CRAWL_RESULT_COLUMN + " integer not null,"
      + LAST_TOP_LEVEL_POLL_TIME_COLUMN + " bigint not null,"
      + LAST_POLL_START_COLUMN + " bigint not null,"
      + LAST_POLL_RESULT_COLUMN + " integer not null,"
      + POLL_DURATION_COLUMN + " bigint not null,"
      + AVERAGE_HASH_DURATION_COLUMN + " bigint not null,"
      + V3_AGREEMENT_COLUMN + " double precision not null,"
      + HIGHEST_V3_AGREEMENT_COLUMN + " double precision not null,"
      + STATE_ACCESS_TYPE_SEQ_COLUMN + " bigint not null references "
      + STATE_ACCESS_TYPE_TABLE + " (" + STATE_ACCESS_TYPE_SEQ_COLUMN
      + ") on delete cascade,"
      + STATE_HAS_SUBSTANCE_SEQ_COLUMN + " bigint not null references "
      + STATE_HAS_SUBSTANCE_TABLE + " (" + STATE_HAS_SUBSTANCE_SEQ_COLUMN
      + ") on delete cascade,"
      + SUBSTANCE_VERSION_COLUMN + " varchar(" + MAX_SUBSTANCE_VERSION_COLUMN
      + "),"
      + METADATA_VERSION_COLUMN + " varchar(" + MAX_METADATA_VERSION_COLUMN
      + "),"
      + LAST_METADATA_INDEX_COLUMN + " bigint not null,"
      + LAST_CONTENT_CHANGE_COLUMN + " bigint not null,"
      + LAST_POP_POLL_COLUMN + " bigint not null,"
      + LAST_POP_POLL_RESULT_COLUMN + " integer not null,"
      + LAST_LOCAL_HASH_SCAN_COLUMN + " bigint not null,"
      + NUM_AGREE_PEERS_LAST_POR_COLUMN + " integer not null,"
      + NUM_WILLING_REPAIRERS_COLUMN + " integer not null,"
      + NUM_CURRENT_SUSPECT_VERSIONS_COLUMN + " integer not null"
      + ")";

  // The SQL code used to create the necessary version 2 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_2_TABLE_CREATE_QUERIES =
    new LinkedHashMap<String, String>() {{
      put(STATE_ACCESS_TYPE_TABLE, CREATE_STATE_ACCESS_TYPE_TABLE_QUERY);
      put(STATE_HAS_SUBSTANCE_TABLE, CREATE_STATE_HAS_SUBSTANCE_TABLE_QUERY);
      put(STATE_LAST_CRAWL_RESULT_MSG_TABLE,
	  CREATE_STATE_LAST_CRAWL_RESULT_MSG_TABLE_QUERY);
      put(STATE_CDN_STEM_TABLE, CREATE_STATE_CDN_STEM_TABLE_QUERY);
      put(ARCHIVAL_UNIT_CDN_STEM_TABLE,
	  CREATE_ARCHIVAL_UNIT_CDN_STEM_TABLE_QUERY);
      put(ARCHIVAL_UNIT_STATE_TABLE, CREATE_ARCHIVAL_UNIT_STATE_TABLE_QUERY);
    }};

  // SQL statements that create the necessary version 2 indices.
  private static final String[] VERSION_2_INDEX_CREATE_QUERIES = new String[] {
      "create unique index idx1_" + STATE_ACCESS_TYPE_TABLE + " on "
	  + STATE_ACCESS_TYPE_TABLE + "(" + STATE_ACCESS_TYPE_COLUMN + ")",
      "create unique index idx1_" + STATE_HAS_SUBSTANCE_TABLE + " on "
	  + STATE_HAS_SUBSTANCE_TABLE + "(" + STATE_HAS_SUBSTANCE_COLUMN + ")",
      "create unique index idx1_" + STATE_LAST_CRAWL_RESULT_MSG_TABLE + " on "
	  + STATE_LAST_CRAWL_RESULT_MSG_TABLE
	  + "(" + STATE_LAST_CRAWL_RESULT_MSG_COLUMN + ")",
      "create unique index idx1_" + STATE_CDN_STEM_TABLE + " on "
	  + STATE_CDN_STEM_TABLE + "(" + STATE_CDN_STEM_COLUMN + ")",
      "create unique index idx1_" + ARCHIVAL_UNIT_CDN_STEM_TABLE + " on "
	  + ARCHIVAL_UNIT_CDN_STEM_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN + ","
	  + STATE_CDN_STEM_SEQ_COLUMN + "," + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN
	  + ")"
    };

  // SQL statements that create the necessary version 1 indices for MySQL.
  private static final String[] VERSION_2_INDEX_CREATE_MYSQL_QUERIES =
      new String[] {
	  "create unique index idx1_" + STATE_ACCESS_TYPE_TABLE + " on "
	      + STATE_ACCESS_TYPE_TABLE + "(" + STATE_ACCESS_TYPE_COLUMN + ")",
	  "create unique index idx1_" + STATE_HAS_SUBSTANCE_TABLE + " on "
	      + STATE_HAS_SUBSTANCE_TABLE
	      + "(" + STATE_HAS_SUBSTANCE_COLUMN + ")",
	      // TODO: Make the index unique when MySQL is fixed.
	  "create index idx1_" + STATE_LAST_CRAWL_RESULT_MSG_TABLE
	      + " on " + STATE_LAST_CRAWL_RESULT_MSG_TABLE
	      + "(" + STATE_LAST_CRAWL_RESULT_MSG_COLUMN + "(255))",
	  "create unique index idx1_" + STATE_CDN_STEM_TABLE + " on "
	      + STATE_CDN_STEM_TABLE + "(" + STATE_CDN_STEM_COLUMN + ")",
	  "create unique index idx1_" + ARCHIVAL_UNIT_CDN_STEM_TABLE + " on "
	      + ARCHIVAL_UNIT_CDN_STEM_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
	      + "," + STATE_CDN_STEM_SEQ_COLUMN + ","
	      + ARCHIVAL_UNIT_CDN_STEM_IDX_COLUMN + ")"
    };

  // Query to insert a type of state access.
  private static final String INSERT_STATE_ACCESS_TYPE_QUERY = "insert into "
      + STATE_ACCESS_TYPE_TABLE
      + "(" + STATE_ACCESS_TYPE_SEQ_COLUMN
      + "," + STATE_ACCESS_TYPE_COLUMN
      + ") values (default,?)";

  // Query to insert a type of state has substance.
  private static final String INSERT_STATE_HAS_SUBSTANCE_QUERY = "insert into "
      + STATE_HAS_SUBSTANCE_TABLE
      + "(" + STATE_HAS_SUBSTANCE_SEQ_COLUMN
      + "," + STATE_HAS_SUBSTANCE_COLUMN
      + ") values (default,?)";

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
  ConfigDbManagerSql(DataSource dataSource, String dataSourceClassName,
      String dataSourceUser, int maxRetryCount, long retryDelay, int fetchSize)
      {
    super(dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
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

    // Create the necessary indices.
    if (isTypeMysql()) {
      executeDdlQueries(conn, VERSION_2_INDEX_CREATE_MYSQL_QUERIES);
    } else {
      executeDdlQueries(conn, VERSION_2_INDEX_CREATE_QUERIES);
    }

    // Initialize necessary data in new tables.
    addStateAccessType(conn, STATE_ACCESS_TYPE_NOT_SET);
    addStateAccessType(conn, STATE_ACCESS_TYPE_OPEN_ACCESS);
    addStateAccessType(conn, STATE_ACCESS_TYPE_SUBSCRIPTION);

    addStateHasSubstanceType(conn, STATE_HAS_SUBSTANCE_TYPE_UNKNOWN);
    addStateHasSubstanceType(conn, STATE_HAS_SUBSTANCE_TYPE_YES);
    addStateHasSubstanceType(conn, STATE_HAS_SUBSTANCE_TYPE_NO);

    log.debug2("Done.");
  }

  /**
   * Adds an Archival Unit state access type to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param typeName
   *          A String with the name of the type to be added.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private void addStateAccessType(Connection conn, String typeName)
      throws SQLException {
    log.debug2("typeName = '{}'", typeName);

    // Ignore an empty state access type.
    if (StringUtil.isNullString(typeName)) {
      return;
    }

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    PreparedStatement insertStateAccessType = null;

    try {
      insertStateAccessType = prepareStatement(conn,
	  INSERT_STATE_ACCESS_TYPE_QUERY);
      insertStateAccessType.setString(1, typeName);

      int count = executeUpdate(insertStateAccessType);
      log.trace("count = {}", count);
    } catch (SQLException sqle) {
      log.error("Cannot add a state access type", sqle);
      log.error("typeName = '{}'", typeName);
      log.error("SQL = '{}'", INSERT_STATE_ACCESS_TYPE_QUERY);
      throw sqle;
    } catch (RuntimeException re) {
      log.error("Cannot add a state access type", re);
      log.error("typeName = '{}'", typeName);
      log.error("SQL = '{}'", INSERT_STATE_ACCESS_TYPE_QUERY);
      throw re;
    } finally {
      safeCloseStatement(insertStateAccessType);
    }

    log.debug2("Done.");
  }

  /**
   * Adds an Archival Unit state has substance type to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param typeName
   *          A String with the name of the type to be added.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private void addStateHasSubstanceType(Connection conn, String typeName)
      throws SQLException {
    log.debug2("typeName = '{}'", typeName);

    // Ignore an empty state has substance type.
    if (StringUtil.isNullString(typeName)) {
      return;
    }

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    PreparedStatement insertStateHasSubstanceType = null;

    try {
      insertStateHasSubstanceType = prepareStatement(conn,
	  INSERT_STATE_HAS_SUBSTANCE_QUERY);
      insertStateHasSubstanceType.setString(1, typeName);

      int count = executeUpdate(insertStateHasSubstanceType);
      log.trace("count = {}", count);
    } catch (SQLException sqle) {
      log.error("Cannot add a state has substance type", sqle);
      log.error("typeName = '{}'", typeName);
      log.error("SQL = '{}'", INSERT_STATE_HAS_SUBSTANCE_QUERY);
      throw sqle;
    } catch (RuntimeException re) {
      log.error("Cannot add a state has substance type", re);
      log.error("typeName = '{}'", typeName);
      log.error("SQL = '{}'", INSERT_STATE_HAS_SUBSTANCE_QUERY);
      throw re;
    } finally {
      safeCloseStatement(insertStateHasSubstanceType);
    }

    log.debug2("Done.");
  }
}
