/*

Copyright (c) 2018-2024 Board of Trustees of Leland Stanford Jr. University,
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
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

  // SQL statements that create the necessary version 6 indices.
  //
  // The unique index on (plugin_seq, archival_unit_key) is the conflict
  // target of ConfigManagerSql's INSERT ... ON CONFLICT upsert, which is what
  // makes concurrent find-or-create of the same AU race-free.
  private static final String[] VERSION_6_INDEX_CREATE_QUERIES = new String[] {
      "create unique index idx3_" + ARCHIVAL_UNIT_TABLE + " on "
      + ARCHIVAL_UNIT_TABLE + "(" + PLUGIN_SEQ_COLUMN + ","
      + ARCHIVAL_UNIT_KEY_COLUMN + ")"
  };

  // Query to find the (plugin, key) pairs that have more than one
  // archival_unit row.
  private static final String FIND_DUPLICATE_ARCHIVAL_UNITS_QUERY = "select "
      + "p." + PLUGIN_ID_COLUMN
      + ", a." + PLUGIN_SEQ_COLUMN
      + ", a." + ARCHIVAL_UNIT_KEY_COLUMN
      + ", count(*)"
      + " from " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + PLUGIN_TABLE + " p"
      + " where a." + PLUGIN_SEQ_COLUMN + " = p." + PLUGIN_SEQ_COLUMN
      + " group by p." + PLUGIN_ID_COLUMN
      + ", a." + PLUGIN_SEQ_COLUMN
      + ", a." + ARCHIVAL_UNIT_KEY_COLUMN
      + " having count(*) > 1";

  // Query to list the archival_unit rows of one (plugin, key) pair, newest
  // (highest sequence number) first.
  private static final String FIND_ARCHIVAL_UNIT_SEQS_QUERY = "select "
      + ARCHIVAL_UNIT_SEQ_COLUMN
      + " from " + ARCHIVAL_UNIT_TABLE
      + " where " + PLUGIN_SEQ_COLUMN + " = ?"
      + " and " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " order by " + ARCHIVAL_UNIT_SEQ_COLUMN + " desc";

  // Query to delete the archival_unit rows of a (plugin, key) pair other than
  // the survivor. Child rows go with them via "on delete cascade".
  private static final String DELETE_OLDER_ARCHIVAL_UNITS_QUERY = "delete from "
      + ARCHIVAL_UNIT_TABLE
      + " where " + PLUGIN_SEQ_COLUMN + " = ?"
      + " and " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and " + ARCHIVAL_UNIT_SEQ_COLUMN + " <> ?";

  // Query to give the surviving archival_unit row the earliest creation_time
  // and the latest last_update_time of its (plugin, key) group. Run before the
  // older rows are deleted.
  private static final String MERGE_ARCHIVAL_UNIT_TIMES_QUERY = "update "
      + ARCHIVAL_UNIT_TABLE
      + " set " + CREATION_TIME_COLUMN + " = (select min("
      + CREATION_TIME_COLUMN + ") from " + ARCHIVAL_UNIT_TABLE
      + " where " + PLUGIN_SEQ_COLUMN + " = ?"
      + " and " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?)"
      + ", " + LAST_UPDATE_TIME_COLUMN + " = (select max("
      + LAST_UPDATE_TIME_COLUMN + ") from " + ARCHIVAL_UNIT_TABLE
      + " where " + PLUGIN_SEQ_COLUMN + " = ?"
      + " and " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?)"
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  // The tables keyed by archival_unit_seq whose rows must be carried over to
  // the surviving archival_unit row when duplicates are merged.
  private static final String[] ARCHIVAL_UNIT_CHILD_TABLES = new String[] {
      ARCHIVAL_UNIT_STATE_TABLE,
      ARCHIVAL_UNIT_AGREEMENTS_TABLE,
      ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE,
      ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE,
      ARCHIVAL_UNIT_CONFIG_TABLE
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

  private String widenColumnQuery(String tableName, String columnName) {
    if (dbMgr.isTypeDerby()) {
      return "alter table " + tableName
        + " alter column " + columnName
        + " SET DATA TYPE VARCHAR(" + DERBY_MAX_VARCHAR + ")";
    } else if (dbMgr.isTypePostgresql() || dbMgr.isTypeMysql()) {
      return "alter table " + tableName
        + " alter column " + columnName
        + " SET DATA TYPE TEXT USING " + columnName + " :: TEXT";
    } else throw new IllegalStateException("Unknown DB type");
  }

  /**
   * Updates the database from version 4 to version 5.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void updateDatabaseFrom4To5(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    String[] queries = {
      widenColumnQuery(ARCHIVAL_UNIT_STATE_TABLE,
                       STATE_STRING_COLUMN),
      widenColumnQuery(ARCHIVAL_UNIT_AGREEMENTS_TABLE,
                       AGREEMENTS_STRING_COLUMN),
      widenColumnQuery(ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE,
                       SUSPECT_URL_VERSIONS_STRING_COLUMN),
      widenColumnQuery(ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE,
                       NO_AU_PEER_SET_STRING_COLUMN),
    };

    executeDdlQueries(conn, queries);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 5 to version 6.
   * <p>
   * Before version 6 nothing prevented two concurrent find-or-create calls
   * from each inserting an archival_unit row for the same (plugin, key), so
   * any such duplicates are merged first and then the unique index that
   * makes the upsert possible is created.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void updateDatabaseFrom5To6(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    mergeDuplicateArchivalUnits(conn);

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_6_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Merges archival_unit rows that share the same (plugin_seq,
   * archival_unit_key).
   * <p>
   * Policy: the row with the highest archival_unit_seq survives. For each
   * child table (archival_unit_state, archival_unit_agreements,
   * archival_unit_suspect_url_versions, archival_unit_no_au_peer_set,
   * archival_unit_config) the survivor's own rows are kept if it has any;
   * otherwise the rows of the highest-numbered older duplicate that has any
   * are re-pointed to the survivor. No child table carries a timestamp of its
   * own, so there is nothing finer to choose by. Configuration rows are moved
   * as a whole set and never merged key by key. The survivor takes the
   * earliest creation_time and the latest last_update_time of the group. The
   * older rows are then deleted, cascading to whatever child rows were not
   * carried over. One
   * warning is logged per merged AU.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  void mergeDuplicateArchivalUnits(Connection conn) throws SQLException {
    // Collect the duplicate groups first so no cursor is open while rows are
    // being moved around.
    List<Object[]> groups = new ArrayList<>();

    PreparedStatement findDups = null;
    ResultSet rs = null;

    try {
      findDups = prepareStatement(conn, FIND_DUPLICATE_ARCHIVAL_UNITS_QUERY);
      rs = executeQuery(findDups);

      while (rs.next()) {
        groups.add(new Object[] {rs.getString(1), rs.getLong(2),   // plugin_id, plugin_seq
                                 rs.getString(3), rs.getLong(4)}); // au_key, count_of_duplicates
      }
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(findDups);
    }

    for (Object[] group : groups) {
      String pluginId = (String)group[0];
      long pluginSeq = (Long)group[1];
      String auKey = (String)group[2];
      long count = (Long)group[3];

      mergeDuplicateArchivalUnit(conn, pluginSeq, auKey);

      log.warn("Merged {} duplicate archival_unit rows for plugin {} key {}",
               count, pluginId, auKey);
    }
  }

  /**
   * Merges the archival_unit rows of one (plugin_seq, archival_unit_key)
   * pair into the one with the highest archival_unit_seq.
   */
  private void mergeDuplicateArchivalUnit(Connection conn, long pluginSeq,
      String auKey) throws SQLException {
    // Newest first.
    List<Long> seqs = new ArrayList<>();

    PreparedStatement findSeqs = null;
    ResultSet rs = null;

    try {
      findSeqs = prepareStatement(conn, FIND_ARCHIVAL_UNIT_SEQS_QUERY);
      findSeqs.setLong(1, pluginSeq);
      findSeqs.setString(2, auKey);
      rs = executeQuery(findSeqs);

      while (rs.next()) {
        seqs.add(rs.getLong(1));
      }
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(findSeqs);
    }

    if (seqs.size() < 2) {
      return;
    }

    long survivor = seqs.get(0);
    List<Long> older = seqs.subList(1, seqs.size());

    for (String table : ARCHIVAL_UNIT_CHILD_TABLES) {
      if (countChildRows(conn, table, survivor) > 0) {
        continue;
      }

      for (Long seq : older) {
        if (countChildRows(conn, table, seq) > 0) {
          repointChildRows(conn, table, seq, survivor);
          break;
        }
      }
    }

    // The survivor takes the earliest creation_time and the latest
    // last_update_time of the group.
    PreparedStatement mergeTimes = null;

    try {
      mergeTimes = prepareStatement(conn, MERGE_ARCHIVAL_UNIT_TIMES_QUERY);
      mergeTimes.setLong(1, pluginSeq);
      mergeTimes.setString(2, auKey);
      mergeTimes.setLong(3, pluginSeq);
      mergeTimes.setString(4, auKey);
      mergeTimes.setLong(5, survivor);
      executeUpdate(mergeTimes);
    } finally {
      DbManager.safeCloseStatement(mergeTimes);
    }

    PreparedStatement delete = null;

    try {
      delete = prepareStatement(conn, DELETE_OLDER_ARCHIVAL_UNITS_QUERY);
      delete.setLong(1, pluginSeq);
      delete.setString(2, auKey);
      delete.setLong(3, survivor);
      executeUpdate(delete);
    } finally {
      DbManager.safeCloseStatement(delete);
    }
  }

  private long countChildRows(Connection conn, String table, long auSeq)
      throws SQLException {
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      ps = prepareStatement(conn, "select count(*) from " + table
                            + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?");
      ps.setLong(1, auSeq);
      rs = executeQuery(ps);
      rs.next();
      return rs.getLong(1);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(ps);
    }
  }

  private void repointChildRows(Connection conn, String table, long fromSeq,
      long toSeq) throws SQLException {
    PreparedStatement ps = null;

    try {
      ps = prepareStatement(conn, "update " + table
                            + " set " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?"
                            + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?");
      ps.setLong(1, toSeq);
      ps.setLong(2, fromSeq);
      executeUpdate(ps);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }
}
