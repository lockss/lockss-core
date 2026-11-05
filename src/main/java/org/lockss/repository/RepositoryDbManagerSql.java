/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.repository;

import org.lockss.db.*;
import org.lockss.log.L4JLogger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.lockss.config.db.SqlConstants.*;

public class RepositoryDbManagerSql extends DbManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  // Query to create the table for AUIDs and their internal sequence number.
  private static final String
      CREATE_AUID_TABLE_QUERY = "create table "
      + AUID_TABLE + " ("
      + AUID_SEQ_COLUMN + " --BigintSerialPk--,"
      + AUID_COLUMN + " varchar(" + MAX_AUID_COLUMN + ") not null)";

  // Query to create the table for tracking AU size statistics.
  private static final String
      CREATE_ARCHIVAL_UNIT_SIZE_TABLE_QUERY = "create table "
      + ARCHIVAL_UNIT_SIZE_TABLE + " ("
      + AUID_SEQ_COLUMN + " bigint not null references "
      + AUID_TABLE + " (" + AUID_SEQ_COLUMN + ") on delete cascade,"
      + AU_LATEST_VERSIONS_SIZE_COLUMN + " bigint not null,"
      + AU_ALL_VERSIONS_SIZE_COLUMN + " bigint not null,"
      // TODO: Set to -1 on any changes to the AU, and update only after du if -1
      + AU_DISK_SIZE_COLUMN + " bigint not null,"
      + LAST_UPDATE_TIME_COLUMN + " bigint not null"
      + ")";

  // SQL statements that create the necessary version 5 indices.
  private static final String[] VERSION_1_INDEX_CREATE_QUERIES = new String[]{
      "create unique index idx1_" + AUID_TABLE
          + " on " + AUID_TABLE + "("
          + AUID_SEQ_COLUMN + ")"
  };

  // The SQL code used to create the necessary version 5 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_1_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(AUID_TABLE, CREATE_AUID_TABLE_QUERY);
        put(ARCHIVAL_UNIT_SIZE_TABLE, CREATE_ARCHIVAL_UNIT_SIZE_TABLE_QUERY);
      }};

  // SQL statements that create the necessary version 2 indices.
  //
  // The unique index on auids(auid) is the conflict target of
  // RepositoryManagerSql's INSERT ... ON CONFLICT upsert, which is what makes
  // concurrent find-or-create of the same AUID race-free.
  private static final String[] VERSION_2_INDEX_CREATE_QUERIES = new String[]{
      "create unique index idx2_" + AUID_TABLE
          + " on " + AUID_TABLE + "("
          + AUID_COLUMN + ")"
  };

  // Query to find the AUIDs that have more than one auids row.
  private static final String FIND_DUPLICATE_AUIDS_QUERY = "select "
      + AUID_COLUMN
      + ", count(*)"
      + " from " + AUID_TABLE
      + " group by " + AUID_COLUMN
      + " having count(*) > 1";

  // Query to find the highest sequence number of the auids rows of one AUID.
  private static final String FIND_NEWEST_AUID_SEQ_QUERY = "select max("
      + AUID_SEQ_COLUMN + ")"
      + " from " + AUID_TABLE
      + " where " + AUID_COLUMN + " = ?";

  // Query to delete the size rows of every auids row of one AUID.
  private static final String DELETE_AU_SIZE_ROWS_QUERY = "delete from "
      + ARCHIVAL_UNIT_SIZE_TABLE
      + " where " + AUID_SEQ_COLUMN + " in (select " + AUID_SEQ_COLUMN
      + " from " + AUID_TABLE + " where " + AUID_COLUMN + " = ?)";

  // Query to delete the auids rows of an AUID other than the survivor.
  private static final String DELETE_OLDER_AUIDS_QUERY = "delete from "
      + AUID_TABLE
      + " where " + AUID_COLUMN + " = ?"
      + " and " + AUID_SEQ_COLUMN + " <> ?";

  /**
   * Constructor.
   *
   * @param dataSource          A DataSource with the datasource that provides the connection.
   * @param dataSourceClassName A String with the data source class name.
   * @param dataSourceUser      A String with the data source user name.
   * @param maxRetryCount       An int with the maximum number of retries to be attempted.
   * @param retryDelay          A long with the number of milliseconds to wait between consecutive
   *                            retries.
   * @param fetchSize           An int with the SQL statement fetch size.
   */
  protected RepositoryDbManagerSql(DbManager dbMgr, DataSource dataSource, String dataSourceClassName,
                                   String dataSourceUser, int maxRetryCount, long retryDelay, int fetchSize) {
    super(dbMgr, dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
        retryDelay, fetchSize);
  }

  /**
   * Sets up the database to version 1.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void setUpDatabaseVersion1(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_1_TABLE_CREATE_QUERIES);

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_1_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 1 to version 2.
   * <p>
   * Before version 2 nothing prevented two concurrent find-or-create calls
   * from each inserting an auids row for the same AUID, so any such
   * duplicates are merged first and then the unique index that makes the
   * upsert possible is created.
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

    mergeDuplicateAuids(conn);

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_2_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Merges auids rows that share the same AUID.
   * <p>
   * Policy: the row with the highest auid_seq survives. The
   * archival_unit_size rows of every duplicate, the survivor included, are
   * deleted, since there is no way to tell which is current. The older rows
   * are then deleted. One warning is logged per merged AUID.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  void mergeDuplicateAuids(Connection conn) throws SQLException {
    // Collect the duplicate groups first so no cursor is open while rows are
    // being moved around.
    List<Object[]> groups = new ArrayList<>();

    PreparedStatement findDups = null;
    ResultSet rs = null;

    try {
      findDups = prepareStatement(conn, FIND_DUPLICATE_AUIDS_QUERY);
      rs = executeQuery(findDups);

      while (rs.next()) {
        groups.add(new Object[] {rs.getString(1), rs.getLong(2)}); // auid, count_of_duplicates
      }
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(findDups);
    }

    for (Object[] group : groups) {
      String auid = (String)group[0];
      long count = (Long)group[1];

      mergeDuplicateAuid(conn, auid);

      log.warn("Merged {} duplicate auids rows for AUID {}", count, auid);
    }
  }

  /**
   * Merges the auids rows of one AUID into the one with the highest auid_seq,
   * dropping the size rows of all of them.
   */
  private void mergeDuplicateAuid(Connection conn, String auid)
      throws SQLException {
    long survivor;

    PreparedStatement findNewest = null;
    ResultSet rs = null;

    try {
      findNewest = prepareStatement(conn, FIND_NEWEST_AUID_SEQ_QUERY);
      findNewest.setString(1, auid);
      rs = executeQuery(findNewest);
      rs.next();
      survivor = rs.getLong(1);
    } finally {
      DbManager.safeCloseResultSet(rs);
      DbManager.safeCloseStatement(findNewest);
    }

    PreparedStatement deleteSizes = null;

    try {
      deleteSizes = prepareStatement(conn, DELETE_AU_SIZE_ROWS_QUERY);
      deleteSizes.setString(1, auid);
      executeUpdate(deleteSizes);
    } finally {
      DbManager.safeCloseStatement(deleteSizes);
    }

    PreparedStatement delete = null;

    try {
      delete = prepareStatement(conn, DELETE_OLDER_AUIDS_QUERY);
      delete.setString(1, auid);
      delete.setLong(2, survivor);
      executeUpdate(delete);
    } finally {
      DbManager.safeCloseStatement(delete);
    }
  }
}
