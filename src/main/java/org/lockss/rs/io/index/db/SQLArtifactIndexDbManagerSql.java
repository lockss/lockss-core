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
package org.lockss.rs.io.index.db;

import org.lockss.db.*;
import org.lockss.log.L4JLogger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.lockss.config.db.SqlConstants.*;

public class SQLArtifactIndexDbManagerSql extends DbManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final String
      CREATE_NAMESPACE_TABLE_QUERY = "CREATE TABLE "
      + NAMESPACE_TABLE + " ("
      + NAMESPACE_SEQ_COLUMN + " --BigintSerialPk--,"
      + NAMESPACE_COLUMN + " VARCHAR(" + MAX_NAMESPACE_COLUMN + ") NOT NULL)";

  // Query to create the table for AUIDs and their internal sequence number.
  private static final String
      CREATE_AUID_TABLE_QUERY = "create table "
      + AUID_TABLE + " ("
      + AUID_SEQ_COLUMN + " --BigintSerialPk--,"
      + AUID_COLUMN + " varchar(" + MAX_AUID_COLUMN + ") not null)";

  private static final String CREATE_URL_TABLE_QUERY = "CREATE TABLE "
      + URL_TABLE + " ("
      + URL_SEQ_COLUMN + " --BigintSerialPk--,"
//      + URL_COLUMN + " VARCHAR NOT NULL)";
      + URL_COLUMN + " --PreferUnboundedTextType--)";

  private static final String CREATE_LONG_URL_TABLE_QUERY = "CREATE TABLE "
      + LONG_URL_TABLE + " ("
      + URL_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + URL_TABLE + " (" + URL_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + LONG_URL_COLUMN + " TEXT NOT NULL"
      + ")";

  private static final String
      CREATE_ARTIFACT_TABLE_QUERY = "CREATE TABLE "
      + ARTIFACT_TABLE + " ("
//      + ARTIFACT_UUID_COLUMN + " uuid DEFAULT gen_random_uuid(),"
      + ARTIFACT_UUID_COLUMN + " CHAR(36) NOT NULL,"
      + NAMESPACE_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + NAMESPACE_TABLE + " (" + NAMESPACE_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + AUID_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + AUID_TABLE + " (" + AUID_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + URL_SEQ_COLUMN + " BIGINT NOT NULL"
      + " REFERENCES " + URL_TABLE + " (" + URL_SEQ_COLUMN + ") ON DELETE CASCADE,"
      + ARTIFACT_VERSION_COLUMN + " INTEGER NOT NULL,"
      + ARTIFACT_COMMITTED_COLUMN + " BOOLEAN,"
      + ARTIFACT_STORAGE_URL_COLUMN + " VARCHAR(" + MAX_ARTIFACT_STORAGE_URL_COLUMN + ") NOT NULL,"
      + ARTIFACT_LENGTH_COLUMN + " BIGINT NOT NULL,"
      + ARTIFACT_DIGEST_COLUMN + " VARCHAR(" + MAX_ARTIFACT_DIGEST_COLUMN + ") NOT NULL,"
      + ARTIFACT_CRAWL_TIME_COLUMN + " BIGINT NOT NULL"
      + ")";

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

  // SQL statements that create the necessary version 1 indices.
  private static final String[] VERSION_1_INDEX_CREATE_QUERIES = new String[]{
      "create unique index idx1_" + AUID_TABLE
          + " on " + AUID_TABLE + "("
          + AUID_SEQ_COLUMN + ")"
  };

  // The SQL code used to create the necessary version 1 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_1_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(AUID_TABLE, CREATE_AUID_TABLE_QUERY);
        put(ARCHIVAL_UNIT_SIZE_TABLE, CREATE_ARCHIVAL_UNIT_SIZE_TABLE_QUERY);
      }};

  // The SQL code used to create the necessary version 2 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_2_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(NAMESPACE_TABLE, CREATE_NAMESPACE_TABLE_QUERY);
        put(URL_TABLE, CREATE_URL_TABLE_QUERY);
        put(ARTIFACT_TABLE, CREATE_ARTIFACT_TABLE_QUERY);
      }};

  private static final String NAMESPACE_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx1_" + NAMESPACE_TABLE + " ON " + NAMESPACE_TABLE + "(" + NAMESPACE_COLUMN + ")";

  private static final String NAMESPACE_SEQ_INDEX_NAMESPACE_TABLE_QUERY =
      "CREATE UNIQUE INDEX idx2_" + NAMESPACE_TABLE + " ON " + NAMESPACE_TABLE + "(" + NAMESPACE_SEQ_COLUMN + ")";

  private static final String NAMESPACE_SEQ_INDEX_ARTIFACT_TABLE_QUERY =
      "CREATE INDEX idx1_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + NAMESPACE_SEQ_COLUMN + ")";

  private static final String AUID_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx2_" + AUID_TABLE + " ON " + AUID_TABLE + "(" + AUID_COLUMN + ")";

  // Not used: Created as part of version 2
  private static final String AUID_SEQ_INDEX_AUID_TABLE_QUERY =
      "CREATE UNIQUE INDEX idx2_" + AUID_TABLE + " ON " + AUID_TABLE + "(" + AUID_SEQ_COLUMN + ")";

  private static final String AUID_SEQ_INDEX_ARTIFACT_TABLE_QUERY =
      "CREATE INDEX idx2_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + AUID_SEQ_COLUMN + ")";

  private static final String UNIQUE_URL_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_COLUMN + ")";

  private static final String URL_SEQ_INDEX_URL_TABLE_QUERY =
      "CREATE UNIQUE INDEX idx2_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_SEQ_COLUMN + ")";

  private static final String URL_SEQ_INDEX_ARTIFACT_TABLE_QUERY =
      "CREATE INDEX idx3_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + URL_SEQ_COLUMN + ")";

  private static final String ARTIFACT_UUID_INDEX_QUERY =
      "CREATE UNIQUE INDEX idx4_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + ARTIFACT_UUID_COLUMN + ")";

  private static final String ARTIFACT_VERSION_INDEX_QUERY =
      "CREATE INDEX idx5_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "(" + ARTIFACT_VERSION_COLUMN + ")";

  private static final String ARTIFACT_STEM_INDEX_QUERY =
      "CREATE INDEX idx6_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "("
          + NAMESPACE_SEQ_COLUMN + ", "
          + AUID_SEQ_COLUMN + ", "
          + URL_SEQ_COLUMN + ")";

  private static final String ARTIFACT_TUPLE_INDEX_QUERY =
      "CREATE INDEX idx7_" + ARTIFACT_TABLE + " ON " + ARTIFACT_TABLE + "("
          + NAMESPACE_SEQ_COLUMN + ", "
          + AUID_SEQ_COLUMN + ", "
          + URL_SEQ_COLUMN  + ", "
          + ARTIFACT_VERSION_COLUMN + ")";

  private static final String[] VERSION_3_INDEX_CREATE_QUERIES = new String[]{
      NAMESPACE_INDEX_QUERY,
      NAMESPACE_SEQ_INDEX_NAMESPACE_TABLE_QUERY,
      NAMESPACE_SEQ_INDEX_ARTIFACT_TABLE_QUERY,
      AUID_INDEX_QUERY,
      // AUID_SEQ_INDEX_AUID_TABLE_QUERY,
      AUID_SEQ_INDEX_ARTIFACT_TABLE_QUERY,
      UNIQUE_URL_INDEX_QUERY,
      URL_SEQ_INDEX_URL_TABLE_QUERY,
      URL_SEQ_INDEX_ARTIFACT_TABLE_QUERY,
      ARTIFACT_UUID_INDEX_QUERY,
      ARTIFACT_VERSION_INDEX_QUERY,
      ARTIFACT_STEM_INDEX_QUERY,
      ARTIFACT_TUPLE_INDEX_QUERY
  };

  private static final String DROP_UNIQUE_URL_INDEX_QUERY =
      "DROP INDEX idx1_" + URL_TABLE;

  private static final String URL_INDEX_QUERY =
      "CREATE INDEX idx1_" + URL_TABLE + " ON " + URL_TABLE + "(" + URL_COLUMN + ")";

  private static final String LONG_URL_HASH_INDEX_QUERY =
      "CREATE INDEX idx1_" + LONG_URL_TABLE + " ON " + LONG_URL_TABLE + " USING HASH (" + LONG_URL_COLUMN + ")";

  private static final Map<String, String> VERSION_4_TABLE_CREATE_QUERIES =
      new LinkedHashMap<String, String>() {{
        put(LONG_URL_TABLE, CREATE_LONG_URL_TABLE_QUERY);
      }};

  private static final String[] VERSION_4_ALTER_TABLE_QUERIES = new String[]{
      DROP_UNIQUE_URL_INDEX_QUERY,
      URL_INDEX_QUERY,
      LONG_URL_HASH_INDEX_QUERY,
  };

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
  protected SQLArtifactIndexDbManagerSql(DbManager dbMgr, DataSource dataSource, String dataSourceClassName,
                                         String dataSourceUser, int maxRetryCount, long retryDelay, int fetchSize) {
    super(dbMgr, dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
        retryDelay, fetchSize);
  }

  /**
   * Sets up the database to version 1.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
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
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
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
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void updateDatabaseFrom2To3(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary indices.
    executeDdlQueries(conn, VERSION_3_INDEX_CREATE_QUERIES);

    log.debug2("Done.");
  }

  /**
   * Updates the database from version 3 to version 4.
   *
   * @param conn A Connection with the database connection to be used.
   * @throws SQLException if any problem occurred updating the database.
   */
  void updateDatabaseFrom3To4(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_4_TABLE_CREATE_QUERIES);

    // Queries to alter tables and indicies
    executeDdlQueries(conn, VERSION_4_ALTER_TABLE_QUERIES);

    log.debug2("Done.");
  }
}