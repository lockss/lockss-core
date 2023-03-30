package org.lockss.repository;

import org.lockss.db.DbManagerSql;
import org.lockss.log.L4JLogger;

import javax.sql.DataSource;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.lockss.config.db.SqlConstants.*;

public class RepositoryDbManagerSql extends DbManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  // Query to create the table for AUIDs and their internal sequence number.
  private static final String
      CREATE_AUID_TABLE_QUERY = "create table "
      + AUID_TABLE + " ("
      + AUID_SEQ_COLUMN + " bigint not null references "
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
  protected RepositoryDbManagerSql(DataSource dataSource, String dataSourceClassName,
                                   String dataSourceUser, int maxRetryCount, long retryDelay, int fetchSize) {
    super(dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
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
}
