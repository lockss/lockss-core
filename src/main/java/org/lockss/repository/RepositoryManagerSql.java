/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

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

import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.time.TimeBase;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.lockss.config.db.SqlConstants.*;

/**
 * SQL queries and operations in support of {@link RepositoryManager}.
 */
public class RepositoryManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  protected final RepositoryDbManager repoDbManager;

  private static final String EMPTY_STRING = "";

  private static final String FIND_URL_SEQ_QUERY = "SELECT "
      + URL_SEQ_COLUMN
      + " FROM " + URL_TABLE
      + " WHERE " + URL_COLUMN + " = ?";

  private static final String INSERT_URL_QUERY = "INSERT INTO "
      + URL_TABLE
      + "(" + URL_SEQ_COLUMN
      + "," + URL_COLUMN
      + ") VALUES (default,?)";

  private static final String GET_URLS_QUERY = "SELECT "
      + URL_COLUMN + " FROM " + URL_TABLE;

  private static final String FIND_NAMESPACE_SEQ_QUERY = "SELECT "
      + NAMESPACE_SEQ_COLUMN
      + " FROM " + NAMESPACE_TABLE
      + " WHERE " + NAMESPACE_COLUMN + " = ?";

  private static final String INSERT_NAMESPACE_QUERY = "INSERT INTO "
      + NAMESPACE_TABLE
      + "(" + NAMESPACE_SEQ_COLUMN
      + "," + NAMESPACE_COLUMN
      + ") VALUES (default,?)";

  private static final String GET_NAMESPACES_QUERY = "SELECT "
      + NAMESPACE_COLUMN + " FROM " + NAMESPACE_TABLE;


  // Query to find an AUID's internal AUID sequence number
  private static final String FIND_AUID_SEQ_QUERY = "select "
      + AUID_SEQ_COLUMN
      + " from " + AUID_TABLE
      + " where " + AUID_COLUMN + " = ?";

  // Query to add an entry to the AUID table
  private static final String INSERT_AUID_QUERY = "insert into "
      + AUID_TABLE
      + "(" + AUID_SEQ_COLUMN
      + "," + AUID_COLUMN
      + ") values (default,?)";

  // Query for the AU sizes of an AU associated with an AUID
  private static final String GET_AU_SIZE_QUERY = "select "
      + "s." + AU_LATEST_VERSIONS_SIZE_COLUMN
      + ", s." + AU_ALL_VERSIONS_SIZE_COLUMN
      + ", s." + AU_DISK_SIZE_COLUMN
      + ", s." + LAST_UPDATE_TIME_COLUMN
      + " from " + AUID_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_SIZE_TABLE + " s"
      + " where a." + AUID_COLUMN + " = ?"
      + " and a." + AUID_SEQ_COLUMN + " = s."
      + AUID_SEQ_COLUMN;

  private static final String GET_ARTIFACT_BY_UUID_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", au." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " au"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = au." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND a." + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String GET_LATEST_ARTIFACT_VERSION_QUERY = "SELECT "
      + " MAX(a." + ARTIFACT_VERSION_COLUMN + ")"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?";

  private static final String GET_LATEST_ARTIFACT_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?";
//      + " AND a." + ARTIFACT_VERSION_COLUMN + " = (" + GET_LATEST_ARTIFACT_VERSION_QUERY + ")";

  private static final String ARTIFACT_COMMITTED_STATUS_CONDITION =
      " AND a." + ARTIFACT_COMMITTED_COLUMN + " = ?";

  private static final String GET_ARTIFACT_WITH_VERSION_QUERY = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " AND u." + URL_COLUMN + " = ?"
      + " AND a." + ARTIFACT_VERSION_COLUMN + " = ?";

  public static final String MAX_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY = "SELECT "
      + URL_SEQ_COLUMN + ","
      + "MAX(" + ARTIFACT_VERSION_COLUMN + ") latest_version"
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?"
      + " --CommittedStatusCondition-- "
      + " GROUP BY "
      + " a." + NAMESPACE_SEQ_COLUMN + ","
      + " a." + AUID_SEQ_COLUMN + ","
      + " a." + URL_SEQ_COLUMN;

  private static final String GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_AUID = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + "," + ARTIFACT_TABLE + " a"
      + " INNER JOIN ( --MaxVersionAllUrlsWithNamespaceAndAuid-- ) m ON"
      + " m." + URL_SEQ_COLUMN + " = a." + URL_SEQ_COLUMN
      + " AND m.latest_version = a." + ARTIFACT_VERSION_COLUMN
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?";

  private static final String GET_ARTIFACTS_WITH_NAMESPACE_AND_AUID = "SELECT "
      + "a." + ARTIFACT_UUID_COLUMN
      + ", ns." + NAMESPACE_COLUMN
      + ", auid." + AUID_COLUMN
      + ", u." + URL_COLUMN
      + ", a." + ARTIFACT_VERSION_COLUMN
      + ", a." + ARTIFACT_COMMITTED_COLUMN
      + ", a." + ARTIFACT_STORAGE_URL_COLUMN
      + ", a." + ARTIFACT_LENGTH_COLUMN
      + ", a." + ARTIFACT_DIGEST_COLUMN
      + ", a." + ARTIFACT_CRAWL_TIME_COLUMN
      + " FROM " + ARTIFACT_TABLE + " a"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + AUID_TABLE + " auid"
      + "," + URL_TABLE + " u"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND a." + URL_SEQ_COLUMN + " = u." + URL_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?"
      + " AND auid." + AUID_COLUMN + " = ?";

  private static final String GET_AUIDS_BY_NAMESPACE = "SELECT DISTINCT "
      + "auid." + AUID_COLUMN
      + " FROM " + AUID_TABLE + " auid"
      + "," + NAMESPACE_TABLE + " ns"
      + "," + ARTIFACT_TABLE + " a"
      + " WHERE  a." + NAMESPACE_SEQ_COLUMN + " = ns." + NAMESPACE_SEQ_COLUMN
      + " AND a." + AUID_SEQ_COLUMN + " = auid." + AUID_SEQ_COLUMN
      + " AND ns." + NAMESPACE_COLUMN + " = ?";

  private static final String UPDATE_ARTIFACT_COMMITTED_QUERY = "UPDATE " + ARTIFACT_TABLE
      + " SET " + ARTIFACT_COMMITTED_COLUMN + " = ?"
      + " WHERE " + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String UPDATE_ARTIFACT_STORAGE_URL_QUERY = "UPDATE " + ARTIFACT_TABLE
      + " SET " + ARTIFACT_STORAGE_URL_COLUMN + " = ?"
      + " WHERE " + ARTIFACT_UUID_COLUMN + " = ?";

  private static final String DELETE_ARTIFACT_QUERY = "DELETE FROM " + ARTIFACT_TABLE
      + " WHERE " + ARTIFACT_UUID_COLUMN + " = ?";

  // Query to delete AU sizes of an AU associated with an AUID
  private static final String DELETE_AU_SIZE_QUERY =
      "delete from " + ARCHIVAL_UNIT_SIZE_TABLE
          + " where " + AUID_SEQ_COLUMN + " = ?";

  // Query to add AU sizes of an AU
  private static final String ADD_AU_SIZE_QUERY = "insert into "
      + ARCHIVAL_UNIT_SIZE_TABLE
      + "(" + AUID_SEQ_COLUMN
      + "," + AU_LATEST_VERSIONS_SIZE_COLUMN
      + "," + AU_ALL_VERSIONS_SIZE_COLUMN
      + "," + AU_DISK_SIZE_COLUMN
      + "," + LAST_UPDATE_TIME_COLUMN
      + ") values (?,?,?,?,?)";

  private static final String INSERT_ARTIFACT_QUERY = "INSERT INTO "
      + ARTIFACT_TABLE
      + "(" + ARTIFACT_UUID_COLUMN
      + "," + NAMESPACE_SEQ_COLUMN
      + "," + AUID_SEQ_COLUMN
      + "," + URL_SEQ_COLUMN
      + "," + ARTIFACT_VERSION_COLUMN
      + "," + ARTIFACT_COMMITTED_COLUMN
      + "," + ARTIFACT_STORAGE_URL_COLUMN
      + "," + ARTIFACT_LENGTH_COLUMN
      + "," + ARTIFACT_DIGEST_COLUMN
      + "," + ARTIFACT_CRAWL_TIME_COLUMN
      + " ) VALUES (?,?,?,?,?,?,?,?,?,?)";

  /**
   * Constructor.
   *
   * @param repoDbManager A RepositoryDbManager with the database manager.
   */
  public RepositoryManagerSql(RepositoryDbManager repoDbManager) throws DbException {
    this.repoDbManager = repoDbManager;
  }

  /**
   * Provides a connection to the database.
   *
   * @return a Connection with the connection to the database.
   * @throws DbException if any problem occurred accessing the database.
   */
  public Connection getConnection() throws DbException {
    return repoDbManager.getConnection();
  }

  protected Long findOrCreateUrlSeq(Connection conn, String url)
      throws DbException {

    log.debug2("url = {}", url);

    // Find the URL in the database
    Long urlSeq = findUrlSeq(conn, url);
    log.trace("urlSeq = {}", urlSeq);

    if (urlSeq == null) {
      // Add the URL to the database
      urlSeq = addUrl(conn, url);
      log.trace("new urlSeq = {}", urlSeq);
    }

    log.debug2("urlSeq = {}", urlSeq);
    return urlSeq;
  }

  protected Long findUrlSeq(Connection conn, String url)
      throws DbException {

    log.debug2("url = {}", url);

    Long urlSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find url";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, FIND_URL_SEQ_QUERY);

      // Populate the query
      ps.setString(1, url);

      // Get the URL row
      resultSet = repoDbManager.executeQuery(ps);

      // Check whether a result was obtained.
      if (resultSet.next()) {
        // Yes: Get the URL sequence
        urlSeq = resultSet.getLong(URL_SEQ_COLUMN);
        log.trace("Found urlSeq = {}", urlSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_URL_SEQ_QUERY);
      log.error("url = {}", url);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("urlSeq = {}", urlSeq);
    return urlSeq;
  }

  private Long addUrl(Connection conn, String url) throws DbException {
    log.debug2("url = {}", url);

    Long urlSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add url";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn,
          INSERT_URL_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query
      ps.setString(1, url);

      // Add the URL
      repoDbManager.executeUpdate(ps);
      resultSet = ps.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
        // Yes: Report the problem.
        String message =
            "Unable to create row in url table for url = " + url;
        log.error(message);
        throw new DbException(message);
      }

      // No: Get the url database identifier.
      urlSeq = resultSet.getLong(1);
      log.trace("Added urlSeq = {}", urlSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_URL_QUERY);
      log.error("url = {}", url);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_URL_QUERY);
      log.error("url = {}", url);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("urlSeq = {}", urlSeq);
    return urlSeq;
  }

  protected Long findOrCreateNamespaceSeq(Connection conn, String namespace)
      throws DbException {

    log.debug2("namespace = {}", namespace);

    // Find the namespace in the database
    Long namespaceSeq = findNamespaceSeq(conn, namespace);
    log.trace("namespaceSeq = {}", namespaceSeq);

    if (namespaceSeq == null) {
      // Add the namespace to the database
      namespaceSeq = addNamespace(conn, namespace);
      log.trace("new namespaceSeq = {}", namespaceSeq);
    }

    log.debug2("namespaceSeq = {}", namespaceSeq);
    return namespaceSeq;
  }

  protected Long findNamespaceSeq(Connection conn, String namespace)
      throws DbException {

    log.debug2("namespace = {}", namespace);

    Long namespaceSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find namespace";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, FIND_NAMESPACE_SEQ_QUERY);

      // Populate the query
      ps.setString(1, namespace);

      // Get the namespace row
      resultSet = repoDbManager.executeQuery(ps);

      // Check whether a result was obtained.
      if (resultSet.next()) {
        // Yes: Get the namespace sequence
        namespaceSeq = resultSet.getLong(NAMESPACE_SEQ_COLUMN);
        log.trace("Found namespaceSeq = {}", namespaceSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_NAMESPACE_SEQ_QUERY);
      log.error("namespace = {}", namespace);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("namespaceSeq = {}", namespaceSeq);
    return namespaceSeq;
  }

  private Long addNamespace(Connection conn, String namespace) throws DbException {
    log.debug2("namespace = {}", namespace);

    Long namespaceSeq = null;
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add namespace";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn,
          INSERT_NAMESPACE_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query
      ps.setString(1, namespace);

      // Add the namespace
      repoDbManager.executeUpdate(ps);
      resultSet = ps.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
        // Yes: Report the problem.
        String message =
            "Unable to create row in namespace table for namespace = " + namespace;
        log.error(message);
        throw new DbException(message);
      }

      // No: Get the namespace database identifier.
      namespaceSeq = resultSet.getLong(1);
      log.trace("Added namespaceSeq = {}", namespaceSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_NAMESPACE_QUERY);
      log.error("namespace = {}", namespace);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_NAMESPACE_QUERY);
      log.error("namespace = {}", namespace);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("namespaceSeq = {}", namespaceSeq);
    return namespaceSeq;
  }

  public List<String> getNamespaces() throws DbException {
    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return getNamespaces(conn);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private List<String> getNamespaces(Connection conn) throws DbException {
    List<String> result = new ArrayList<>();
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get namespaces";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, GET_NAMESPACES_QUERY);

      resultSet = repoDbManager.executeQuery(ps);

      while (resultSet.next()) {
        result.add(resultSet.getString(NAMESPACE_COLUMN));
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_NAMESPACES_QUERY);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("result = {}", result);
    return result;
  }

  public Iterable<String> findAuids(String namespace) throws DbException {
    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return findAuids(conn, namespace);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Iterable<String> findAuids(Connection conn, String namespace) throws DbException {
    List<String> result = new ArrayList<>();
    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AUIDs in namespace";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, GET_AUIDS_BY_NAMESPACE);
      ps.setString(1, namespace);

      resultSet = repoDbManager.executeQuery(ps);

      while (resultSet.next()) {
        result.add(resultSet.getString(AUID_COLUMN));
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AUIDS_BY_NAMESPACE);
      log.error("namespace = {}", namespace);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }

    log.debug2("result = {}", result);
    return result;
  }

  protected Long findOrCreateAuidSeq(Connection conn, String auid)
      throws DbException {

    log.debug2("auid = {}", auid);

    // Find the AUID in the database
    Long auidSeq = findAuidSeq(conn, auid);
    log.trace("auidSeq = {}", auidSeq);

    if (auidSeq == null) {
      // Add the AUID to the database
      auidSeq = addAuid(conn, auid);
      log.trace("new auidSeq = {}", auidSeq);
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  protected Long findAuidSeq(Connection conn, String auid)
      throws DbException {

    log.debug2("auid = {}", auid);

    Long auidSeq = null;
    PreparedStatement findAuid = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find AUID";

    try {
      // Prepare the query
      findAuid = repoDbManager.prepareStatement(conn, FIND_AUID_SEQ_QUERY);

      // Populate the query
      findAuid.setString(1, auid);

      // Get the AUID row
      resultSet = repoDbManager.executeQuery(findAuid);

      // Check whether a result was obtained.
      if (resultSet.next()) {
        // Yes: Get the AUID sequence
        auidSeq = resultSet.getLong(AUID_SEQ_COLUMN);
        log.trace("Found auidSeq = {}", auidSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_AUID_SEQ_QUERY);
      log.error("auid = {}", auid);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAuid);
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  private Long addAuid(Connection conn, String auid) throws DbException {
    log.debug2("auid = {}", auid);

    Long auidSeq = null;
    PreparedStatement insertAuid = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add AUID";

    try {
      // Prepare the query
      insertAuid = repoDbManager.prepareStatement(conn,
          INSERT_AUID_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query
      insertAuid.setString(1, auid);

      // Add the AUID
      repoDbManager.executeUpdate(insertAuid);
      resultSet = insertAuid.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
        // Yes: Report the problem.
        String message =
            "Unable to create row in AUID table for auid = " + auid;
        log.error(message);
        throw new DbException(message);
      }

      // No: Get the AUID database identifier.
      auidSeq = resultSet.getLong(1);
      log.trace("Added auidSeq = {}", auidSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_AUID_QUERY);
      log.error("auid = {}", auid);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_AUID_QUERY);
      log.error("auid = {}", auid);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertAuid);
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  public Artifact getArtifact(String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return getArtifact(conn, uuid);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Artifact getArtifact(Connection conn, String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifact";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, GET_ARTIFACT_BY_UUID_QUERY);

      // Populate the query
      ps.setString(1, uuid);

      resultSet = repoDbManager.executeQuery(ps);

      Artifact result = getArtifactFromResultSet(resultSet);
      log.debug2("result = {}", result);
      return result;
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_ARTIFACT_BY_UUID_QUERY);
      log.error("artifactId = {}", uuid);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  public Artifact getArtifact(String namespace, String auid, String url, int version) throws DbException {
    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("url = {}", url);
    log.debug2("version = {}", version);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return getArtifact(conn, namespace, auid, url, version);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Artifact getArtifact(Connection conn, String namespace, String auid, String url, int version)
      throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifact";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, GET_ARTIFACT_WITH_VERSION_QUERY);

      // Populate the query
      ps.setString(1, namespace);
      ps.setString(2, auid);
      ps.setString(3, url);
      ps.setInt(4, version);

      resultSet = repoDbManager.executeQuery(ps);

      Artifact result = getArtifactFromResultSet(resultSet);
      log.debug2("result = {}", result);
      return result;
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_ARTIFACT_WITH_VERSION_QUERY);
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("url = {}", url);
      log.error("version = {}", version);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  public List<Artifact> findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(
      String namespace, String auid, boolean includeUncommitted) throws DbException {
    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(conn, namespace, auid, includeUncommitted);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private List<Artifact> findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(
      Connection conn, String namespace, String auid, boolean includeUncommitted) throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifacts";

    try {
      String sqlQuery = GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_AUID;
      String latestVersionsQuery = MAX_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY;

      // FIXME: These string replacements are pretty damn ugly and fragile
      latestVersionsQuery = latestVersionsQuery.replace("--CommittedStatusCondition--",
          !includeUncommitted ? ARTIFACT_COMMITTED_STATUS_CONDITION : EMPTY_STRING);

      sqlQuery = sqlQuery.replace("--MaxVersionAllUrlsWithNamespaceAndAuid--", latestVersionsQuery);

      log.info("sqlQuery = " + sqlQuery);

      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, sqlQuery);

      // Populate the query
      ps.setString(1, namespace);
      ps.setString(2, auid);
      if (!includeUncommitted) {
        ps.setBoolean(3, true);
        ps.setString(4, namespace);
        ps.setString(5, auid);
      } else {
        ps.setString(3, namespace);
        ps.setString(4, auid);
      }

      resultSet = repoDbManager.executeQuery(ps);

      return getArtifactsFromResultSet(resultSet);
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_LATEST_ARTIFACTS_WITH_NAMESPACE_AND_AUID);
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("includeUncommitted = {}", includeUncommitted);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  public List<Artifact> findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(String namespace, String auid, boolean includeUncommitted) throws DbException {
    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(conn, namespace, auid, includeUncommitted);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private List<Artifact> findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(Connection conn, String namespace, String auid, boolean includeUncommitted)
      throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifacts";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, GET_ARTIFACTS_WITH_NAMESPACE_AND_AUID);

      // Populate the query
      ps.setString(1, namespace);
      ps.setString(2, auid);

      resultSet = repoDbManager.executeQuery(ps);

      return getArtifactsFromResultSet(resultSet);
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_ARTIFACTS_WITH_NAMESPACE_AND_AUID);
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("includeUncommitted = {}", includeUncommitted);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  private List<Artifact> getArtifactsFromResultSet(ResultSet resultSet) throws SQLException {
    List<Artifact> result = new ArrayList<>();

    // FIXME: Is there a better way to do this?
    while (resultSet.next()) {
      result.add(getArtifactFromCurrentRow(resultSet));
    }

    return result;
  }

  public static Artifact getArtifactFromResultSet(ResultSet resultSet) throws SQLException {
    // Get the single result, if any
    if (resultSet.next()) {
      return getArtifactFromCurrentRow(resultSet);
    }

    return null;
  }

  private static Artifact getArtifactFromCurrentRow(ResultSet resultSet) throws SQLException {
    Artifact result = new Artifact();

    result.setUuid(resultSet.getString(ARTIFACT_UUID_COLUMN));
    result.setNamespace(resultSet.getString(NAMESPACE_COLUMN));
    result.setAuid(resultSet.getString(AUID_COLUMN));
    result.setUri(resultSet.getString(URL_COLUMN));
    result.setVersion(resultSet.getInt(ARTIFACT_VERSION_COLUMN));
    result.setCommitted(resultSet.getBoolean(ARTIFACT_COMMITTED_COLUMN));
    result.setStorageUrl(resultSet.getString(ARTIFACT_STORAGE_URL_COLUMN));
    result.setContentLength(resultSet.getLong(ARTIFACT_LENGTH_COLUMN));
    result.setContentDigest(resultSet.getString(ARTIFACT_DIGEST_COLUMN));
    result.setCollectionDate(resultSet.getLong(ARTIFACT_CRAWL_TIME_COLUMN));

    return result;
  }

  public Artifact getLatestArtifact(String namespace, String auid, String url, boolean includeUncommitted)
      throws DbException {

    log.debug2("namespace = {}", namespace);
    log.debug2("auid = {}", auid);
    log.debug2("url = {}", url);
    log.debug2("includeUncommitted = {}", includeUncommitted);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      return getLatestArtifact(conn, namespace, auid, url, includeUncommitted);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Artifact getLatestArtifact(Connection conn,
                                     String namespace, String auid, String url, boolean includeUncommitted)
      throws DbException {

    PreparedStatement ps = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get artifact";

    try {
      String sqlQuery = GET_LATEST_ARTIFACT_QUERY;
      String latestVersionQuery = GET_LATEST_ARTIFACT_VERSION_QUERY;

      if (!includeUncommitted) {
        latestVersionQuery += ARTIFACT_COMMITTED_STATUS_CONDITION;
      }

      sqlQuery += " AND a." + ARTIFACT_VERSION_COLUMN + " = (" + latestVersionQuery + ")";

      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, sqlQuery);

      // Populate the query
      ps.setString(1, namespace);
      ps.setString(2, auid);
      ps.setString(3, url);
      ps.setString(4, namespace);
      ps.setString(5, auid);
      ps.setString(6, url);

      if (!includeUncommitted) {
        ps.setBoolean(7, true);
      }

      resultSet = repoDbManager.executeQuery(ps);

      Artifact result = getArtifactFromResultSet(resultSet);
      log.debug2("result = {}", result);
      return result;
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_LATEST_ARTIFACT_QUERY);
      log.error("namespace = {}", namespace);
      log.error("auid = {}", auid);
      log.error("url = {}", url);
      log.error("includeUncommitted = {}", includeUncommitted);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(ps);
    }
  }

  public void commitArtifact(String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      commitArtifact(conn, uuid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private void commitArtifact(Connection conn, String uuid) throws DbException {
    PreparedStatement ps = null;
    String errorMessage = "Cannot update committed status for artifact";

    try {
      // Prepare the query
      ps = repoDbManager.prepareStatement(conn, UPDATE_ARTIFACT_COMMITTED_QUERY);

      // Populate the query
      ps.setBoolean(1, true);
      ps.setString(2, uuid);

      repoDbManager.executeUpdate(ps);
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", UPDATE_ARTIFACT_COMMITTED_QUERY);
      log.error("uuid = {}", uuid);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public AuSize findAuSize(String auid) throws DbException {
    log.debug2("auid = {}", auid);

    Connection conn = null;

    try {
      // Get a connection to the database
      conn = repoDbManager.getConnection();

      return findAuSize(conn, auid);
    } catch (DbException dbe) {
      String message = "Cannot find AU size";
      log.error(message, dbe);
      log.error("auid = {}", auid);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private AuSize findAuSize(Connection conn, String auid) throws DbException {
    log.debug2("auid = {}", auid);

    AuSize result = null;
    PreparedStatement getAuSize = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU size";

    try {
      // Prepare the query
      getAuSize = repoDbManager.prepareStatement(conn, GET_AU_SIZE_QUERY);

      // Populate the query
      getAuSize.setString(1, auid);

      // Get the AU size of the AU associated with this AUID
      resultSet = repoDbManager.executeQuery(getAuSize);

      // Get the single result, if any
      if (resultSet.next()) {
        result = new AuSize();

        // Populate the AuSize
        result.setTotalLatestVersions(
            resultSet.getLong(AU_LATEST_VERSIONS_SIZE_COLUMN));

        result.setTotalAllVersions(
            resultSet.getLong(AU_ALL_VERSIONS_SIZE_COLUMN));

        result.setTotalWarcSize(resultSet.getLong(AU_DISK_SIZE_COLUMN));
      }
    } catch (SQLException e) {
      log.error(errorMessage, e);
      log.error("SQL = '{}'.", GET_AU_SIZE_QUERY);
      log.error("auid = {}", auid);
      throw new DbException(errorMessage, e);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuSize);
    }

    log.debug2("result = {}", result);
    return result;
  }

  public Long updateAuSize(String auid, AuSize auSize)
      throws DbException {

    log.debug2("auid = {}", auid);
    log.debug2("auSize = {}", auSize);

    Long result = null;
    Connection conn = null;

    try {
      // Get a connection to the database
      conn = repoDbManager.getConnection();

      // Update the AU size
      result = updateAuSize(conn, auid, auSize);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  public void addArtifact(Artifact artifact) throws DbException {
    log.debug2("artifact = {}", artifact);

    Connection conn = null;

    try {
      conn = repoDbManager.getConnection();
      addArtifact(conn, artifact);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private void addArtifact(Connection conn, Artifact artifact) throws DbException {
    long namespaceSeq = findOrCreateNamespaceSeq(conn, artifact.getNamespace());
    long auidSeq = findOrCreateAuidSeq(conn, artifact.getAuid());
    long urlSeq = findOrCreateUrlSeq(conn, artifact.getUri());
    addArtifact(conn, auidSeq, namespaceSeq, urlSeq, artifact);
  }

  private void addArtifact(Connection conn, long auidSeq, long namespaceSeq, long urlSeq, Artifact artifact)
      throws DbException {

    PreparedStatement ps = repoDbManager.prepareStatement(conn, INSERT_ARTIFACT_QUERY);
    ArtifactIdentifier artifactId = artifact.getIdentifier();

    try {
      ps.setString(1, artifactId.getUuid());
      ps.setLong(2, namespaceSeq);
      ps.setLong(3, auidSeq);
      ps.setLong(4, urlSeq);
      ps.setInt(5, artifactId.getVersion());
      ps.setBoolean(6, artifact.isCommitted());
      ps.setString(7, artifact.getStorageUrl());
      ps.setLong(8, artifact.getContentLength());
      ps.setString(9, artifact.getContentDigest());
      ps.setLong(10, artifact.getCollectionDate());

      repoDbManager.executeUpdate(ps);
    } catch (SQLException e) {
      log.error("Error preparing SQL statement", e);
      throw new DbException("Error preparing SQL statement", e);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public void updateStorageUrl(String uuid, String storageUrl) throws DbException {
    log.debug2("uuid = {}", uuid);
    log.debug2("storageUrl = {}", storageUrl);

    Connection conn = null;

    try {
      // Get a connection to the database
      conn = repoDbManager.getConnection();

      // Update the storage URL
      updateStorageUrl(conn, uuid, storageUrl);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private int updateStorageUrl(Connection conn, String uuid, String storageUrl) throws DbException {
    log.debug2("artifactId = {}", uuid);
    log.debug2("storageUrl = {}", storageUrl);

    PreparedStatement ps = null;
    String errorMessage = "Cannot update storage URL";

    try {
      // Prepare the query.
      ps = repoDbManager.prepareStatement(conn, UPDATE_ARTIFACT_STORAGE_URL_QUERY);
      ps.setString(1, storageUrl);
      ps.setString(2, uuid);

      // Execute the query
      return repoDbManager.executeUpdate(ps);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPDATE_ARTIFACT_STORAGE_URL_QUERY);
      log.error("artifactId = {}", uuid);
      log.error("storageUrl = {}", storageUrl);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public void deleteArtifact(String uuid) throws DbException {
    log.debug2("uuid = {}", uuid);

    Connection conn = null;

    try {
      // Get a connection to the database
      conn = repoDbManager.getConnection();

      // Delete the artifact
      deleteArtifact(conn, uuid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private int deleteArtifact(Connection conn, String uuid) throws DbException {
    log.debug2("artifactId = {}", uuid);

    PreparedStatement ps = null;
    String errorMessage = "Cannot delete artifact";

    try {
      // Prepare the query.
      ps = repoDbManager.prepareStatement(conn, DELETE_ARTIFACT_QUERY);
      ps.setString(1, uuid);

      // Execute the query
      return repoDbManager.executeUpdate(ps);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_ARTIFACT_QUERY);
      log.error("artifactId = {}", uuid);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(ps);
    }
  }

  public void deleteAuSize(String auid) throws DbException {
    log.debug2("auid = {}", auid);

    Connection conn = null;

    try {
      // Get a connection to the database
      conn = repoDbManager.getConnection();

      // Update the AU size
      deleteAuSize(conn, auid);

      // Commit the transaction.
      DbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  private Long updateAuSize(Connection conn, String auid, AuSize auSize)
      throws DbException {

    log.debug2("auid = {}", auid);
    log.debug2("auSize = {}", auSize);

    Long auidSeq = null;

    try {
      // Find the AUID sequence number, or add an entry for it
      auidSeq = findOrCreateAuidSeq(conn, auid);

      // TODO: Replace with UPDATE operation

      // Delete any existing AuSize of the AU associated with this AUID
      int deletedCount = deleteAuSize(conn, auidSeq);
      log.trace("deletedCount = {}", deletedCount);

      // Add the new AuSize of the AU associated with this AUID
      int addedCount = addAuSize(conn, auidSeq, auSize);
      log.trace("addedCount = {}", addedCount);
    } catch (DbException e) {
      String message = "Cannot update AU size";
      log.error(message, e);
      log.error("auid = {}", auid);
      log.error("auSize = {}", auSize);
      throw e;
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  private long deleteAuSize(Connection conn, String auid)
      throws DbException {
    log.debug2("auid = {}", auid);

    Long auidSeq = null;

    try {
      // Find the AUID sequence number, or add an entry for it
      auidSeq = findOrCreateAuidSeq(conn, auid);

      // Delete any existing AuSize of the AU associated with this AUID
      int deletedCount = deleteAuSize(conn, auidSeq);
      log.trace("deletedCount = {}", deletedCount);
    } catch (DbException e) {
      String message = "Cannot update AU size";
      log.error(message, e);
      log.error("auid = {}", auid);
      throw e;
    }

    log.debug2("auidSeq = {}", auidSeq);
    return auidSeq;
  }

  /**
   * Deletes from the database the AU sizes of an AU.
   *
   * @param conn    A Connection with the database connection to be used.
   * @param auidSeq A Long with the database identifier of the AUID.
   * @return an int with the count of database rows deleted.
   * @throws DbException if any problem occurred accessing the database.
   */
  private int deleteAuSize(Connection conn, Long auidSeq) throws DbException {
    log.debug2("auidSeq = {}", auidSeq);

    int result = -1;
    PreparedStatement deleteAuSize = null;
    String errorMessage = "Cannot delete AU size";

    try {
      // Prepare the query.
      deleteAuSize = repoDbManager.prepareStatement(conn,
          DELETE_AU_SIZE_QUERY);

      // Populate the query.
      deleteAuSize.setLong(1, auidSeq);

      // Execute the query
      result = repoDbManager.executeUpdate(deleteAuSize);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_AU_SIZE_QUERY);
      log.error("auidSeq = {}", auidSeq);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(deleteAuSize);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the sizes of an AU.
   *
   * @param conn    A Connection with the database connection to be used.
   * @param auidSeq A Long with the database identifier of the AUID.
   * @param auSize  An {@link AuSize} with the AU's content sizes.
   * @return an int with the count of database rows added.
   * @throws DbException if any problem occurred accessing the database.
   */
  private int addAuSize(Connection conn, Long auidSeq, AuSize auSize)
      throws DbException {
    log.debug2("auidSeq = {}", auidSeq);
    log.debug2("auSize = {}", auSize);

    PreparedStatement addAuSize = null;
    String errorMessage = "Cannot add AU size";

    try {
      // Prepare the query.
      addAuSize = repoDbManager.prepareStatement(conn, ADD_AU_SIZE_QUERY);

      // Populate the query.
      addAuSize.setLong(1, auidSeq);
      addAuSize.setLong(2, auSize.getTotalLatestVersions());
      addAuSize.setLong(3, auSize.getTotalAllVersions());
      addAuSize.setLong(4, auSize.getTotalWarcSize());
      addAuSize.setLong(5, TimeBase.nowMs());

      // Execute the query
      int count = repoDbManager.executeUpdate(addAuSize);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_SIZE_QUERY);
      log.error("auidSeq = {}", auidSeq);
      log.error("auSize = {}", auSize);
      throw new DbException(errorMessage, sqle);
    } finally {
      DbManager.safeCloseStatement(addAuSize);
    }
  }
}
