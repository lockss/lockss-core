package org.lockss.repository;

import org.lockss.config.db.ConfigDbManager;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.time.TimeBase;

import java.sql.*;

import static org.lockss.config.db.SqlConstants.*;

public class RepositoryManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  protected final RepositoryDbManager repoDbManager;

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

  // Query to delete AU sizes of an AU associated with an AUID
  private static final String DELETE_AU_SIZE_QUERY =
      "delete from " + ARCHIVAL_UNIT_SIZE_TABLE
          + "where " + AUID_SEQ_COLUMN + " = ?";

  // Query to add AU sizes of an AU
  private static final String ADD_AU_SIZE_QUERY = "insert into"
      + ARCHIVAL_UNIT_SIZE_TABLE
      + "(" + AUID_SEQ_COLUMN
      + "," + AU_LATEST_VERSIONS_SIZE_COLUMN
      + "," + AU_ALL_VERSIONS_SIZE_COLUMN
      + "," + AU_DISK_SIZE_COLUMN
      + "," + LAST_UPDATE_TIME_COLUMN
      + ") values (?,?,?,?,?)";

  /**
   * Constructor.
   *
   * @param repoDbManager
   *          A RepositoryDbManager with the database manager.
   */
  public RepositoryManagerSql(RepositoryDbManager repoDbManager) throws DbException {
    this.repoDbManager = repoDbManager;
  }

  /**
   * Provides a connection to the database.
   *
   * @return a Connection with the connection to the database.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Connection getConnection() throws DbException {
    return repoDbManager.getConnection();
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

  protected AuSize findAuSize(String auid) throws DbException {
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

    AuSize result = new AuSize();
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

  protected Long updateAuSize(String auid, AuSize auSize)
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
      ConfigDbManager.commitOrRollback(conn, log);
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
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

  /**
   * Deletes from the database the AU sizes of an AU.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auidSeq
   *          A Long with the database identifier of the AUID.
   * @return an int with the count of database rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
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
      ConfigDbManager.safeCloseStatement(deleteAuSize);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the sizes of an AU.
   *
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auidSeq
   *          A Long with the database identifier of the AUID.
   * @param auSize
   *          An {@link AuSize} with the AU's content sizes.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
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
      ConfigDbManager.safeCloseStatement(addAuSize);
    }
  }
}
