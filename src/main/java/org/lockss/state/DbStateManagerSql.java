/*

Copyright (c) 2000-2019, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

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

package org.lockss.state;

import static org.lockss.config.db.SqlConstants.*;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.zip.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.*;
import org.lockss.app.*;
import org.lockss.config.db.*;
import org.lockss.db.*;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.PluginManager;
import org.lockss.protocol.*;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;
import org.lockss.util.time.TimeBase;
import org.lockss.util.io.*;

/**
 * The DbStateManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class DbStateManagerSql extends ConfigManagerSql implements StateStore {
  
  private static L4JLogger log = L4JLogger.getLogger();

  // Query to retrieve an AU state string
  private static final String GET_AU_STATE_QUERY = "select "
      + "a." + CREATION_TIME_COLUMN
      + ", s." + STATE_STRING_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_STATE_TABLE + " s"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = s."
      + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to delete an AU state.
  private static final String DELETE_AU_STATE_QUERY = "delete from "
      + ARCHIVAL_UNIT_STATE_TABLE
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  // Query to add an AU state string
  private static final String ADD_AU_STATE_QUERY = "insert into "
      + ARCHIVAL_UNIT_STATE_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + STATE_STRING_COLUMN
      + ") values (?,?)";

  // Query to retrieve an AU poll agreements string.
  private static final String GET_AU_AGREEMENTS_QUERY = "select "
      + "s." + AGREEMENTS_STRING_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_AGREEMENTS_TABLE + " s"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = s."
      + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to delete the poll agreements of an AU.
  private static final String DELETE_AU_AGREEMENTS_QUERY = "delete from "
      + ARCHIVAL_UNIT_AGREEMENTS_TABLE
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  // Query to add an AU poll agreements string.
  private static final String ADD_AU_AGREEMENTS_QUERY = "insert into "
      + ARCHIVAL_UNIT_AGREEMENTS_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + AGREEMENTS_STRING_COLUMN
      + ") values (?,?)";

  // Query to retrieve an AU suspect URL versions string.
  private static final String GET_AU_SUSPECT_URL_VERSIONS_QUERY = "select "
      + "s." + SUSPECT_URL_VERSIONS_STRING_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE + " s"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = s."
      + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to delete the suspect URL versions of an AU.
  private static final String DELETE_AU_SUSPECT_URL_VERSIONS_QUERY =
      "delete from " + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  // Query to add an AU suspect URL versions string.
  private static final String ADD_AU_SUSPECT_URL_VERSIONS_QUERY = "insert into "
      + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + SUSPECT_URL_VERSIONS_STRING_COLUMN
      + ") values (?,?)";

  // Query to retrieve a no AU peer set string.
  private static final String GET_NO_AU_PEER_SET_QUERY = "select "
      + "s." + NO_AU_PEER_SET_STRING_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE + " s"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = s."
      + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to delete the no AU peer set of an AU.
  private static final String DELETE_NO_AU_PEER_SET_QUERY = "delete from "
      + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  // Query to add a no AU peer set string.
  private static final String ADD_NO_AU_PEER_SET_QUERY = "insert into "
      + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + NO_AU_PEER_SET_STRING_COLUMN
      + ") values (?,?)";

  public static final int JSON_COMPRESSION_THRESHOLD = 500;

  /**
   * Constructor.
   * 
   * @param configDbManager
   *          A ConfigDbManager with the database manager.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public DbStateManagerSql(ConfigDbManager configDbManager) throws DbException {
    super(configDbManager);
  }

  /**
   * Provides the state of an Archival Unit stored in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Map<String, Object> with the Archival Unit state properties.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitState(String pluginId,
                                       String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the state of the Archival Unit, if it exists.
      return findArchivalUnitState(conn, pluginId, auKey);
    } catch (DbException dbe) {
      String message = "Cannot find AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the state of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a string of Archival Unit state properties.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitState(Connection conn,
                                       String pluginId,
                                       String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    String result = null;
    PreparedStatement getAuState = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU state";

    try {
      // Prepare the query.
      getAuState =
	  configDbManager.prepareStatement(conn, GET_AU_STATE_QUERY);

      // Populate the query.
      getAuState.setString(1, pluginId);
      getAuState.setString(2, auKey);

      // Get the AuState json
      resultSet = configDbManager.executeQuery(getAuState);

      // Get the single result, if any.
      if (resultSet.next()) {
        String dbJson = resultSet.getString(STATE_STRING_COLUMN);
        dbJson = uncompressJson(dbJson, "AuState");

        Map<String, Object> dbMap = AuUtil.jsonToMap(dbJson);
        dbMap.put("auCreationTime", new Long(resultSet.getLong(CREATION_TIME_COLUMN)));
	result = AuUtil.mapToJson(dbMap);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_STATE_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_STATE_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } catch (IOException ioe) {
      log.error(errorMessage, ioe);
      log.error("SQL = '{}'.", GET_AU_STATE_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, ioe);
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuState);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Deletes from the database the state of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @return an int with the count of database rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int deleteArchivalUnitState(Connection conn,
                                      Long auSeq)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);

    int result = -1;
    PreparedStatement deleteState = null;
    String errorMessage = "Cannot delete Archival Unit state";

    try {
      // Prepare the query.
      deleteState =
	  configDbManager.prepareStatement(conn, DELETE_AU_STATE_QUERY);

      // Populate the query.
      deleteState.setLong(1, auSeq);

      // Execute the query
      result = configDbManager.executeUpdate(deleteState);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", DELETE_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(deleteState);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the state of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @param ausb
   *          An {@link AuStateBean}.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitState(Connection conn,
                                   Long auSeq,
                                   AuStateBean ausb)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("ausb = {}", ausb);

    PreparedStatement addState = null;
    String errorMessage = "Cannot add Archival Unit state";

    try {
      // Convert to JSON
      String json = ausb.toJsonExcept(auId_auCreationTime);
      
      String storedString = compressJson(json, "AuState");
      // Prepare the query.
      addState = configDbManager.prepareStatement(conn, ADD_AU_STATE_QUERY);

      // Populate the query.
      addState.setLong(1, auSeq);
      addState.setString(2, storedString);

      // Execute the query
      int count = configDbManager.executeUpdate(addState);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (IOException ioe) {
      log.error(errorMessage, ioe);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausb = {}", ausb);
      throw new DbException(errorMessage, ioe);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausb = {}", ausb);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausb = {}", ausb);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addState);
    }
  }

  /**
   * Updates the state of an Archival Unit in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param ausb
   *          An {@link AuStateBean}.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitState(String pluginId,
                                       String auKey,
                                       AuStateBean ausb)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("ausb = {}", ausb);

    Long result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Update the state.
      result = updateArchivalUnitState(conn, pluginId, auKey, ausb);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      String message = "Cannot add AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("ausb = {}", ausb);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Updates the state of an Archival Unit in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param ausb
   *          An {@link AuStateBean}.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitState(Connection conn,
                                       String pluginId,
                                       String auKey,
                                       AuStateBean ausb)
          throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("ausb = {}", ausb);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

      // Delete any existing state of the Archival Unit.
      int deletedCount = deleteArchivalUnitState(conn, auSeq);
      log.trace("deletedCount = {}", deletedCount);

      // Add the new state of the Archival Unit.
      int addedCount = addArchivalUnitState(conn, auSeq, ausb);
      log.trace("addedCount = {}", addedCount);
    } catch (DbException dbe) {
      String message = "Cannot update AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("ausb = {}", ausb);
      throw dbe;
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  // StateStore interface adapter

  @Override
  public AuStateBean findArchivalUnitState(String key)
      throws DbException, IOException {
    String json = findArchivalUnitState(PluginManager.pluginIdFromAuId(key),
					PluginManager.auKeyFromAuId(key));
    AuStateBean res = null;
    if (json != null) {
      res = AuStateBean.fromJson(key, json, LockssDaemon.getLockssDaemon());
    }
    return res;
  }

  @Override
  public Long updateArchivalUnitState(String key,
				      AuStateBean ausb,
				      Set<String> fields)
      throws DbException {
    return updateArchivalUnitState(PluginManager.pluginIdFromAuId(key),
				   PluginManager.auKeyFromAuId(key),
				   ausb);
  }

  @Override
  public AuAgreements findAuAgreements(String key)
      throws DbException, IOException {
    String json = findArchivalUnitAgreements(
	PluginManager.pluginIdFromAuId(key), PluginManager.auKeyFromAuId(key));

    AuAgreements res = null;

    if (json != null) {
      res = AuAgreements.fromJson(key, json, LockssDaemon.getLockssDaemon());
    }

    return res;
  }

  @Override
  public Long updateAuAgreements(String key,
				 AuAgreements aua,
				 Set<PeerIdentity> peers) throws DbException {
    return updateArchivalUnitAgreements(PluginManager.pluginIdFromAuId(key),
	PluginManager.auKeyFromAuId(key), aua);
  }

  protected static final Set<String> auId_auCreationTime =
      Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("auId", "auCreationTime")));

  /**
   * Provides the poll agreements of an Archival Unit stored in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a String with the Archival Unit poll agreements.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitAgreements(String pluginId, String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the poll agreements of the Archival Unit, if it exists.
      return findArchivalUnitAgreements(conn, pluginId, auKey);
    } catch (DbException dbe) {
      String message = "Cannot find AU poll agreements";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the poll agreements of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a String with the Archival Unit poll agreements.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitAgreements(Connection conn, String pluginId,
      String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    PreparedStatement getAuAgreements = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU poll agreements";

    try {
      // Prepare the query.
      getAuAgreements =
	  configDbManager.prepareStatement(conn, GET_AU_AGREEMENTS_QUERY);

      // Populate the query.
      getAuAgreements.setString(1, pluginId);
      getAuAgreements.setString(2, auKey);

      // Get the configuration of the Archival Unit.
      resultSet = configDbManager.executeQuery(getAuAgreements);

      // Get the single result, if any.
      if (resultSet.next()) {
	String result = resultSet.getString(AGREEMENTS_STRING_COLUMN);
        result = uncompressJson(result, "AuAgreements");
        return result;
      }
      return null;
    } catch (SQLException | IOException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_AGREEMENTS_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_AGREEMENTS_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuAgreements);
    }
  }

  /**
   * Updates the poll agreements of an Archival Unit in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param aua
   *          An {@link AuAgreements} with the poll agreements.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitAgreements(String pluginId, String auKey,
      AuAgreements aua) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("aua = {}", aua);

    Long result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Update the poll agreements.
      result = updateArchivalUnitAgreements(conn, pluginId, auKey, aua);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      String message = "Cannot update AU poll agreements";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("aua = {}", aua);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Updates the poll agreements of an Archival Unit in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param aua
   *          An {@link AuAgreements} with the poll agreements.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitAgreements(Connection conn, String pluginId,
      String auKey, AuAgreements aua) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("aua = {}", aua);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

      // Delete any existing poll agreements of the Archival Unit.
      int deletedCount = deleteArchivalUnitAgreements(conn, auSeq);
      log.trace("deletedCount = {}", deletedCount);

      // Add the new poll agreements of the Archival Unit.
      int addedCount = addArchivalUnitAgreements(conn, auSeq, aua);
      log.trace("addedCount = {}", addedCount);
    } catch (DbException dbe) {
      String message = "Cannot update AU poll agreements";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("aua = {}", aua);
      throw dbe;
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Deletes from the database the poll agreements of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @return an int with the count of database rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int deleteArchivalUnitAgreements(Connection conn, Long auSeq)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);

    int result = -1;
    PreparedStatement deleteAgreements = null;
    String errorMessage = "Cannot delete Archival Unit poll agreements";

    try {
      // Prepare the query.
      deleteAgreements =
	  configDbManager.prepareStatement(conn, DELETE_AU_AGREEMENTS_QUERY);

      // Populate the query.
      deleteAgreements.setLong(1, auSeq);

      // Execute the query
      result = configDbManager.executeUpdate(deleteAgreements);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_AU_AGREEMENTS_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", DELETE_AU_AGREEMENTS_QUERY);
      log.error("auSeq = {}", auSeq);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(deleteAgreements);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the poll agreements of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @param aua
   *          An {@link AuAgreements} with the poll agreements.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitAgreements(Connection conn, Long auSeq,
      AuAgreements aua) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("aua = {}", aua);

    PreparedStatement addAgreements = null;
    String errorMessage = "Cannot add Archival Unit poll agreements";

    try {
      // Convert to JSON
      String json = aua.toJson();
      
      String storedString = compressJson(aua.toJson(), "AuAgreements");
      // Prepare the query.
      addAgreements =
	  configDbManager.prepareStatement(conn, ADD_AU_AGREEMENTS_QUERY);

      // Populate the query.
      addAgreements.setLong(1, auSeq);
      addAgreements.setString(2, storedString);

      // Execute the query
      int count = configDbManager.executeUpdate(addAgreements);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (IOException ioe) {
      log.error(errorMessage, ioe);
      log.error("SQL = '{}'.", ADD_AU_AGREEMENTS_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("aua = {}", aua);
      throw new DbException(errorMessage, ioe);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_AGREEMENTS_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("aua = {}", aua);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_AU_AGREEMENTS_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("aua = {}", aua);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addAgreements);
    }
  }

  /**
   * Provides the AuSuspectUrlVersions associated with the key (an AUID).
   * 
   * @param key
   *          A String with the key under which the AuSuspectUrlVersions is
   *          stored.
   * @return an AuSuspectUrlVersions, or null if not present in the store.
   * @throws DbException
   *           if any problem occurred accessing the data.
   * @throws IOException
   *           if any problem occurred accessing the data.
   */
  @Override
  public AuSuspectUrlVersions findAuSuspectUrlVersions(String key)
      throws DbException, IOException {
    String json = findArchivalUnitSuspectUrlVersions(
	PluginManager.pluginIdFromAuId(key), PluginManager.auKeyFromAuId(key));

    AuSuspectUrlVersions res = null;

    if (json != null) {
      res = AuSuspectUrlVersions.fromJson(key, json,
	  LockssDaemon.getLockssDaemon());
    }

    return res;
  }

  /**
   * Updates an AuSuspectUrlVersions in the store, creating it if not already
   * present. If already present, only those fields listed in <code>ausuv</code>
   * must be stored, but it it permissible to ignore <code>ausuv</code> and
   * store the entire object.
   * 
   * @param key
   *          A String with the key under which the AuSuspectUrlVersions is
   *          stored.
   * @param ausuv
   *          An AuSuspectUrlVersions with the object to be updated.
   * @param versions
   *          A Set<SuspectUrlVersion> with the fields that must be written.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the data.
   */
  @Override
  public Long updateAuSuspectUrlVersions(String key,
					 AuSuspectUrlVersions ausuv,
					 Set<SuspectUrlVersion> versions)
					     throws DbException {
    return updateArchivalUnitSuspectUrlVersions(
	PluginManager.pluginIdFromAuId(key),
	PluginManager.auKeyFromAuId(key), ausuv);
  }

  /**
   * Provides the suspect URL versions of an Archival Unit stored in the
   * database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a String with the Archival Unit suspect URL versions.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitSuspectUrlVersions(String pluginId,
      String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the suspect URL versions of the Archival Unit, if it exists.
      return findArchivalUnitSuspectUrlVersions(conn, pluginId, auKey);
    } catch (DbException dbe) {
      String message = "Cannot find AU suspect URL versions";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the suspect URL versions of an Archival Unit stored in the
   * database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a String with the Archival Unit suspect URL versions.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitSuspectUrlVersions(Connection conn,
      String pluginId, String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    String result = null;
    PreparedStatement getAuSuspectUrlVersions = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU suspect URL versions";

    try {
      // Prepare the query.
      getAuSuspectUrlVersions = configDbManager.prepareStatement(conn,
	  GET_AU_SUSPECT_URL_VERSIONS_QUERY);

      // Populate the query.
      getAuSuspectUrlVersions.setString(1, pluginId);
      getAuSuspectUrlVersions.setString(2, auKey);

      // Get the suspect URL versions of the Archival Unit.
      resultSet = configDbManager.executeQuery(getAuSuspectUrlVersions);

      // Get the single result, if any.
      if (resultSet.next()) {
	result = resultSet.getString(SUSPECT_URL_VERSIONS_STRING_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getAuSuspectUrlVersions);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Updates the suspect URL versions of an Archival Unit in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param ausuv
   *          An {@link AuSuspectUrlVersions} with the suspect URL versions.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitSuspectUrlVersions(String pluginId,
      String auKey, AuSuspectUrlVersions ausuv) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("ausuv = {}", ausuv);

    Long result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Update the suspect URL versions.
      result =
	  updateArchivalUnitSuspectUrlVersions(conn, pluginId, auKey, ausuv);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      String message = "Cannot update AU suspect URL versions";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("ausuv = {}", ausuv);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Updates the suspect URL versions of an Archival Unit in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param ausuv
   *          An {@link AuSuspectUrlVersions} with the suspect URL versions.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitSuspectUrlVersions(Connection conn,
      String pluginId, String auKey, AuSuspectUrlVersions ausuv)
	  throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("ausuv = {}", ausuv);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

      // Delete any existing suspect URL versions of the Archival Unit.
      int deletedCount = deleteArchivalUnitSuspectUrlVersions(conn, auSeq);
      log.trace("deletedCount = {}", deletedCount);

      // Add the new suspect URL versions of the Archival Unit.
      int addedCount = addArchivalUnitSuspectUrlVersions(conn, auSeq, ausuv);
      log.trace("addedCount = {}", addedCount);
    } catch (DbException dbe) {
      String message = "Cannot update AU suspect URL versions";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("ausuv = {}", ausuv);
      throw dbe;
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Deletes from the database the suspect URL versions of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @return an int with the count of database rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int deleteArchivalUnitSuspectUrlVersions(Connection conn, Long auSeq)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);

    int result = -1;
    PreparedStatement deleteSuspectUrlVersions = null;
    String errorMessage = "Cannot delete Archival Unit suspect URL versions";

    try {
      // Prepare the query.
      deleteSuspectUrlVersions = configDbManager.prepareStatement(conn,
	  DELETE_AU_SUSPECT_URL_VERSIONS_QUERY);

      // Populate the query.
      deleteSuspectUrlVersions.setLong(1, auSeq);

      // Execute the query
      result = configDbManager.executeUpdate(deleteSuspectUrlVersions);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", DELETE_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("auSeq = {}", auSeq);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(deleteSuspectUrlVersions);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the suspect URL versions of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @param ausuv
   *          An {@link AuSuspectUrlVersions} with the suspect URL versions.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitSuspectUrlVersions(Connection conn, Long auSeq,
      AuSuspectUrlVersions ausuv) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("ausuv = {}", ausuv);

    PreparedStatement addSuspectUrlVersions = null;
    String errorMessage = "Cannot add Archival Unit suspect URL versions";

    try {
      // Convert to JSON
      String json = ausuv.toJson();
      
      // Prepare the query.
      addSuspectUrlVersions = configDbManager.prepareStatement(conn,
	  ADD_AU_SUSPECT_URL_VERSIONS_QUERY);

      // Populate the query.
      addSuspectUrlVersions.setLong(1, auSeq);
      addSuspectUrlVersions.setString(2, json);

      // Execute the query
      int count = configDbManager.executeUpdate(addSuspectUrlVersions);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (IOException ioe) {
      log.error(errorMessage, ioe);
      log.error("SQL = '{}'.", ADD_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausuv = {}", ausuv);
      throw new DbException(errorMessage, ioe);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausuv = {}", ausuv);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_AU_SUSPECT_URL_VERSIONS_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausuv = {}", ausuv);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addSuspectUrlVersions);
    }
  }

  /**
   * Provides the NoAuPeerSet associated with the key (an AUID).
   * 
   * @param key
   *          A String with the key under which the NoAuPeerSet is stored.
   * @return a DatedPeerIdSet, or null if not present in the store.
   * @throws DbException
   *           if any problem occurred accessing the data.
   * @throws IOException
   *           if any problem occurred accessing the data.
   */
  @Override
  public DatedPeerIdSet findNoAuPeerSet(String key)
      throws DbException, IOException {
    String json = findArchivalUnitNoAuPeerSet(
	PluginManager.pluginIdFromAuId(key), PluginManager.auKeyFromAuId(key));

    DatedPeerIdSet res = null;

    if (json != null) {
      res = DatedPeerIdSetImpl.fromJson(key, json, LockssDaemon.getLockssDaemon());
    }

    return res;
  }

  /**
   * Updates a NoAuPeerSet in the store, creating it if not already present.
   * If already present, only those peers listed in <code>peers</code> must be
   * stored, but it it permissible to ignore <code>peers</code> and store the
   * entire object.
   * 
   * @param key
   *          A String with the key under which the NoAuPeerSet is stored.
   * @param dpis
   *          A DatedPeerIdSet with the object to be updated.
   * @param peers
   *          A Set<PeerIdentity> with the peers that must be written.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the data.
   */
  @Override
  public Long updateNoAuPeerSet(String key,
				     DatedPeerIdSet dpis,
				     Set<PeerIdentity> peers)
					 throws DbException {
    return updateArchivalUnitNoAuPeerSet(PluginManager.pluginIdFromAuId(key),
	PluginManager.auKeyFromAuId(key), dpis);
  }

  /**
   * Provides the NoAuPeerSet of an Archival Unit stored in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a String with the Archival Unit NoAuPeerSet.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitNoAuPeerSet(String pluginId, String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the NoAuPeerSet of the Archival Unit, if it exists.
      return findArchivalUnitNoAuPeerSet(conn, pluginId, auKey);
    } catch (DbException dbe) {
      String message = "Cannot find AU NoAuPeerSet";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }
  }

  /**
   * Provides the NoAuPeerSet of an Archival Unit stored in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a String with the Archival Unit NoAuPeerSet.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private String findArchivalUnitNoAuPeerSet(Connection conn, String pluginId,
      String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    String result = null;
    PreparedStatement getNoAuPeerSet = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU NoAuPeerSet";

    try {
      // Prepare the query.
      getNoAuPeerSet =
	  configDbManager.prepareStatement(conn, GET_NO_AU_PEER_SET_QUERY);

      // Populate the query.
      getNoAuPeerSet.setString(1, pluginId);
      getNoAuPeerSet.setString(2, auKey);

      // Get the configuration of the Archival Unit.
      resultSet = configDbManager.executeQuery(getNoAuPeerSet);

      // Get the single result, if any.
      if (resultSet.next()) {
	result = resultSet.getString(NO_AU_PEER_SET_STRING_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_NO_AU_PEER_SET_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_NO_AU_PEER_SET_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getNoAuPeerSet);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Updates the NoAuPeerSet of an Archival Unit in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param dpis
   *          An {@link DatedPeerIdSet} with the dated peers.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitNoAuPeerSet(String pluginId, String auKey,
      DatedPeerIdSet dpis) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("dpis = {}", dpis);

    Long result = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Update the NoAuPeerSet.
      result = updateArchivalUnitNoAuPeerSet(conn, pluginId, auKey, dpis);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      String message = "Cannot update AU NoAuPeerSet";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("dpis = {}", dpis);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Updates the NoAuPeerSet of an Archival Unit in the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param dpis
   *          An {@link DatedPeerIdSet} with the dated peers.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long updateArchivalUnitNoAuPeerSet(Connection conn, String pluginId,
      String auKey, DatedPeerIdSet dpis) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("dpis = {}", dpis);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

      // Delete any existing NoAuPeerSet of the Archival Unit.
      int deletedCount = deleteArchivalUnitNoAuPeerSet(conn, auSeq);
      log.trace("deletedCount = {}", deletedCount);

      // Add the new NoAuPeerSet of the Archival Unit.
      int addedCount = addArchivalUnitNoAuPeerSet(conn, auSeq, dpis);
      log.trace("addedCount = {}", addedCount);
    } catch (DbException dbe) {
      String message = "Cannot update AU NoAuPeerSet";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("dpis = {}", dpis);
      throw dbe;
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Deletes from the database the NoAuPeerSet of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @return an int with the count of database rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int deleteArchivalUnitNoAuPeerSet(Connection conn, Long auSeq)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);

    int result = -1;
    PreparedStatement deleteNoAuPeerSet = null;
    String errorMessage = "Cannot delete Archival Unit NoAuPeerSet";

    try {
      // Prepare the query.
      deleteNoAuPeerSet =
	  configDbManager.prepareStatement(conn, DELETE_NO_AU_PEER_SET_QUERY);

      // Populate the query.
      deleteNoAuPeerSet.setLong(1, auSeq);

      // Execute the query
      result = configDbManager.executeUpdate(deleteNoAuPeerSet);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_NO_AU_PEER_SET_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", DELETE_NO_AU_PEER_SET_QUERY);
      log.error("auSeq = {}", auSeq);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(deleteNoAuPeerSet);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Adds to the database the NoAuPeerSet of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the database identifier of the Archival Unit.
   * @param dpis
   *          An {@link DatedPeerIdSet} with the dated peers.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitNoAuPeerSet(Connection conn, Long auSeq,
      DatedPeerIdSet dpis) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("dpis = {}", dpis);

    PreparedStatement addNoAuPeerSet = null;
    String errorMessage = "Cannot add Archival Unit NoAuPeerSet";

    try {
      // Convert to JSON
      String json = dpis.toJson();
      
      // Prepare the query.
      addNoAuPeerSet =
	  configDbManager.prepareStatement(conn, ADD_NO_AU_PEER_SET_QUERY);

      // Populate the query.
      addNoAuPeerSet.setLong(1, auSeq);
      addNoAuPeerSet.setString(2, json);

      // Execute the query
      int count = configDbManager.executeUpdate(addNoAuPeerSet);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (IOException ioe) {
      log.error(errorMessage, ioe);
      log.error("SQL = '{}'.", ADD_NO_AU_PEER_SET_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("dpis = {}", dpis);
      throw new DbException(errorMessage, ioe);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_NO_AU_PEER_SET_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("dpis = {}", dpis);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_NO_AU_PEER_SET_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("dpis = {}", dpis);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addNoAuPeerSet);
    }
  }

  private String compressJson(String json, String objname) throws IOException {
    String res = json;
    log.trace("storing {} json: {}", objname, json);
    if (json.length() >= JSON_COMPRESSION_THRESHOLD) {

      // compress and base91 encode the JSON
      UnsynchronizedByteArrayOutputStream baos =
        new UnsynchronizedByteArrayOutputStream();

      Base91.encode(new GZIPpedInputStream(json), baos);
      res = IOUtils.toString(baos.toInputStream());
      log.trace("storing () string: {}", objname, res);
    }
    log.debug2("{} json len: {}, compressed len: {}", objname,
               json.length(), res.length());
    return res;
  }

  private String uncompressJson(String storedString, String objname)
    throws IOException {
    String res = storedString;
    if (!storedString.startsWith("{\"")) {
      UnsynchronizedByteArrayOutputStream baos1 =
        new UnsynchronizedByteArrayOutputStream();
      Base91.decode(IOUtils.toInputStream(storedString, "UTF-8"), baos1);
      res = IOUtils.toString(new GZIPInputStream(baos1.toInputStream()));
    }
    log.debug2("{} result = {}", objname, res);
    return res;
  }
}
