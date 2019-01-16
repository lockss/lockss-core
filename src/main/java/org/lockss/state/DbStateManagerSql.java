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
import org.lockss.app.*;
import org.lockss.config.db.*;
import org.lockss.db.*;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.AuUtil;
import org.lockss.plugin.PluginManager;
import org.lockss.protocol.*;
import org.lockss.util.time.TimeBase;

/**
 * The DbStateManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class DbStateManagerSql extends ConfigManagerSql implements StateStore {
  
  private static L4JLogger log = L4JLogger.getLogger();

  // Query to retrieve an AU state string
  private static final String GET_AU_STATE_QUERY = "select "
      + "a." + ARCHIVAL_UNIT_SEQ_COLUMN
      + ", a." + CREATION_TIME_COLUMN
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
  public String findArchivalUnitState(String pluginId,
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
  public String findArchivalUnitState(Connection conn,
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

      // Get the configuration of the Archival Unit.
      resultSet = configDbManager.executeQuery(getAuState);

      // Get the single result, if any.
      if (resultSet.next()) {
        String dbJson = resultSet.getString(STATE_STRING_COLUMN);
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
      
      // Prepare the query.
      addState = configDbManager.prepareStatement(conn, ADD_AU_STATE_QUERY);

      // Populate the query.
      addState.setLong(1, auSeq);
      addState.setString(2, json);

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
  public Long updateArchivalUnitState(String pluginId,
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
  public Long updateArchivalUnitState(Connection conn,
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
  public AuAgreements findAuAgreements(String key) {
    return null;
  }

  @Override
  public Long updateAuAgreements(String key,
				 AuAgreements aua,
				 Set<PeerIdentity> peers) {
    return 0L;
  }


  protected static final Set<String> auId_auCreationTime =
      Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("auId", "auCreationTime")));
  
}
