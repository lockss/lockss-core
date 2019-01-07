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

import org.lockss.config.db.*;
import org.lockss.db.*;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.AuUtil;
import org.lockss.util.SetUtil;
import org.lockss.util.time.TimeBase;

/**
 * The DbStateManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class DbStateManagerSql extends ConfigManagerSql {
  
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

  // Query to add an AU state string
  private static final String ADD_AU_STATE_QUERY = "insert into "
      + ARCHIVAL_UNIT_STATE_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + STATE_STRING_COLUMN
      + ") values (?,?)";

  // Query to update an AU state string
  private static final String UPDATE_AU_STATE_QUERY =
      "update " + ARCHIVAL_UNIT_STATE_TABLE
      + " set " + STATE_STRING_COLUMN + " = ?"
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";
  
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
   * Adds to the database the state of an Archival Unit.
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
  public Long addArchivalUnitState(String pluginId,
                                   String auKey,
                                   AuStateBean ausb)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("ausb = {}", ausb);

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the state of the Archival Unit, if it exists.
      String existingString =
	  findArchivalUnitState(conn, pluginId, auKey);
      log.trace("existingAuState = {}", existingString);

      if (existingString != null) {
	String message = "Attempt to replace existing AuState";
	log.error(message);
        log.error("pluginId = '{}, auKey = '{}'", pluginId, auKey);
	log.error("Existing state string = '{}'", existingString);
//	log.error("Replacement state string = '{}'", ausb.toJson()); FIXME
	throw new IllegalStateException(message);
      }

      return addArchivalUnitState(conn, pluginId, auKey, ausb, true);
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
  }

  /**
   * Adds to the database the state of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param ausb
   *          An {@link AuStateBean}.
   * @param commitAfterAdd
   *          A boolean with the indication of whether the addition should be
   *          committed in this method, or not.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitState(Connection conn,
                                   String pluginId,
                                   String auKey,
                                   AuStateBean ausb,
                                   boolean commitAfterAdd)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("ausb = {}", ausb);
    log.debug2("commitAfterAdd = {}", commitAfterAdd);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin, adding it if necessary.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      long auCreationTime = TimeBase.nowMs();
      log.trace("auCreationTime = {}", auCreationTime);

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, auCreationTime);

      // Add the new state of the Archival Unit.
      int count = addArchivalUnitState(conn, auSeq, ausb);
      log.trace("count = {}", count);
      
      if (commitAfterAdd) {
	// Commit the transaction.
	ConfigDbManager.commitOrRollback(conn, log);
      }
    } catch (DbException dbe) {
      String message = "Cannot add AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("ausb = {}", ausb);
      log.error("commitAfterAdd = {}", commitAfterAdd);
      throw dbe;
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
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

    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = getConnection();

      // Get the state of the Archival Unit, if it exists.
      String existingString =
          findArchivalUnitState(conn, pluginId, auKey);
      log.trace("existingAuState = {}", existingString);

      if (existingString == null) {
        String message = "Attempt to update a non-existent AuState";
        log.error(message);
        log.error("pluginId = '{}, auKey = '{}'", pluginId, auKey);
        log.error("ausb = '{}'", ausb);
        throw new IllegalStateException(message);
      }

      return updateArchivalUnitState(conn, pluginId, auKey, ausb);
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
      Long pluginSeq = findPlugin(conn, pluginId);

      // Find the Archival Unit.
      auSeq = findArchivalUnit(conn, pluginSeq, auKey);

      // Add the new state of the Archival Unit.
      int count = updateArchivalUnitState(conn, auSeq, ausb);
      log.trace("count = {}", count);
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
  private int updateArchivalUnitState(Connection conn,
                                      Long auSeq,
                                      AuStateBean ausb)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("ausb = {}", ausb);

    PreparedStatement addState = null;
    String errorMessage = "Cannot update Archival Unit state";

    try {
      // Convert to JSON
      String json = ausb.toJsonExcept(auId_auCreationTime);
      
      // Prepare the query.
      addState = configDbManager.prepareStatement(conn, UPDATE_AU_STATE_QUERY);

      // Populate the query.
      addState.setString(1, json);
      addState.setLong(2, auSeq);

      // Execute the query
      int count = configDbManager.executeUpdate(addState);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (IOException ioe) {
      log.error(errorMessage, ioe);
      log.error("SQL = '{}'.", UPDATE_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausb = {}", ausb);
      throw new DbException(errorMessage, ioe);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPDATE_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausb = {}", ausb);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", UPDATE_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("ausb = {}", ausb);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addState);
    }
  }
  
  protected static final Set<String> auId_auCreationTime =
      Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("auId", "auCreationTime")));
  
}
