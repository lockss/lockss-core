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
package org.lockss.state;

import static org.lockss.config.db.SqlConstants.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.config.db.ConfigManagerSql;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.AuUtil;
import org.lockss.state.AuState.AccessType;
import org.lockss.state.SubstanceChecker.State;
import org.lockss.util.time.TimeBase;

/**
 * The DbStateManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class DbStateManagerSql extends ConfigManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final String NULL_LAST_CRAWL_RESULT_MSG =
      "DbStateManagerSql-LastCrawlResult_Message.Is.Null";
  
  // Query to find the state of an Archival Unit.
  private static final String GET_AU_STATE_QUERY = "select "
      + "a." + ARCHIVAL_UNIT_SEQ_COLUMN
      + ", s." + STATE_STRING_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_STATE_TABLE + " s"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = s."
      + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to add a state of an Archival Unit.
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
      String auKey) throws DbException {
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
      String pluginId, String auKey) throws DbException {
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
	result = resultSet.getString(STATE_STRING_COLUMN);
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
   * @param stateString
   *          A string of Archival Unit state properties.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitState(String pluginId, String auKey,
      String stateString) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("stateString = {}", stateString);

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
	log.error("Existing AuState properties = '{}'", existingString);
	log.error("Replacement AuState properties = '{}'", stateString);
	log.error("pluginId = '{}, auKey = '{}'", pluginId, auKey);
	throw new IllegalStateException(message);
      }

      return addArchivalUnitState(conn, pluginId, auKey, stateString, true);
    } catch (DbException dbe) {
      String message = "Cannot add AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("stateString = {}", stateString);
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
   * @param stateString
   *          A string of Archival Unit state properties.
   * @param commitAfterAdd
   *          A boolean with the indication of whether the addition should be
   *          committed in this method, or not.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitState(Connection conn, String pluginId,
      String auKey, String stateString, boolean commitAfterAdd)
	  throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("stateString = {}", stateString);
    log.debug2("commitAfterAdd = {}", commitAfterAdd);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin, adding it if necessary.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The Archival Unit creation time.
      long auCreationTime;
      try {
        auCreationTime = ((Number)AuUtil.jsonToMap(stateString).get("auCreationTime")).longValue();
      }
      catch (IOException exc) {
        auCreationTime = TimeBase.nowMs(); // FIXME
      }
      log.trace("auCreationTime = {}", auCreationTime);

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, auCreationTime);

      // Add the new state of the Archival Unit.
      addArchivalUnitState(conn, auSeq, stateString);

      if (commitAfterAdd) {
	// Commit the transaction.
	ConfigDbManager.commitOrRollback(conn, log);
      }
    } catch (DbException dbe) {
      String message = "Cannot add AU state";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("stateString = {}", stateString);
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
   * @param stateString
   *          A Map<String, Object> with the Archival Unit state properties.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitState(Connection conn, Long auSeq,
      String stateString) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("stateString = {}", stateString);

    PreparedStatement addState = null;
    String errorMessage = "Cannot add Archival Unit configuration";

    try {
      // Prepare the query.
      addState = configDbManager.prepareStatement(conn, ADD_AU_STATE_QUERY);

      // Populate the query.
      addState.setLong(1, auSeq);
      addState.setString(2, stateString);

      // Execute the query
      int count = configDbManager.executeUpdate(addState);
      log.debug2("addedCount = {}", count);
      return count;
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("stateString = {}", stateString);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_AU_STATE_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("stateString = {}", stateString);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addState);
    }
  }

}
