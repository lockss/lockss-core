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
package org.lockss.config.db;

import static org.lockss.config.db.SqlConstants.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.PluginManager;

/**
 * The ConfigManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class ConfigManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  private final ConfigDbManager configDbManager;

  // Query to find a plugin by its identifier.
  private static final String FIND_PLUGIN_QUERY = "select "
      + PLUGIN_SEQ_COLUMN
      + " from " + PLUGIN_TABLE
      + " where " + PLUGIN_ID_COLUMN + " = ?";

  // Query to add a plugin.
  private static final String INSERT_PLUGIN_QUERY = "insert into "
      + PLUGIN_TABLE
      + "(" + PLUGIN_SEQ_COLUMN
      + "," + PLUGIN_ID_COLUMN
      + ") values (default,?)";

  // Query to find an Archival Unit by its plugin and key.
  private static final String FIND_ARCHIVAL_UNIT_QUERY = "select "
      + ARCHIVAL_UNIT_SEQ_COLUMN
      + " from " + ARCHIVAL_UNIT_TABLE
      + " where " + PLUGIN_SEQ_COLUMN + " = ?"
      + " and " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?";

  // Query to add an Archival Unit.
  private static final String INSERT_ARCHIVAL_UNIT_QUERY = "insert into "
      + ARCHIVAL_UNIT_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + PLUGIN_SEQ_COLUMN
      + "," + ARCHIVAL_UNIT_KEY_COLUMN
      + "," + CREATION_TIME_COLUMN
      + "," + LAST_UPDATE_TIME_COLUMN
      + ") values (default,?,?,?,?)";

  // Query to find the configurations of all the Archival Units.
  private static final String GET_ALL_AU_CONFIGURATION_QUERY = "select "
      + "p." + PLUGIN_ID_COLUMN
      + ", a." + ARCHIVAL_UNIT_KEY_COLUMN
      + ", a." + ARCHIVAL_UNIT_SEQ_COLUMN
      + ", ac." + CONFIG_KEY_COLUMN
      + ", ac." + CONFIG_VALUE_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + " left outer join " + ARCHIVAL_UNIT_CONFIG_TABLE + " ac"
      + " on a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = ac."
      + ARCHIVAL_UNIT_SEQ_COLUMN
      + " order by a." + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to find the configurations of an Archival Unit.
  private static final String GET_AU_CONFIGURATION_QUERY = "select "
      + "ac." + CONFIG_KEY_COLUMN
      + ", ac." + CONFIG_VALUE_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + ", " + ARCHIVAL_UNIT_CONFIG_TABLE + " ac"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = ac."
      + ARCHIVAL_UNIT_SEQ_COLUMN;

  // Query to find the creation time of an Archival Unit.
  private static final String GET_AU_CREATION_TIME_QUERY = "select "
      + "a." + CREATION_TIME_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?";

  // Query to find the last update time of an Archival Unit.
  private static final String GET_AU_LAST_UPDATE_TIME_QUERY = "select "
      + "a." + LAST_UPDATE_TIME_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + " where p." + PLUGIN_ID_COLUMN + " = ?"
      + " and p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and a." + ARCHIVAL_UNIT_KEY_COLUMN + " = ?";

  // Query to update the last update time of an Archival Unit.
  private static final String UPDATE_ARCHIVAL_UNIT_LAST_UPDATE_TIME_QUERY =
      "update "
      + ARCHIVAL_UNIT_TABLE
      + " set " + LAST_UPDATE_TIME_COLUMN + " = ?"
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  // Query to add a configuration property of an Archival Unit.
  private static final String ADD_AU_CONFIGURATION_QUERY = "insert into "
      + ARCHIVAL_UNIT_CONFIG_TABLE
      + "(" + ARCHIVAL_UNIT_SEQ_COLUMN
      + "," + CONFIG_KEY_COLUMN
      + "," + CONFIG_VALUE_COLUMN
      + ") values (?,?,?)";

  // Query to delete an Archival Unit.
  private static final String DELETE_ARCHIVAL_UNIT_QUERY = "delete from "
      + ARCHIVAL_UNIT_TABLE
      + " where " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?"
      + " and " + PLUGIN_SEQ_COLUMN + " = (select " + PLUGIN_SEQ_COLUMN
      + " from " + PLUGIN_TABLE + " where " + PLUGIN_ID_COLUMN + " = ?)";

  // Query to delete the configuration of an Archival Unit.
  private static final String DELETE_AU_CONFIGURATION_QUERY = "delete from "
      + ARCHIVAL_UNIT_CONFIG_TABLE
      + " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

  /**
   * Constructor.
   */
  public ConfigManagerSql(ConfigDbManager configDbManager) throws DbException {
    this.configDbManager = configDbManager;
  }

  /**
   * Adds to the database the configuration of an Archival Unit.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @param auConfig
   *          A Map<String,String> with the Archival Unit configuration.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long addArchivalUnitConfiguration(String pluginId, String auKey,
      Map<String,String> auConfig) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);
    log.debug2("auConfig = {}", () -> auConfig);

    Long auSeq = null;
    Connection conn = null;

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Find the Archival Unit plugin, adding it if necessary.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = new Date().getTime();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

      // Delete the configuration of the Archival Unit, if it exists.
      removeArchivalUnitConfiguration(conn, auSeq);

      // Add the new configuration of the Archival Unit.
      addArchivalUnitConfiguration(conn, auSeq, auConfig);

      // Update the Archival Unit last update timestamp.
      updateArchivalUnitLastUpdateTimestamp(conn, auSeq, now);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (DbException dbe) {
      String message = "Cannot add AU configuration";
      log.error(message, dbe);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("auConfig = {}", auConfig);
      throw dbe;
    } finally {
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Provides all the Archival Unit configurations stored in the database.
   * 
   * @return a Map<String, Map<String,String>> with all the Archival Unit
   *         configurations, keyed by each Archival Unit identifier.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Map<String,String>> findAllArchivalUnitConfiguration()
      throws DbException {
    log.debug2("Invoked");

    Map<String, Map<String,String>> result = new HashMap<>();
    Map<String,String> auConfig = null;
    Connection conn = null;
    PreparedStatement getConfigurations = null;
    ResultSet resultSet = null;
    Long previousAuSeq = null;
    String errorMessage = "Cannot get AU configurations";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      getConfigurations = configDbManager.prepareStatement(conn,
	  GET_ALL_AU_CONFIGURATION_QUERY);

      // Get the configurations grouped by Archival Unit.
      resultSet = configDbManager.executeQuery(getConfigurations);

      // Loop while there are more results.
      while (resultSet.next()) {
	// Get the Archival Unit database identifier of this result.
	Long auSeq = resultSet.getLong(ARCHIVAL_UNIT_SEQ_COLUMN);
  	log.trace("auSeq = {}", auSeq);

  	// Check whether this Archival Unit database identifier does not match
  	// the previous one.
  	if (!auSeq.equals(previousAuSeq)) {
  	  // Yes: Get the identifier of the plugin of this result.
  	  String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
  	  log.trace("pluginId = {}", pluginId);
 
  	  // Get the key identifier of the Archival Unit of this result.
  	  String auKey = resultSet.getString(ARCHIVAL_UNIT_KEY_COLUMN);
  	  log.trace("auKey = {}", auKey);
  
  	  // Build the Archival Unit identifier.
  	  String auId = PluginManager.generateAuId(pluginId, auKey);
  	  log.trace("auId = {}", auId);

  	  // Initialize the configuration of this newly seen Archival Unit.
  	  auConfig = new HashMap<>();
  	  result.put(auId, auConfig);

  	  // Remember this result Archival Unit database identifier.
  	  previousAuSeq = auSeq;
  	}

	// Get the key of the Archival Unit configuration property of this
	// result.
  	String key = resultSet.getString(CONFIG_KEY_COLUMN);
  	log.trace("key = {}", key);

	// Get the value of the Archival Unit configuration property of this
	// result.
  	String value = resultSet.getString(CONFIG_VALUE_COLUMN);
  	log.trace("value = {}", value);

  	// Save the property.
  	auConfig.put(key, value);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_ALL_AU_CONFIGURATION_QUERY);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_ALL_AU_CONFIGURATION_QUERY);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getConfigurations);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("Done");
    return result;
  }

  /**
   * Provides the configuration of an Archival Unit stored in the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Map<String,String> with the Archival Unit configurations.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String,String> findArchivalUnitConfiguration(String pluginId,
      String auKey) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Map<String,String> auConfig = new HashMap<>();
    Connection conn = null;
    PreparedStatement getConfiguration = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU configuration";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      getConfiguration =
	  configDbManager.prepareStatement(conn, GET_AU_CONFIGURATION_QUERY);

      // Populate the query.
      getConfiguration.setString(1, pluginId);
      getConfiguration.setString(2, auKey);

      // Get the configuration of the Archival Unit.
      resultSet = configDbManager.executeQuery(getConfiguration);

      // Loop while there are more results.
      while (resultSet.next()) {
	// Get the key of the Archival Unit configuration property.
  	String key = resultSet.getString(CONFIG_KEY_COLUMN);
  	log.trace("key = {}", key);

	// Get the value of the Archival Unit configuration property.
  	String value = resultSet.getString(CONFIG_VALUE_COLUMN);
  	log.trace("value = {}", value);

  	// Save the property.
  	auConfig.put(key, value);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_CONFIGURATION_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_CONFIGURATION_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getConfiguration);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("auConfig = {}", () -> auConfig);
    return auConfig;
  }

  /**
   * Provides the configuration creation time of an Archival Unit stored in the
   * database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Long with the Archival Unit configuration creation time, as epoch
   *         milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findArchivalUnitCreationTime(String pluginId, String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Long result = null;
    Connection conn = null;
    PreparedStatement getCreationTime = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU creation time";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      getCreationTime =
	  configDbManager.prepareStatement(conn, GET_AU_CREATION_TIME_QUERY);

      // Populate the query.
      getCreationTime.setString(1, pluginId);
      getCreationTime.setString(2, auKey);

      // Get the Archival Unit configuration creation time.
      resultSet = configDbManager.executeQuery(getCreationTime);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the creation time of the Archival Unit configuration.
  	result = resultSet.getLong(CREATION_TIME_COLUMN);
  	log.trace("result = {}", result);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_CREATION_TIME_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_CREATION_TIME_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getCreationTime);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides the configuration last update time of an Archival Unit stored in
   * the database.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return a Long with the Archival Unit configuration last update time, as
   *         epoch milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Long findArchivalUnitLastUpdateTime(String pluginId, String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    Long result = null;
    Connection conn = null;
    PreparedStatement getLastUpdateTime = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get AU last update time";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      getLastUpdateTime =
	  configDbManager.prepareStatement(conn, GET_AU_LAST_UPDATE_TIME_QUERY);

      // Populate the query.
      getLastUpdateTime.setString(1, pluginId);
      getLastUpdateTime.setString(2, auKey);

      // Get the Archival Unit configuration last update time.
      resultSet = configDbManager.executeQuery(getLastUpdateTime);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the last update time of the Archival Unit configuration.
  	result = resultSet.getLong(LAST_UPDATE_TIME_COLUMN);
  	log.trace("result = {}", result);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_AU_LAST_UPDATE_TIME_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_AU_LAST_UPDATE_TIME_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getLastUpdateTime);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Removes from the database an Archival Unit.
   * 
   * @param pluginId
   *          A String with the Archival Unit plugin identifier.
   * @param auKey
   *          A String with the Archival Unit key identifier.
   * @return an int with the count of database rows removed.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public int removeArchivalUnit(String pluginId, String auKey)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);
    log.debug2("auKey = {}", auKey);

    int deletedCount = -1;
    Connection conn = null;
    PreparedStatement deleteAu = null;
    String errorMessage = "Cannot delete Archival Unit configuration";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      deleteAu =
	  configDbManager.prepareStatement(conn, DELETE_ARCHIVAL_UNIT_QUERY);

      // Populate the query.
      deleteAu.setString(1, auKey);
      deleteAu.setString(2, pluginId);

      // Remove the Archival Unit.
      deletedCount = configDbManager.executeUpdate(deleteAu);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_ARCHIVAL_UNIT_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", DELETE_ARCHIVAL_UNIT_QUERY);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(deleteAu);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("deletedCount = {}", deletedCount);
    return deletedCount;
  }

  /**
   * Provides the database identifier of a plugin if existing or after creating
   * it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @return a Long with the database identifier of the plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findOrCreatePlugin(Connection conn, String pluginId)
      throws DbException {
    log.debug2("pluginId = {}", pluginId);

    // Find the plugin in the database.
    Long pluginSeq = findPlugin(conn, pluginId);
    log.trace("pluginSeq = {}", pluginSeq);

    // Check whether it is a new plugin.
    if (pluginSeq == null) {
      // Yes: Add to the database the new plugin.
      pluginSeq = addPlugin(conn, pluginId);
      log.trace("new pluginSeq = {}", pluginSeq);
    }

    log.debug2("pluginSeq = {}", pluginSeq);
    return pluginSeq;
  }

  /**
   * Provides the database identifier of a plugin.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @return a Long with the database identifier of the plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findPlugin(Connection conn, String pluginId) throws DbException {
    log.debug2("pluginId = {}", pluginId);
    Long pluginSeq = null;
    PreparedStatement findPlugin = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find plugin";

    try {
      // Prepare the query.
      findPlugin = configDbManager.prepareStatement(conn, FIND_PLUGIN_QUERY);

      // Populate the query.
      findPlugin.setString(1, pluginId);

      // Get the plugin.
      resultSet = configDbManager.executeQuery(findPlugin);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the plugin database identifier.
	pluginSeq = resultSet.getLong(PLUGIN_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_PLUGIN_QUERY);
      log.error("pluginId = {}", pluginId);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", FIND_PLUGIN_QUERY);
      log.error("pluginId = {}", pluginId);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findPlugin);
    }

    log.debug2("pluginSeq = {}", pluginSeq);
    return pluginSeq;
  }

  /**
   * Adds a plugin to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginId
   *          A String with the plugin identifier.
   * @return a Long with the database identifier of the plugin just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long addPlugin(Connection conn, String pluginId) throws DbException {
    log.debug2("pluginId = {}", pluginId);

    Long pluginSeq = null;
    PreparedStatement insertPlugin = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add plugin";

    try {
      // Prepare the query.
      insertPlugin = configDbManager.prepareStatement(conn,
	  INSERT_PLUGIN_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query. Skip auto-increment column #0.
      insertPlugin.setString(1, pluginId);

      // Add the plugin.
      configDbManager.executeUpdate(insertPlugin);
      resultSet = insertPlugin.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
	// Yes: Report the problem.
	String message =
	    "Unable to create plugin table row for pluginId = " + pluginId;
	log.error(message);
	throw new DbException(message);
      }

      // No: Get the plugin database identifier.
      pluginSeq = resultSet.getLong(1);
      log.trace("Added pluginSeq = {}", pluginSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_PLUGIN_QUERY);
      log.error("pluginId = {}", pluginId);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_PLUGIN_QUERY);
      log.error("pluginId = {}", pluginId);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertPlugin);
    }

    log.debug2("pluginSeq = {}", pluginSeq);
    return pluginSeq;
  }
  
  /**
   * Provides the identifier of an Archival Unit if existing or after creating
   * it otherwise.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginSeq
   *          A Long with the identifier of the plugin.
   * @param auKey
   *          A String with the Archival Unit key.
   * @param creationTime
   *          A long with the Archival Unit creation time as epoch milliseconds.
   * @return a Long with the database identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findOrCreateArchivalUnit(Connection conn, Long pluginSeq,
      String auKey, long creationTime) throws DbException {
    log.debug2("pluginSeq = {}", pluginSeq);
    log.debug2("auKey = {}", auKey);
    log.debug2("creationTime = {}", creationTime);

    // Find the Archival Unit in the database.
    Long auSeq = findArchivalUnit(conn, pluginSeq, auKey);
    log.trace("auSeq = {}", auSeq);

    // Check whether it is a new Archival Unit.
    if (auSeq == null) {
      // Yes: Add to the database the new Archival Unit.
      auSeq =
	  addArchivalUnit(conn, pluginSeq, auKey, creationTime, creationTime);
      log.trace("new auSeq = {}", auSeq);
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Provides the identifier of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginSeq
   *          A Long with the identifier of the plugin.
   * @param auKey
   *          A String with the Archival Unit key.
   * @return a Long with the identifier of the Archival Unit.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long findArchivalUnit(Connection conn, Long pluginSeq, String auKey)
      throws DbException {
    log.debug2("pluginSeq = {}", pluginSeq);
    log.debug2("auKey = {}", auKey);

    Long auSeq = null;
    PreparedStatement findAu = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find AU";

    try {
      // Prepare the query.
      findAu = configDbManager.prepareStatement(conn, FIND_ARCHIVAL_UNIT_QUERY);

      // Populate the query.
      findAu.setLong(1, pluginSeq);
      findAu.setString(2, auKey);

      // Get the Archival Unit.
      resultSet = configDbManager.executeQuery(findAu);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the Archival Unit database identifier.
	auSeq = resultSet.getLong(ARCHIVAL_UNIT_SEQ_COLUMN);
	log.trace("Found auSeq = {}", auSeq);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_ARCHIVAL_UNIT_QUERY);
      log.error("pluginSeq = {}", pluginSeq);
      log.error("auKey = {}", auKey);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", FIND_ARCHIVAL_UNIT_QUERY);
      log.error("pluginSeq = {}", pluginSeq);
      log.error("auKey = {}", auKey);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(findAu);
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Adds an Archival Unit to the database.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param pluginSeq
   *          A Long with the identifier of the plugin.
   * @param auKey
   *          A String with the Archival Unit key.
   * @param creationTime
   *          A long with the Archival Unit creation time as epoch milliseconds.
   * @param lastUpdateTime
   *          A long with the Archival Unit last update time as epoch
   *          milliseconds.
   * @return a Long with the identifier of the Archival Unit just added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private Long addArchivalUnit(Connection conn, Long pluginSeq, String auKey,
      long creationTime, long lastUpdateTime) throws DbException {
    log.debug2("pluginSeq = {}", pluginSeq);
    log.debug2("auKey = {}", auKey);
    log.debug2("creationTime = {}", creationTime);
    log.debug2("lastUpdateTime = {}", lastUpdateTime);

    Long auSeq = null;
    PreparedStatement insertAu = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add AU";

    try {
      // Prepare the query.
      insertAu = configDbManager.prepareStatement(conn,
	  INSERT_ARCHIVAL_UNIT_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query. Skip auto-increment column #0.
      insertAu.setLong(1, pluginSeq);
      insertAu.setString(2, auKey);
      insertAu.setLong(3, creationTime);
      insertAu.setLong(4, lastUpdateTime);

      // Add the Archival Unit.
      configDbManager.executeUpdate(insertAu);
      resultSet = insertAu.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
	// Yes: Report the problem.
	String message =
	    "Unable to create archival unit table row for aukey = " + auKey;
	log.error(message);
	throw new DbException(message);
      }

      // No: Get the Archival Unit database identifier.
      auSeq = resultSet.getLong(1);
      log.trace("Added auSeq = {}", auSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_ARCHIVAL_UNIT_QUERY);
      log.error("pluginSeq = {}", pluginSeq);
      log.error("auKey = {}", auKey);
      log.error("creationTime = {}", creationTime);
      log.error("lastUpdateTime = {}", lastUpdateTime);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", INSERT_ARCHIVAL_UNIT_QUERY);
      log.error("pluginSeq = {}", pluginSeq);
      log.error("auKey = {}", auKey);
      log.error("creationTime = {}", creationTime);
      log.error("lastUpdateTime = {}", lastUpdateTime);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(insertAu);
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
  }

  /**
   * Updates the timestamp of the last update of an Archival Unit configuration.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @param lastUpdateTime
   *          A long with the Archival Unit last update time as epoch
   *          milliseconds.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void updateArchivalUnitLastUpdateTimestamp(Connection conn,
      Long auSeq, long lastUpdateTime) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("lastUpdateTime = {}", lastUpdateTime);

    PreparedStatement updateAuLastUpdateTime = null;
    String errorMessage = "Cannot update the Archival Unit last update time";

    try {
      // Prepare the query.
      updateAuLastUpdateTime = configDbManager.prepareStatement(conn,
	  UPDATE_ARCHIVAL_UNIT_LAST_UPDATE_TIME_QUERY);

      // Populate the query.
      updateAuLastUpdateTime.setLong(1, lastUpdateTime);
      updateAuLastUpdateTime.setLong(2, auSeq);

      // Update the last update timestamp.
      configDbManager.executeUpdate(updateAuLastUpdateTime);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPDATE_ARCHIVAL_UNIT_LAST_UPDATE_TIME_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("lastUpdateTime = {}", lastUpdateTime);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", UPDATE_ARCHIVAL_UNIT_LAST_UPDATE_TIME_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("lastUpdateTime = {}", lastUpdateTime);
      throw dbe;
    } finally {
      DbManager.safeCloseStatement(updateAuLastUpdateTime);
    }

    log.debug2("Done");
  }

  /**
   * Adds to the database the configuration of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the identifier of the Archival Unit metadata.
   * @param auConfig
   *          A Map<String,String> with the Archival Unit configuration.
   * @return an int with the count of database rows added.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int addArchivalUnitConfiguration(Connection conn, Long auSeq,
      Map<String,String> auConfig) throws DbException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("auConfig = {}", () -> auConfig);

    int addedCount = 0;
    String keyInProgress = null;
    String valueInProgress = null;

    PreparedStatement addConfiguration = null;
    String errorMessage = "Cannot add Archival Unit configuration";

    try {
      // Prepare the query.
      addConfiguration =
	  configDbManager.prepareStatement(conn, ADD_AU_CONFIGURATION_QUERY);

      // Loop through all the configuration properties.
      for (String key : auConfig.keySet()) {
	// Populate the query with this property.
	addConfiguration.setLong(1, auSeq);
	keyInProgress = key;
	valueInProgress = auConfig.get(key);
	addConfiguration.setString(2, keyInProgress);
	addConfiguration.setString(3, valueInProgress);

	// Add this property to the database.
	int count = configDbManager.executeUpdate(addConfiguration);
	log.trace("count = {}", count);

	// Update the count of added rows.
	addedCount += count;
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", ADD_AU_CONFIGURATION_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("auConfig = {}", auConfig);
      log.error("Failed Key = {}", keyInProgress);
      log.error("Failed Value = {}",valueInProgress);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", ADD_AU_CONFIGURATION_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("auConfig = {}", auConfig);
      log.error("Failed Key = {}", keyInProgress);
      log.error("Failed Value = {}",valueInProgress);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(addConfiguration);
    }

    log.debug2("addedCount = {}", addedCount);
    return addedCount;
  }

  /**
   * Deletes the configuration of an Archival Unit.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param auSeq
   *          A Long with the Archival Unit identifier.
   * @return an int with the count of database rows deleted.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private int removeArchivalUnitConfiguration(Connection conn, Long auSeq)
      throws DbException {
    log.debug2("auSeq = {}", auSeq);

    int deletedCount = -1;
    PreparedStatement deleteAuConfig = null;
    String errorMessage = "Cannot delete Archival Unit configuration";

    try {
      // Prepare the query.
      deleteAuConfig =
	  configDbManager.prepareStatement(conn, DELETE_AU_CONFIGURATION_QUERY);

      // Populate the query.
      deleteAuConfig.setLong(1, auSeq);

      // Delete the Archival Unit configuration.
      deletedCount = configDbManager.executeUpdate(deleteAuConfig);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", DELETE_AU_CONFIGURATION_QUERY);
      log.error("auSeq = {}", auSeq);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", DELETE_AU_CONFIGURATION_QUERY);
      log.error("auSeq = {}", auSeq);
      throw dbe;
    } finally {
      ConfigDbManager.safeCloseStatement(deleteAuConfig);
    }

    log.debug2("deletedCount = {}", deletedCount);
    return deletedCount;
  }
}
