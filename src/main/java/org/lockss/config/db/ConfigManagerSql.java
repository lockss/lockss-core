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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.PluginManager;
import org.lockss.util.time.TimeBase;

/**
 * The ConfigManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class ConfigManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  private final ConfigDbManager configDbManager;

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

  // Query to find the identifiers of all the plugins.
  private static final String GET_ALL_PLUGIN_ID_QUERY = "select "
      + PLUGIN_ID_COLUMN
      + " from " + PLUGIN_TABLE;

  // Query to find the configurations of all the Archival Units.
  private static final String GET_PLUGIN_AU_CONFIGURATION_QUERY = "select "
      + "a." + ARCHIVAL_UNIT_KEY_COLUMN
      + ", a." + ARCHIVAL_UNIT_SEQ_COLUMN
      + ", ac." + CONFIG_KEY_COLUMN
      + ", ac." + CONFIG_VALUE_COLUMN
      + " from " + PLUGIN_TABLE + " p"
      + ", " + ARCHIVAL_UNIT_TABLE + " a"
      + " left outer join " + ARCHIVAL_UNIT_CONFIG_TABLE + " ac"
      + " on a." + ARCHIVAL_UNIT_SEQ_COLUMN + " = ac."
      + ARCHIVAL_UNIT_SEQ_COLUMN
      + " where p." + PLUGIN_SEQ_COLUMN + " = a." + PLUGIN_SEQ_COLUMN
      + " and p." + PLUGIN_ID_COLUMN + " = ?"
      + " order by a." + ARCHIVAL_UNIT_SEQ_COLUMN;

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
      // Get the SQL helper.
      ConfigDbManagerSql configDbManagerSql =
	  configDbManager.getConfigDbManagerSql();

      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Find the Archival Unit plugin, adding it if necessary.
      Long pluginSeq = configDbManagerSql.findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = configDbManagerSql.findOrCreateArchivalUnit(conn, pluginSeq,
	  auKey, now);

      // Delete the configuration of the Archival Unit, if it exists.
      removeArchivalUnitConfiguration(conn, auSeq);

      // Add the new configuration of the Archival Unit.
      configDbManagerSql.addArchivalUnitConfiguration(conn, auSeq, auConfig);

      // Update the Archival Unit last update timestamp.
      configDbManagerSql.updateArchivalUnitLastUpdateTimestamp(conn, auSeq,
	  now);

      // Commit the transaction.
      ConfigDbManager.commitOrRollback(conn, log);
    } catch (SQLException sqle) {
      String message = "Cannot add AU configuration";
      log.error(message, sqle);
      log.error("pluginId = {}", pluginId);
      log.error("auKey = {}", auKey);
      log.error("auConfig = {}", auConfig);
      throw new DbException(message, sqle);
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

  /**
   * Provides all the plugin identifiers stored in the database.
   * 
   * @return a Collection<String> with all the plugin identifiers.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Collection<String> findAllPluginIds() throws DbException {
    log.debug2("Invoked");

    Collection<String> result = new ArrayList<>();
    Connection conn = null;
    PreparedStatement getPluginIds = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot get plugin identifiers";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      getPluginIds =
	  configDbManager.prepareStatement(conn, GET_ALL_PLUGIN_ID_QUERY);

      // Get the plugin identifiers.
      resultSet = configDbManager.executeQuery(getPluginIds);

      // Loop while there are more results.
      while (resultSet.next()) {
	// Get the plugin identifier of this result.
	String pluginId = resultSet.getString(PLUGIN_ID_COLUMN);
	log.trace("pluginId = {}", pluginId);
 
  	// Save it.
	result.add(pluginId);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", GET_ALL_PLUGIN_ID_QUERY);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_ALL_PLUGIN_ID_QUERY);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getPluginIds);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("Done");
    return result;
  }

  /**
   * Provides the Archival Unit configurations for a plugin that are stored in
   * the database.
   * 
   * @param pluginId
   *          A String with the plugin identifier.
   * @return a Map<String, Map<String, String>> with the Archival Unit
   *         configurations for the plugin.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public Map<String, Map<String,String>> findPluginAuConfigurations(
      String pluginId) throws DbException {
    log.debug2("pluginId = {}", pluginId);

    Map<String, Map<String,String>> result = new HashMap<>();
    Map<String,String> auConfig = null;
    Connection conn = null;
    PreparedStatement getPluginAuConfigurations = null;
    ResultSet resultSet = null;
    Long previousAuSeq = null;
    String errorMessage =
	"Cannot get AU configurations for plugin '" + pluginId + "'";

    try {
      // Get a connection to the database.
      conn = configDbManager.getConnection();

      // Prepare the query.
      getPluginAuConfigurations = configDbManager.prepareStatement(conn,
	  GET_PLUGIN_AU_CONFIGURATION_QUERY);

      // Populate the query.
      getPluginAuConfigurations.setString(1, pluginId);

      // Get the configurations grouped by Archival Unit.
      resultSet = configDbManager.executeQuery(getPluginAuConfigurations);

      // Loop while there are more results.
      while (resultSet.next()) {
	// Get the Archival Unit database identifier of this result.
	Long auSeq = resultSet.getLong(ARCHIVAL_UNIT_SEQ_COLUMN);
  	log.trace("auSeq = {}", auSeq);

  	// Check whether this Archival Unit database identifier does not match
  	// the previous one.
  	if (!auSeq.equals(previousAuSeq)) {
  	  // Yes: Get the key identifier of the Archival Unit of this result.
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
      log.error("SQL = '{}'.", GET_PLUGIN_AU_CONFIGURATION_QUERY);
      log.error("pluginId = {}", pluginId);
      throw new DbException(errorMessage, sqle);
    } catch (DbException dbe) {
      log.error(errorMessage, dbe);
      log.error("SQL = '{}'.", GET_PLUGIN_AU_CONFIGURATION_QUERY);
      log.error("pluginId = {}", pluginId);
      throw dbe;
    } finally {
      DbManager.safeCloseResultSet(resultSet);
      DbManager.safeCloseStatement(getPluginAuConfigurations);
      DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("Done");
    return result;
  }
}
