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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.lockss.config.ConfigManager;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.db.DbManagerSql;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.PluginManager;
import org.lockss.util.time.TimeBase;

/**
 * The ConfigDbManager SQL code executor.
 * 
 * @author Fernando García-Loygorri
 */
public class ConfigDbManagerSql extends DbManagerSql {
  private static L4JLogger log = L4JLogger.getLogger();

  // Query to create the table for recording plugins.
  private static final String CREATE_PLUGIN_TABLE_QUERY = "create table "
      + PLUGIN_TABLE + " ("
      + PLUGIN_SEQ_COLUMN + " --BigintSerialPk--,"
      + PLUGIN_ID_COLUMN + " varchar(" + MAX_PLUGIN_ID_COLUMN + ") not null"
      + ")";

  // Query to create the table for recording archival units.
  static final String CREATE_ARCHIVAL_UNIT_TABLE_QUERY = "create table "
      + ARCHIVAL_UNIT_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " --BigintSerialPk--,"
      + PLUGIN_SEQ_COLUMN + " bigint not null references " + PLUGIN_TABLE
      + " (" + PLUGIN_SEQ_COLUMN + ") on delete cascade,"
      + ARCHIVAL_UNIT_KEY_COLUMN + " varchar(" + MAX_ARCHIVAL_UNIT_KEY_COLUMN
      + ") not null,"
      + CREATION_TIME_COLUMN + " bigint not null,"
      + LAST_UPDATE_TIME_COLUMN + " bigint not null"
      + ")";

  // Query to create the table for recording archival units configurations.
  static final String CREATE_ARCHIVAL_UNIT_CONFIG_TABLE_QUERY = "create table "
      + ARCHIVAL_UNIT_CONFIG_TABLE + " ("
      + ARCHIVAL_UNIT_SEQ_COLUMN + " bigint not null references "
      + ARCHIVAL_UNIT_TABLE + " (" + ARCHIVAL_UNIT_SEQ_COLUMN
      + ") on delete cascade,"
      + CONFIG_KEY_COLUMN + " varchar(" + MAX_CONFIG_KEY_COLUMN + ") not null,"
      + CONFIG_VALUE_COLUMN + " varchar(" + MAX_CONFIG_VALUE_COLUMN
      + ") not null"
      + ")";

  // The SQL code used to create the necessary version 1 database tables.
  @SuppressWarnings("serial")
  private static final Map<String, String> VERSION_1_TABLE_CREATE_QUERIES =
    new LinkedHashMap<String, String>() {{
      put(PLUGIN_TABLE, CREATE_PLUGIN_TABLE_QUERY);
      put(ARCHIVAL_UNIT_TABLE, CREATE_ARCHIVAL_UNIT_TABLE_QUERY);
      put(ARCHIVAL_UNIT_CONFIG_TABLE, CREATE_ARCHIVAL_UNIT_CONFIG_TABLE_QUERY);
    }};

  // SQL statements that create the necessary version 1 indices.
  private static final String[] VERSION_1_INDEX_CREATE_QUERIES = new String[] {
      "create unique index idx1_" + PLUGIN_TABLE + " on " + PLUGIN_TABLE
      + "(" + PLUGIN_ID_COLUMN + ")",
      "create index idx1_" + ARCHIVAL_UNIT_TABLE + " on " + ARCHIVAL_UNIT_TABLE
      + "(" + ARCHIVAL_UNIT_KEY_COLUMN + ")",
      "create index idx2_" + ARCHIVAL_UNIT_TABLE + " on " + ARCHIVAL_UNIT_TABLE
      + "(" + PLUGIN_SEQ_COLUMN + ")",
      "create unique index idx1_" + ARCHIVAL_UNIT_CONFIG_TABLE + " on "
      + ARCHIVAL_UNIT_CONFIG_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN + ","
      + CONFIG_KEY_COLUMN + ")"
  };

  // SQL statements that create the necessary version 1 indices for MySQL.
  private static final String[] VERSION_1_INDEX_CREATE_MYSQL_QUERIES =
      new String[] {
	  // TODO: Make the index unique when MySQL is fixed.
	  "create index idx1_" + PLUGIN_TABLE + " on " + PLUGIN_TABLE
	  + "(" + PLUGIN_ID_COLUMN + "(255))",
	  "create index idx1_" + ARCHIVAL_UNIT_TABLE + " on "
	  + ARCHIVAL_UNIT_TABLE + "(" + ARCHIVAL_UNIT_KEY_COLUMN + "(255))",
	  "create index idx2_" + ARCHIVAL_UNIT_TABLE + " on "
	  + ARCHIVAL_UNIT_TABLE + "(" + PLUGIN_SEQ_COLUMN + ")",
	  "create unique index idx1_" + ARCHIVAL_UNIT_CONFIG_TABLE + " on "
	  + ARCHIVAL_UNIT_CONFIG_TABLE + "(" + ARCHIVAL_UNIT_SEQ_COLUMN + ","
	  + CONFIG_KEY_COLUMN + ")"
  };

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

  /**
   * Constructor.
   * 
   * @param dataSource
   *          A DataSource with the datasource that provides the connection.
   * @param dataSourceClassName
   *          A String with the data source class name.
   * @param dataSourceUser
   *          A String with the data source user name.
   * @param maxRetryCount
   *          An int with the maximum number of retries to be attempted.
   * @param retryDelay
   *          A long with the number of milliseconds to wait between consecutive
   *          retries.
   * @param fetchSize
   *          An int with the SQL statement fetch size.
   */
  ConfigDbManagerSql(DataSource dataSource, String dataSourceClassName,
      String dataSourceUser, int maxRetryCount, long retryDelay, int fetchSize)
      {
    super(dataSource, dataSourceClassName, dataSourceUser, maxRetryCount,
	retryDelay, fetchSize);
  }

  /**
   * Sets up the database to version 1.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred setting up the database.
   */
  void setUpDatabaseVersion1(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // Create the necessary tables if they do not exist.
    createTablesIfMissing(conn, VERSION_1_TABLE_CREATE_QUERIES);

    // Create the necessary indices.
    if (isTypeMysql()) {
      executeDdlQueries(conn, VERSION_1_INDEX_CREATE_MYSQL_QUERIES);
    } else {
      executeDdlQueries(conn, VERSION_1_INDEX_CREATE_QUERIES);
    }

    log.debug2("Done");
  }

  /**
   * Updates the database from version 1 to version 2.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  void updateDatabaseFrom1To2(Connection conn) throws SQLException {
    log.debug2("Invoked");

    if (conn == null) {
      throw new IllegalArgumentException("Null connection");
    }

    // The file with the configurations of the Archival Units to be stored in
    // the database.
    File auTxtFile = ConfigManager.getConfigManager().getCacheConfigFile(
	ConfigManager.CONFIG_FILE_AU_CONFIG);
    log.trace("auTxtFile = {}", () -> auTxtFile);

    // Check where the file does not exist.
    if (!auTxtFile.exists()) {
      // Done with this update.
      log.info("Skipping migration of missing file {}", () -> auTxtFile);
      return;
    }

    // The line prefix of a line with an Archival Unit configuration property.
    String auConfigPrefix = PluginManager.PARAM_AU_TREE + ".";

    String previousAuId = null;
    Map<String,String> auConfig = null;

    // Process the file with the configurations of the Archival Units, line by
    // line
    log.debug("Starting to read the file {}", () -> auTxtFile);

    try (BufferedReader br = new BufferedReader(new FileReader(auTxtFile))) {
      for(String line; (line = br.readLine()) != null; ) {
	log.trace("line = {}", line);

	// Skip lines that do not start with the appropriate prefix.
	if (!line.startsWith(auConfigPrefix)) {
	  log.trace("Skipped.");
	  continue;
	}

	// Load the line as a Properties property, as it was written that way.
	Properties properties = new Properties();
	properties.load(new StringReader(line));

	// Get the property name.
	String propertyName =
	    properties.stringPropertyNames().iterator().next();
	log.trace("propertyName = {}", propertyName);

	// Remove the line prefix and replace the first dot with an ampersand.
	String auIdAndKey = propertyName.substring(auConfigPrefix.length())
	    .replaceFirst("\\.", "&");
	log.trace("auIdAndKey = {}", auIdAndKey);

	// Locate the separator between the Archival Unit ID and the
	// configuration property key.
	int dotLocation = auIdAndKey.indexOf(".");
	log.trace("dotLocation = {}", dotLocation);

	// Skip lines that do not have the appropriate separator.
	if (dotLocation < 1 || dotLocation == auIdAndKey.length()) {
	  log.error("Skipping invalid line '{}'", line);
	  continue;
	}

	// Get the Archival Unit ID.
	String auId = auIdAndKey.substring(0, dotLocation);
	log.trace("auId = {}", auId);

	// Check whether this Archival Unit ID is different than the one for the
	// previous line.
	if (!auId.equals(previousAuId)) {
	  // Yes.
	  log.trace("Newly seen auId = {}", auId);

	  // Check whether the configuration of a previous Archival Unit has
	  // been read already.
	  if (previousAuId != null) {
	    // Yes: Store in the database the configuration of the previous
	    // Archival Unit before forgetting about it.
	    log.trace("Storing the configuration of previousAuId = {}",
		previousAuId);
	    Long auSeq =
		addArchivalUnitConfiguration(conn, previousAuId, auConfig);
	    log.info("Stored configuration for auid '{}': auSeq = {}",
		previousAuId, auSeq);
	  }

	  // Remember this new Archival Unit.
	  previousAuId = auId;
	  auConfig = new HashMap<>();
	}

	// Save this line configuration property.
	String key = auIdAndKey.substring(dotLocation + 1);
	log.trace("key = {}", key);

	String value = properties.getProperty(propertyName);
	log.trace("value = {}", value);

	auConfig.put(key, value);
      }
    } catch (IOException ioe) {
      log.error("Cannot process file " + auTxtFile, ioe);
      return;
    }

    log.debug("The file has been completely read.");

    // Store in the database the configuration of the last Archival Unit in the
    // file.
    if (previousAuId != null && !auConfig.isEmpty()) {
      Long auSeq = addArchivalUnitConfiguration(conn, previousAuId, auConfig);
      log.info("Stored configuration for auid '{}': auSeq = {}", previousAuId,
	  auSeq);
    }

    // TODO: Uncomment the next block once the au.txt file has been completely
    // migrated.
//    boolean renamed = auTxtFile.renameTo(ConfigManager.getConfigManager()
//	.getCacheConfigFile(ConfigManager.CONFIG_FILE_AU_CONFIG + ".migrated"));
//    log.trace("renamed = {}", renamed);

    log.debug2("Done");
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
  public Long addArchivalUnitConfiguration(Connection conn, String auId,
      Map<String,String> auConfig) throws SQLException {
    log.debug2("auId = {}", auId);
    log.debug2("auConfig = {}", () -> auConfig);

    String pluginId = PluginManager.pluginIdFromAuId(auId);
    String auKey = PluginManager.auKeyFromAuId(auId);

    Long auSeq = null;

    try {
      // Find the Archival Unit plugin, adding it if necessary.
      Long pluginSeq = findOrCreatePlugin(conn, pluginId);

      // The current time.
      long now = TimeBase.nowMs();

      // Find the Archival Unit, adding it if necessary.
      auSeq = findOrCreateArchivalUnit(conn, pluginSeq, auKey, now);

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
      throw new SQLException(message);
    } finally {
      //DbManager.safeRollbackAndClose(conn);
    }

    log.debug2("auSeq = {}", auSeq);
    return auSeq;
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  Long findOrCreatePlugin(Connection conn, String pluginId)
      throws SQLException {
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private Long findPlugin(Connection conn, String pluginId)
      throws SQLException {
    log.debug2("pluginId = {}", pluginId);
    Long pluginSeq = null;
    PreparedStatement findPlugin = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find plugin";

    try {
      // Prepare the query.
      findPlugin = prepareStatement(conn, FIND_PLUGIN_QUERY);

      // Populate the query.
      findPlugin.setString(1, pluginId);

      // Get the plugin.
      resultSet = executeQuery(findPlugin);

      // Check whether a result was obtained.
      if (resultSet.next()) {
	// Yes: Get the plugin database identifier.
	pluginSeq = resultSet.getLong(PLUGIN_SEQ_COLUMN);
      }
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", FIND_PLUGIN_QUERY);
      log.error("pluginId = {}", pluginId);
      throw sqle;
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private Long addPlugin(Connection conn, String pluginId) throws SQLException {
    log.debug2("pluginId = {}", pluginId);

    Long pluginSeq = null;
    PreparedStatement insertPlugin = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot add plugin";

    try {
      // Prepare the query.
      insertPlugin = prepareStatement(conn, INSERT_PLUGIN_QUERY,
	  Statement.RETURN_GENERATED_KEYS);

      // Populate the query. Skip auto-increment column #0.
      insertPlugin.setString(1, pluginId);

      // Add the plugin.
      executeUpdate(insertPlugin);
      resultSet = insertPlugin.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
	// Yes: Report the problem.
	String message =
	    "Unable to create plugin table row for pluginId = " + pluginId;
	log.error(message);
	throw new SQLException(message);
      }

      // No: Get the plugin database identifier.
      pluginSeq = resultSet.getLong(1);
      log.trace("Added pluginSeq = {}", pluginSeq);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", INSERT_PLUGIN_QUERY);
      log.error("pluginId = {}", pluginId);
      throw sqle;
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  Long findOrCreateArchivalUnit(Connection conn, Long pluginSeq,
      String auKey, long creationTime) throws SQLException {
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private Long findArchivalUnit(Connection conn, Long pluginSeq, String auKey)
      throws SQLException {
    log.debug2("pluginSeq = {}", pluginSeq);
    log.debug2("auKey = {}", auKey);

    Long auSeq = null;
    PreparedStatement findAu = null;
    ResultSet resultSet = null;
    String errorMessage = "Cannot find AU";

    try {
      // Prepare the query.
      findAu = prepareStatement(conn, FIND_ARCHIVAL_UNIT_QUERY);

      // Populate the query.
      findAu.setLong(1, pluginSeq);
      findAu.setString(2, auKey);

      // Get the Archival Unit.
      resultSet = executeQuery(findAu);

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
      throw sqle;
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  private Long addArchivalUnit(Connection conn, Long pluginSeq, String auKey,
      long creationTime, long lastUpdateTime) throws SQLException {
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
      insertAu = prepareStatement(conn,
	  INSERT_ARCHIVAL_UNIT_QUERY, Statement.RETURN_GENERATED_KEYS);

      // Populate the query. Skip auto-increment column #0.
      insertAu.setLong(1, pluginSeq);
      insertAu.setString(2, auKey);
      insertAu.setLong(3, creationTime);
      insertAu.setLong(4, lastUpdateTime);

      // Add the Archival Unit.
      executeUpdate(insertAu);
      resultSet = insertAu.getGeneratedKeys();

      // Check whether a result was not obtained.
      if (!resultSet.next()) {
	// Yes: Report the problem.
	String message =
	    "Unable to create archival unit table row for aukey = " + auKey;
	log.error(message);
	throw new SQLException(message);
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
      throw sqle;
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  void updateArchivalUnitLastUpdateTimestamp(Connection conn,
      Long auSeq, long lastUpdateTime) throws SQLException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("lastUpdateTime = {}", lastUpdateTime);

    PreparedStatement updateAuLastUpdateTime = null;
    String errorMessage = "Cannot update the Archival Unit last update time";

    try {
      // Prepare the query.
      updateAuLastUpdateTime = prepareStatement(conn,
	  UPDATE_ARCHIVAL_UNIT_LAST_UPDATE_TIME_QUERY);

      // Populate the query.
      updateAuLastUpdateTime.setLong(1, lastUpdateTime);
      updateAuLastUpdateTime.setLong(2, auSeq);

      // Update the last update timestamp.
      executeUpdate(updateAuLastUpdateTime);
    } catch (SQLException sqle) {
      log.error(errorMessage, sqle);
      log.error("SQL = '{}'.", UPDATE_ARCHIVAL_UNIT_LAST_UPDATE_TIME_QUERY);
      log.error("auSeq = {}", auSeq);
      log.error("lastUpdateTime = {}", lastUpdateTime);
      throw sqle;
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
   * @throws SQLException
   *           if any problem occurred accessing the database.
   */
  int addArchivalUnitConfiguration(Connection conn, Long auSeq,
      Map<String,String> auConfig) throws SQLException {
    log.debug2("auSeq = {}", auSeq);
    log.debug2("auConfig = {}", () -> auConfig);

    int addedCount = 0;
    String keyInProgress = null;
    String valueInProgress = null;

    PreparedStatement addConfiguration = null;
    String errorMessage = "Cannot add Archival Unit configuration";

    try {
      // Prepare the query.
      addConfiguration = prepareStatement(conn, ADD_AU_CONFIGURATION_QUERY);

      // Loop through all the configuration properties.
      for (String key : auConfig.keySet()) {
	// Populate the query with this property.
	addConfiguration.setLong(1, auSeq);
	keyInProgress = key;
	valueInProgress = auConfig.get(key);
	addConfiguration.setString(2, keyInProgress);
	addConfiguration.setString(3, valueInProgress);

	// Add this property to the database.
	int count = executeUpdate(addConfiguration);
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
      throw sqle;
    } finally {
      ConfigDbManager.safeCloseStatement(addConfiguration);
    }

    log.debug2("addedCount = {}", addedCount);
    return addedCount;
  }
}
