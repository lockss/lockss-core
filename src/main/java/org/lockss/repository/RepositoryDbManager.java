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

import org.lockss.app.ConfigurableManager;
import org.lockss.config.Configuration;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;

import java.sql.Connection;
import java.sql.SQLException;

public class RepositoryDbManager extends DbManager implements ConfigurableManager {
  private static L4JLogger log = L4JLogger.getLogger();

  // Prefix for the database manager configuration entries.
  private static final String PREFIX = Configuration.PREFIX
      + "repositoryDbManager.";

//  // Prefix for the datasource configuration entries.
//  private static final String DATASOURCE_ROOT = PREFIX + "datasource";
//
//  /**
//   * Name of the database schema. Changes require daemon restart.
//   */
//  public static final String PARAM_DATASOURCE_SCHEMA_NAME = DATASOURCE_ROOT
//      + ".schemaName";
//
//  /**
//   * Name of the database with the relative path to the DB directory. Changes
//   * require daemon restart.
//   */
//  public static final String PARAM_DATASOURCE_DATABASENAME = DATASOURCE_ROOT
//      + ".databaseName";
//
//  /**
//   * Indication of whether the startup code should wait for the external setup
//   * of the database. Changes require daemon restart.
//   */
//  public static final String PARAM_WAIT_FOR_EXTERNAL_SETUP = PREFIX
//      + "waitForExternalSetup";

  // Prefix for the Derby configuration entries.
  private static final String DERBY_ROOT = PREFIX + "derby";

  /**
   * Derby log append option. Changes require daemon restart.
   */
  public static final String PARAM_DERBY_INFOLOG_APPEND = DERBY_ROOT
      + ".infologAppend";

  /**
   * Derby log query plan option. Changes require daemon restart.
   */
  public static final String PARAM_DERBY_LANGUAGE_LOGQUERYPLAN = DERBY_ROOT
      + ".languageLogqueryplan";

  /**
   * Derby log statement text option. Changes require daemon restart.
   */
  public static final String PARAM_DERBY_LANGUAGE_LOGSTATEMENTTEXT = DERBY_ROOT
      + ".languageLogstatementtext";

  /**
   * Name of the Derby log file path. Changes require daemon restart.
   */
  public static final String PARAM_DERBY_STREAM_ERROR_FILE = DERBY_ROOT
      + ".streamErrorFile";

  /**
   * Name of the Derby log severity level. Changes require daemon restart.
   */
  public static final String PARAM_DERBY_STREAM_ERROR_LOGSEVERITYLEVEL =
      DERBY_ROOT + ".streamErrorLogseveritylevel";

  // Prefix for the datasource configuration entries.
  private static final String DATASOURCE_ROOT = PREFIX + "datasource";

  /**
   * Name of the database datasource class. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_CLASSNAME = DATASOURCE_ROOT
      + ".className";

  /**
   * Name of the database create. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_CREATEDATABASE = DATASOURCE_ROOT
      + ".createDatabase";

  /**
   * Name of the database with the relative path to the DB directory. Changes
   * require daemon restart.
   */
  public static final String PARAM_DATASOURCE_DATABASENAME = DATASOURCE_ROOT
      + ".databaseName";

  /**
   * Port number of the database. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_PORTNUMBER = DATASOURCE_ROOT
      + ".portNumber";

  /**
   * Name of the server. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_SERVERNAME = DATASOURCE_ROOT
      + ".serverName";

  /**
   * Name of the database user. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_USER = DATASOURCE_ROOT + ".user";

  /**
   * Name of the existing database password. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_PASSWORD = DATASOURCE_ROOT
      + ".password";

  /**
   * Name of the database schema. Changes require daemon restart.
   */
  public static final String PARAM_DATASOURCE_SCHEMA_NAME = DATASOURCE_ROOT
      + ".schemaName";

  /**
   * Set to false to prevent ConfigDbManager from running
   */
  public static final String PARAM_DBMANAGER_ENABLED = PREFIX + "enabled";

  /**
   * Maximum number of retries for transient SQL exceptions.
   */
  public static final String PARAM_MAX_RETRY_COUNT = PREFIX + "maxRetryCount";

  /**
   * Delay  between retries for transient SQL exceptions.
   */
  public static final String PARAM_RETRY_DELAY = PREFIX + "retryDelay";

  /**
   * SQL statement fetch size.
   */
  public static final String PARAM_FETCH_SIZE = PREFIX + "fetchSize";

  /**
   * Indication of whether the startup code should wait for the external setup
   * of the database. Changes require daemon restart.
   */
  public static final String PARAM_WAIT_FOR_EXTERNAL_SETUP = PREFIX
      + "waitForExternalSetup";

  private RepositoryDbManagerSql repoDbManagerSql =
      new RepositoryDbManagerSql(null,
          DEFAULT_DATASOURCE_CLASSNAME, DEFAULT_DATASOURCE_USER,
          DEFAULT_MAX_RETRY_COUNT, DEFAULT_RETRY_DELAY, DEFAULT_FETCH_SIZE);

  /**
   * Default constructor.
   */
  public RepositoryDbManager() {
    super();
    setUpVersions();
  }

  /**
   * Sets up update versions.
   */
  private void setUpVersions() {
    targetDatabaseVersion = 1;
  }

  @Override
  public void startService() {
    log.debug2("Invoked");

    setDbManagerSql(repoDbManagerSql);
    super.startService();
  }

  @Override
  protected void updateDatabaseToVersion(Connection conn, int databaseVersion)
      throws SQLException {
    log.debug2("databaseVersion = {}", databaseVersion);

    // Perform the appropriate update for this version.
    if (databaseVersion == 1) {
      repoDbManagerSql.setUpDatabaseVersion1(conn);
    } else {
      throw new RuntimeException("Non-existent method to update the database "
          + "to version " + databaseVersion + ".");
    }

    log.debug2("Done");
  }

  @Override
  protected String getDataSourceRootName() {
    return DATASOURCE_ROOT;
  }

  @Override
  protected String getDataSourceDatabaseName(Configuration config) {
    // Return the configured database name.
    return getFullDataSourceDatabaseName(config.get(
        PARAM_DATASOURCE_DATABASENAME,
        getSimpleDbName()));
  }

  @Override
  protected String getDataSourceSchemaName(Configuration config) {
    return config.get(PARAM_DATASOURCE_SCHEMA_NAME, getDataSourceUser(config));
  }

  @Override
  protected boolean getWaitForExternalSetup(Configuration config) {
    return config.getBoolean(PARAM_WAIT_FOR_EXTERNAL_SETUP,
        DEFAULT_WAIT_FOR_EXTERNAL_SETUP);
  }
}
