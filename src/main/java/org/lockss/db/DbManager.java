/*

 Copyright (c) 2013-2020 Board of Trustees of Leland Stanford Jr. University,
 all rights reserved.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 Except as contained in this notice, the name of Stanford University shall not
 be used in advertising or otherwise to promote the sale, use or other dealings
 in this Software without prior written authorization from Stanford University.

 */
package org.lockss.db;

import static org.lockss.db.SqlConstants.*;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.lockss.app.BaseLockssManager;
import org.lockss.app.ConfigurableManager;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.log.L4JLogger;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;

/**
 * Generic database manager.
 * 
 * @author Fernando García-Loygorri
 */
public abstract class DbManager extends BaseLockssManager
  implements ConfigurableManager {
  protected static final Logger log = Logger.getLogger();

  // Prefix for the database manager configuration entries.
  private static final String PREFIX = Configuration.PREFIX + "db.";

  /**
   * Derby log append option. Changes require daemon restart.
   */
  protected static final String DEFAULT_DERBY_INFOLOG_APPEND = "false";

  /**
   * Derby log query plan option. Changes require daemon restart.
   */
  protected static final String DEFAULT_DERBY_LANGUAGE_LOGQUERYPLAN = "false";

  /**
   * Derby log statement text option. Changes require daemon restart.
   */
  protected static final String DEFAULT_DERBY_LANGUAGE_LOGSTATEMENTTEXT =
      "false";

  /**
   * Name of the Derby log file path. Changes require daemon restart.
   */
  protected static final String DEFAULT_DERBY_STREAM_ERROR_FILE = "derby.log";

  /**
   * Name of the Derby log severity level. Changes require daemon restart.
   */
  protected static final String DEFAULT_DERBY_STREAM_ERROR_LOGSEVERITYLEVEL =
      "4000";

  /**
   * Name of the database datasource class. Changes require daemon restart.
   */
  protected static final String DEFAULT_DATASOURCE_CLASSNAME =
      EmbeddedDataSource.class.getCanonicalName();

  /**
   * Name of the database create. Changes require daemon restart.
   */
  protected static final String DEFAULT_DATASOURCE_CREATEDATABASE = "create";

  /**
   * Port number of the database. Changes require daemon restart.
   */
  protected static final String DEFAULT_DATASOURCE_PORTNUMBER = "1527";
  protected static final String DEFAULT_DATASOURCE_PORTNUMBER_PG = "5432";
  protected static final String DEFAULT_DATASOURCE_PORTNUMBER_MYSQL = "3306";

  /**
   * Name of the server. Changes require daemon restart.
   */
  protected static final String DEFAULT_DATASOURCE_SERVERNAME = "localhost";

  /**
   * Name of the database user. Changes require daemon restart.
   */
  protected static final String DEFAULT_DATASOURCE_USER = "LOCKSS";

  /**
   * Name of the existing database password. Changes require daemon restart.
   */
  protected static final String DEFAULT_DATASOURCE_PASSWORD = "insecure";

  /**
   * Set to false to prevent DbManager from running
   */
  protected static final boolean DEFAULT_DBMANAGER_ENABLED = true;

  /**
   * Maximum number of retries for transient SQL exceptions.
   */
  protected static final int DEFAULT_MAX_RETRY_COUNT = 10;

  /**
   * Delay  between retries for transient SQL exceptions.
   */
  protected static final long DEFAULT_RETRY_DELAY = 3 * Constants.SECOND;

  /**
   * SQL statement fetch size.
   */
  public static final int DEFAULT_FETCH_SIZE = 5000;

  /**
   * Indication of whether the startup code should wait for the external setup
   * of the database. Changes require daemon restart.
   */
  protected static final boolean DEFAULT_WAIT_FOR_EXTERNAL_SETUP = false;

  /**
   * The name of the configuration parameter for the prefix for database names.
   */
  public static final String PARAM_DATABASE_NAME_PREFIX =
      PREFIX + "databaseNamePrefix";

  /** 
   * The default prefix for database names.
   */
  private static final String DEFAULT_DATABASE_NAME_PREFIX = "Lockss";

  /**
   * The name of the configuration parameter for the option to start the Derby
   * Network Server Control.
   */
  public static final String PARAM_START_DERBY_NETWORK_SERVER_CONTROL =
      PREFIX + "startDerbyNetworkServerControl";

  /** 
   * The default value for the option to start the Derby Network Server Control.
   */
  private static final boolean DEFAULT_START_DERBY_NETWORK_SERVER_CONTROL =
      false;

  /**
   * Absolute or relative path to base dir to store Derby databases.  If
   * not set defaults to "<cache-dir>/db"
   */
  public static final String PARAM_DERBY_DB_DIR = PREFIX + "derbyDbDir";

  // Derby SQL state of exception thrown on successful database shutdown.
  private static final String SHUTDOWN_SUCCESS_STATE_CODE = "08006";

  // An indication of whether this object has been enabled.
  protected boolean dbManagerEnabled = DEFAULT_DBMANAGER_ENABLED;

  // The database data source.
  protected DataSource dataSource = null;

  // The data source configuration.
  protected Configuration dataSourceConfig = null;

  // The data source class name.
  protected String dataSourceClassName = null;

  // The data source database name.
  protected String dataSourceDbName = null;

  // The data source user.
  protected String dataSourceUser = null;

  // The data source password.
  protected String dataSourcePassword = null;

  // The data source schema name.
  protected String dataSourceSchemaName = null;

  // The network server control.
  protected NetworkServerControl networkServerControl = null;

  // An indication of whether this object is ready to be used.
  protected boolean ready = false;

  // The version of the database subsystem to be targeted by this daemon.
  //
  // After this service has started successfully, this is the version of the
  // database subsystem that will be in place, as long as the database subsystem
  // version prior to starting the service was not higher already.
  protected int targetDatabaseVersion = 0;

  // The database version updates that are performed asynchronously.
  protected int[] asynchronousUpdates = null;

  // An indication of whether to perform only synchronous updates to the
  // database. This is useful for performance reasons when creating an empty
  // database from scratch.
  protected boolean skipAsynchronousUpdates = false;

  // The maximum number of retries to be attempted when encountering transient
  // SQL exceptions.
  protected int maxRetryCount = DEFAULT_MAX_RETRY_COUNT;

  // The interval to wait between consecutive retries when encountering
  // transient SQL exceptions.
  protected long retryDelay = DEFAULT_RETRY_DELAY;

  // The SQL statement fetch size.
  protected int fetchSize = DEFAULT_FETCH_SIZE;

  // An indication of whether the database was booted.
  protected boolean dbBooted = false;

  // The spawned threads.
  protected List<Thread> threads = new ArrayList<Thread>();

  // The SQL code executor.
  protected DbManagerSql dbManagerSql = new DbManagerSql(null,
      DEFAULT_DATASOURCE_CLASSNAME, DEFAULT_DATASOURCE_USER,
      DEFAULT_MAX_RETRY_COUNT, DEFAULT_RETRY_DELAY, DEFAULT_FETCH_SIZE);

  private static Map<String, DbCredentials> dbCredentialsMap =
      new HashMap<String, DbCredentials>();

  // The database subsystem.
  private static final String DB_VERSION_SUBSYSTEM = "DbManager";

  // The interval in milliseconds between consecutive checks while waiting for a
  // database to be set up externally.
  static final long waitForExternalSetupInterval = 15 * Constants.SECOND;

  // The interval message used while waiting for a database to be set up
  // externally.
  static final String waitForExternalSetupMessage =
      "Sleeping for 15 seconds...";

  // The prefix for database names.
  private String databaseNamePrefix = DEFAULT_DATABASE_NAME_PREFIX;

  // The value for the option to start the Derby Network Server Control.
  private boolean shouldStartDerbyNetworkServerControl =
      DEFAULT_START_DERBY_NETWORK_SERVER_CONTROL;

  private String derbyDbBaseDir;
  /**
   * Default constructor.
   */
  public DbManager() {
  }

  /**
   * Constructor.
   * 
   * @param skipAsynchronousUpdates A boolean indicating whether to skip the
   * asynchronous updates and just mark them as done.
   */
  public DbManager(boolean skipAsynchronousUpdates) {
    this.skipAsynchronousUpdates = skipAsynchronousUpdates;
  }

  /**
   * Starts the DbManager service.
   */
  @Override
  public void startService() {
    final String DEBUG_HEADER = "startService(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Do nothing if not enabled
    if (!dbManagerEnabled) {
      log.info("DbManager not enabled.");
      return;
    }

    // Do nothing more if it is already initialized.
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "dataSource != null = " + (dataSource != null));
    ready = ready && dataSource != null;
    if (ready) {
      return;
    }

    try {
      // Set up the database infrastructure.
      setUpInfrastructure();

      // Update the existing database if necessary.
      updateDatabaseIfNeeded(targetDatabaseVersion);

      ready = true;
    } catch (DbException dbe) {
      log.error(dbe.getMessage() + " - DbManager not ready", dbe);
      // Do nothing more if the database infrastructure cannot be setup.
      dataSource = null;
      dbManagerSql.setDataSource(dataSource);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "DbManager ready? = " + ready);
  }

  /**
   * Handler of configuration changes.
   * 
   * @param config
   *          A Configuration with the new configuration.
   * @param prevConfig
   *          A Configuration with the previous configuration.
   * @param changedKeys
   *          A Configuration.Differences with the keys of the configuration
   *          elements that have changed.
   */
  @Override
  public void setConfig(Configuration config, Configuration prevConfig,
			Configuration.Differences changedKeys) {
    final String DEBUG_HEADER = "setConfig(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    if (changedKeys.contains(PREFIX)) {
      // Update the prefix for database names.
      databaseNamePrefix =
	  config.get(PARAM_DATABASE_NAME_PREFIX, DEFAULT_DATABASE_NAME_PREFIX);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "databaseNamePrefix = "
	  + databaseNamePrefix);

      // Update the option to start the Derby Network Server Control.
      shouldStartDerbyNetworkServerControl =
	  config.getBoolean(PARAM_START_DERBY_NETWORK_SERVER_CONTROL,
	      DEFAULT_START_DERBY_NETWORK_SERVER_CONTROL);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "shouldStartDerbyNetworkServerControl = "
	  + shouldStartDerbyNetworkServerControl);

      derbyDbBaseDir = config.get(PARAM_DERBY_DB_DIR);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Stops the DbManager service.
   */
  @Override
  public void stopService() {
    // Check whether the Derby database was booted.
    if (dbManagerSql.isTypeDerby() && dbBooted) {
      try {
	// Yes: Shutdown the database.
	shutdownDerbyDb(dataSourceConfig);

	// Stop the network server control, if it had been started.
	if (networkServerControl != null) {
	  networkServerControl.shutdown();
	}
      } catch (Exception e) {
	log.error("Cannot shutdown the database cleanly", e);
      }
    }

    ready = false;
    dataSource = null;
    dbManagerSql.setDataSource(dataSource);
  }

  protected void setDbManagerSql(DbManagerSql dbManagerSql) {
    this.dbManagerSql = dbManagerSql;
  }

  /**
   * Closes a result set without throwing exceptions.
   * 
   * @param resultSet
   *          A ResultSet with the database result set to be closed.
   */
  public static void safeCloseResultSet(ResultSet resultSet) {
    DbManagerSql.safeCloseResultSet(resultSet);
  }

  /**
   * Closes a statement without throwing exceptions.
   * 
   * @param statement
   *          A Statement with the database statement to be closed.
   */
  public static void safeCloseStatement(Statement statement) {
    DbManagerSql.safeCloseStatement(statement);
  }

  /**
   * Closes a connection without throwing exceptions.
   * 
   * @param conn
   *          A Connection with the database connection to be closed.
   */
  public static void safeCloseConnection(Connection conn) {
    DbManagerSql.safeCloseConnection(conn);
  }

  /**
   * Rolls back and closes a connection without throwing exceptions.
   * 
   * @param conn
   *          A Connection with the database connection to be rolled back and
   *          closed.
   */
  public static void safeRollbackAndClose(Connection conn) {
    DbManagerSql.safeRollbackAndClose(conn);
  }

  /**
   * Commits a connection or rolls it back if it's not possible.
   * 
   * @param conn
   *          A connection with the database connection to be committed.
   * @param logger
   *          A Logger used to report errors.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public static void commitOrRollback(Connection conn, Logger logger)
      throws DbException {
    try {
      DbManagerSql.commitOrRollback(conn, logger);
    } catch (SQLException sqle) {
      String message = "Cannot commit the connection";
      logger.error(message, sqle);
      DbManagerSql.safeRollbackAndClose(conn);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Cannot commit the connection";
      logger.error(message, re);
      DbManagerSql.safeRollbackAndClose(conn);
      throw new DbException(message, re);
    }
  }

  /**
   * Commits a connection or rolls it back if it's not possible.
   * 
   * @param conn
   *          A connection with the database connection to be committed.
   * @param logger
   *          A L4JLogger used to report errors.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public static void commitOrRollback(Connection conn, L4JLogger logger)
      throws DbException {
    try {
      DbManagerSql.commitOrRollback(conn, logger);
    } catch (SQLException sqle) {
      String message = "Cannot commit the connection";
      logger.error(message, sqle);
      DbManagerSql.safeRollbackAndClose(conn);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Cannot commit the connection";
      logger.error(message, re);
      DbManagerSql.safeRollbackAndClose(conn);
      throw new DbException(message, re);
    }
  }

  /**
   * Rolls back a transaction.
   * 
   * @param conn
   *          A connection with the database connection to be rolled back.
   * @param logger
   *          A Logger used to report errors.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  public static void rollback(Connection conn, Logger logger)
      throws DbException {
    try {
      DbManagerSql.rollback(conn, logger);
    } catch (SQLException sqle) {
      String message = "Cannot roll back the connection";
      logger.error(message, sqle);
      DbManagerSql.safeRollbackAndClose(conn);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Cannot roll back the connection";
      logger.error(message, re);
      DbManagerSql.safeRollbackAndClose(conn);
      throw new DbException(message, re);
    }
  }

  /**
   * Provides a version of a text truncated to a maximum length, if necessary,
   * including an indication of the truncation.
   * 
   * @param original
   *          A String with the original text to be truncated, if necessary.
   * @param maxLength
   *          An int with the maximum length of the truncated text to be
   *          provided.
   * @return a String with the original text if it is not longer than the
   *         maximum length allowed or the truncated text including an
   *         indication of the truncation.
   */
  public static String truncateVarchar(String original, int maxLength) {
    return DbManagerSql.truncateVarchar(original, maxLength);
  }

  /**
   * Provides an indication of whether this object is ready to be used.
   * 
   * @return <code>true</code> if this object is ready to be used,
   *         <code>false</code> otherwise.
   */
  public boolean isReady() {
    return ready;
  }

  /**
   * Provides an indication of whether the Derby database is being used.
   * 
   * @return <code>true</code> if the Derby database is being used,
   *         <code>false</code> otherwise.
   */
  public boolean isTypeDerby() {
    return dbManagerSql.isTypeDerby();
  }

  /**
   * Provides an indication of whether the PostgreSQL database is being used.
   * 
   * @return <code>true</code> if the PostgreSQL database is being used,
   *         <code>false</code> otherwise.
   */
  public boolean isTypePostgresql() {
    return dbManagerSql.isTypePostgresql();
  }

  /**
   * Provides an indication of whether the MySQL database is being used.
   * 
   * @return <code>true</code> if the MySQL database is being used,
   *         <code>false</code> otherwise.
   */
  public boolean isTypeMysql() {
    return dbManagerSql.isTypeMysql();
  }

  /**
   * Provides a database connection using the default datasource, retrying the
   * operation in the default manner in case of transient failures.
   * <p>
   * Autocommit is disabled to allow the client code to manage transactions.
   * 
   * @return a Connection with the database connection to be used.
   * @throws DbException
   *           if any problem occurred getting the connection.
   */
  public Connection getConnection() throws DbException {
    if (!ready) {
      throw new DbException("DbManager has not been initialized.");
    }

    try {
      return dbManagerSql.getConnection(dataSource, maxRetryCount,
	  retryDelay, false, true);
    } catch (SQLException sqle) {
      throw new DbException("Cannot get a connection to the database", sqle);
    } catch (RuntimeException re) {
      throw new DbException("Cannot get a connection to the database", re);
    }
  }

  /**
   * Prepares a statement, retrying the preparation in the default manner in
   * case of transient failures.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sql
   *          A String with the prepared statement SQL query.
   * @return a PreparedStatement with the prepared statement.
   * @throws DbException
   *           if any problem occurred preparing the statement.
   */
  public PreparedStatement prepareStatement(Connection conn, String sql)
      throws DbException {
    if (!ready) {
      throw new DbException("DbManager has not been initialized.");
    }

    try {
      return dbManagerSql.prepareStatement(conn, sql, maxRetryCount,
	retryDelay);
    } catch (SQLException sqle) {
      String message = "Cannot prepare statement";
      log.error(message, sqle);
      log.error("sql = '" + sql + "'");
      log.error("maxRetryCount = " + maxRetryCount);
      log.error("retryDelay = " + retryDelay);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Cannot prepare statement";
      log.error(message, re);
      log.error("sql = '" + sql + "'");
      log.error("maxRetryCount = " + maxRetryCount);
      log.error("retryDelay = " + retryDelay);
      throw new DbException(message, re);
    }
  }

  /**
   * Prepares a statement, retrying the preparation in the default manner in
   * case of transient failures and specifying whether to return generated keys.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param sql
   *          A String with the prepared statement SQL query.
   * @param returnGeneratedKeys
   *          An int indicating whether generated keys should be made available
   *          for retrieval.
   * @return a PreparedStatement with the prepared statement.
   * @throws DbException
   *           if any problem occurred preparing the statement.
   */
  public PreparedStatement prepareStatement(Connection conn, String sql,
      int returnGeneratedKeys) throws DbException {
    if (!ready) {
      throw new DbException("DbManager has not been initialized.");
    }

    try {
      return dbManagerSql.prepareStatement(conn, sql, returnGeneratedKeys,
	  maxRetryCount, retryDelay);
    } catch (SQLException sqle) {
      String message = "Cannot prepare statement";
      log.error(message, sqle);
      log.error("sql = '" + sql + "'");
      log.error("returnGeneratedKeys = " + returnGeneratedKeys);
      log.error("maxRetryCount = " + maxRetryCount);
      log.error("retryDelay = " + retryDelay);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Cannot prepare statement";
      log.error(message, re);
      log.error("sql = '" + sql + "'");
      log.error("returnGeneratedKeys = " + returnGeneratedKeys);
      log.error("maxRetryCount = " + maxRetryCount);
      log.error("retryDelay = " + retryDelay);
      throw new DbException(message, re);
    }
  }

  /**
   * Executes a querying prepared statement, retrying the execution in the
   * default manner in case of transient failures.
   * 
   * @param statement
   *          A PreparedStatement with the querying prepared statement to be
   *          executed.
   * @return a ResultSet with the results of the query.
   * @throws DbException
   *           if any problem occurred executing the query.
   */
  public ResultSet executeQuery(PreparedStatement statement) throws DbException
  {
    if (!ready) {
      throw new DbException("DbManager has not been initialized.");
    }

    try {
      return dbManagerSql.executeQuery(statement, maxRetryCount, retryDelay);
    } catch (SQLException sqle) {
      throw new DbException("Cannot execute query", sqle);
    } catch (RuntimeException re) {
      throw new DbException("Cannot execute query", re);
    }
  }

  /**
   * Executes an updating prepared statement, retrying the execution in the
   * default manner in case of transient failures.
   * 
   * @param statement
   *          A PreparedStatement with the updating prepared statement to be
   *          executed.
   * @return an int with the number of database rows updated.
   * @throws DbException
   *           if any problem occurred executing the query.
   */
  public int executeUpdate(PreparedStatement statement) throws DbException {
    if (!ready) {
      throw new DbException("DbManager has not been initialized.");
    }

    try {
      return dbManagerSql.executeUpdate(statement, maxRetryCount, retryDelay);
    } catch (SQLException sqle) {
      throw new DbException("Cannot execute update", sqle);
    } catch (RuntimeException re) {
      throw new DbException("Cannot execute update", re);
    }
  }

  /**
   * Provides the SQL code executor.
   * 
   * @return a DbManagerSql with the SQL code executor.
   * @throws DbException
   *           if this object is not ready.
   */
  protected DbManagerSql getDbManagerSql() throws DbException {
    if (!ready) {
      throw new DbException("DbManager has not been initialized.");
    }

    return getDbManagerSqlBeforeReady();
  }

  /**
   * Provides the SQL code executor. To be used during initialization.
   * 
   * @return a DbManagerSql with the SQL code executor.
   */
  private DbManagerSql getDbManagerSqlBeforeReady() {
    return dbManagerSql;
  }

  /**
   * Provides the data source class name. To be used during initialization.
   * 
   * @return a String with the data source class name.
   */
  public String getDataSourceClassNameBeforeReady() {
    return dataSourceClassName;
  }

  /**
   * Provides the data source database name. To be used during initialization.
   * 
   * @return a String with the data source database name.
   */
  public String getDataSourceDbNameBeforeReady() {
    return dataSourceDbName;
  }

  /**
   * Provides the data source user name. To be used during initialization.
   * 
   * @return a String with the data source user name.
   */
  public String getDataSourceUserBeforeReady() {
    return dataSourceUser;
  }

  /**
   * Provides the data source password. To be used during initialization.
   * 
   * @return a String with the data source password.
   */
  public String getDataSourcePasswordBeforeReady() {
    return dataSourcePassword;
  }

  /**
   * Provides the data source schema name. To be used during initialization.
   * 
   * @return a String with the data source schema name.
   */
  public String getDataSourceSchemaNameBeforeReady() {
    return dataSourceSchemaName;
  }

  /**
   * Creates a datasource using the specified class name.
   * 
   * @param dsClassName
   *          A String with the datasource class name.
   * @return a DataSource with the created datasource.
   * @throws DbException
   *           if the datasource could not be created.
   */
  protected DataSource createDataSource(String dsClassName) throws DbException {
    final String DEBUG_HEADER = "createDataSource(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "dsClassName = '" + dsClassName + "'.");

    // Locate the datasource class.
    Class<?> dataSourceClass;

    try {
      dataSourceClass = Class.forName(dsClassName);
    } catch (Throwable t) {
      throw new DbException("Cannot locate datasource class '" + dsClassName
	  + "'", t);
    }

    // Create the datasource.
    try {
      return ((DataSource) dataSourceClass.newInstance());
    } catch (ClassCastException cce) {
      throw new DbException("Class '" + dsClassName + "' is not a DataSource.",
	  cce);
    } catch (Throwable t) {
      throw new DbException("Cannot create instance of datasource class '"
	  + dsClassName + "'", t);
    }
  }

  /**
   * Sets up the database infrastructure.
   * 
   * @throws DbException
   *           if any problem occurred setting up the database infrastructure.
   */
  protected void setUpInfrastructure() throws DbException {
    final String DEBUG_HEADER = "setUpInfrastructure(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Get the datasource configuration.
    dataSourceConfig = getDataSourceConfig();

    // Check whether the Derby database is being used.
    if (dbManagerSql.isTypeDerby()) {
      // Yes: Set up the Derby database properties.
      setDerbyDatabaseConfiguration();
    }

    // Check whether authentication is required and it is not available.
    if (StringUtil.isNullString(dataSourceUser)
	|| (dbManagerSql.isTypeDerby()
	&& !EmbeddedDataSource.class.getCanonicalName()
	    .equals(dataSourceClassName)
	&& !EmbeddedConnectionPoolDataSource.class.getCanonicalName()
	    .equals(dataSourceClassName)
	&& StringUtil.isNullString(dataSourcePassword))) {
      // Yes: Report the problem.
      throw new DbException("Missing required authentication");
    }

    // No: Create the datasource.
    dataSource = createDataSource(dataSourceConfig.get("className"));
    dbManagerSql.setDataSource(dataSource);

    // Check whether the database is external.
    if (dbManagerSql.isTypePostgresql() || dbManagerSql.isTypeMysql()) {
      // Yes: Initialize the database, if necessary.
      initializeExternalDbIfNeeded(dataSourceConfig);
    }

    // Initialize the datasource properties.
    initializeDataSourceProperties(dataSourceConfig, dataSource);

    // Get the indication of whether the startup code should wait for the
    // external setup of the database.
    boolean waitForExternalSetup =
	getWaitForExternalSetup(ConfigManager.getCurrentConfig());
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "waitForExternalSetup = " + waitForExternalSetup);

    // Get the name of the database from the configuration.
    String databaseName = dataSourceConfig.get("databaseName");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "databaseName = " + databaseName);

    // Check whether the Derby database is being used.
    if (dbManagerSql.isTypeDerby()) {
      // Yes: Check whether the Derby model to be used is the client/server
      // model.
      if (dataSource instanceof ClientDataSource) {
	// Yes: Check whether the Derby NetworkServerControl for client
	// connections needs to be started.
	if (shouldStartDerbyNetworkServerControl) {
	  // Yes: Start it.
	  ClientDataSource cds = (ClientDataSource)dataSource;
	  startDerbyNetworkServerControl(cds);

	  // Set up the Derby authentication configuration, if necessary.
	  setUpDerbyAuthentication(dataSourceConfig, cds);
	} else {
	  // No.
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER
	      + "Not starting a Derby Network Server Control");

	  // Wait for the ability to establish a connection to the database.
	  waitForConnection(databaseName);

	  // Wait until the database metadata is available.
	  waitForMetadata(databaseName);

	  // Wait until the database version table is available.
	  waitForDerbyVersionTable(databaseName);
	}
      } else {
	// No: Remove the Derby authentication configuration, if necessary.
	removeDerbyAuthentication(dataSourceConfig, dataSource);
      }

      // Remember that the Derby database has been booted.
      dbBooted = true;

      // No: Check whether the PostgreSQL database is being used.
    } else if (dbManagerSql.isTypePostgresql()) {
      // Yes.
      try {
	// Create the schema if it does not exist.
	dbManagerSql.createPostgresqlSchemaIfMissing(dataSourceSchemaName,
	    dataSource, waitForExternalSetup);
      } catch (SQLException sqle) {
	String msg = "Error creating PostgreSQL schema if missing";
	log.error(msg, sqle);
	log.error("databaseName = " + databaseName);
	throw new DbException(msg, sqle);
      } catch (RuntimeException re) {
	String msg = "Error creating PostgreSQL schema if missing";
	log.error(msg, re);
	log.error("databaseName = " + databaseName);
	throw new DbException(msg, re);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the datasource configuration.
   * 
   * @return a Configuration with the datasource configuration parameters.
   */
  private Configuration getDataSourceConfig() {
    final String DEBUG_HEADER = "getDataSourceConfig(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Get the current configuration.
    Configuration currentConfig = ConfigManager.getCurrentConfig();

    // Create the new datasource configuration.
    Configuration dsConfig = ConfigManager.newConfiguration();

    // Populate it from the current configuration datasource tree.
    dsConfig.copyFrom(currentConfig.getConfigTree(getDataSourceRootName()));

    // Save the default class name, if not configured.
    dsConfig.put("className", getDataSourceClassName(currentConfig));
    dataSourceClassName = dsConfig.get("className");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "dataSourceClassName = " + dataSourceClassName);

    dbManagerSql.setDataSourceClassName(dataSourceClassName);

    // Check whether the Derby database is being used.
    if (dbManagerSql.isTypeDerby()) {
      // Yes: Save the Derby creation directive, if not configured.
      dsConfig.put("createDatabase",
	  getDataSourceCreatedDatabase(currentConfig));
    }

    // Save the port number, if not configured.
    if (dbManagerSql.isTypeDerby()) {
      dsConfig.put("portNumber", getDataSourcePortNumber(currentConfig));
    } else if (dbManagerSql.isTypePostgresql()) {
      dsConfig.put("portNumber", getDataSourcePortNumber(currentConfig));
    } else if (dbManagerSql.isTypeMysql()) {
      dsConfig.put("port", getDataSourcePortNumber(currentConfig));
    }

    // Save the server name, if not configured.
    dsConfig.put("serverName", getDataSourceServerName(currentConfig));

    // Save the user name, if not configured.
    dsConfig.put("user", getDataSourceUser(currentConfig));
    dataSourceUser = dsConfig.get("user");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "dataSourceUser = " + dataSourceUser);

    dbManagerSql.setDataSourceUser(dataSourceUser);

    // Save the configured password.
    dataSourcePassword = getDataSourcePassword(currentConfig);
    //if (log.isDebug3())
      //log.debug3(DEBUG_HEADER + "dataSourcePassword = " + dataSourcePassword);

    dsConfig.put("password", dataSourcePassword);

    // Save the configured database name.
    dataSourceDbName = getDataSourceDatabaseName(currentConfig);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "dataSourceDbName = '" + dataSourceDbName + "'.");
    log.info(getVersionSubsystemName() + " using DB: " + dataSourceDbName);

    dsConfig.put("databaseName", dataSourceDbName);

    // Save the schema name, if not configured.
    if (dbManagerSql.isTypePostgresql()) {
      dataSourceSchemaName = getDataSourceSchemaName(currentConfig);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "dataSourceSchemaName = " + dataSourceSchemaName);

      dsConfig.put("schemaName", dataSourceSchemaName);
    }

    // Save the Derby credentials for later authentication.
    if (dbManagerSql.isTypeDerby()) {
      synchronized (this) {
	// Get the stored credentials for this database.
	DbCredentials dbCredentials = dbCredentialsMap.get(dataSourceDbName);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "dbCredentials = " + dbCredentials);

	// Check whether there are no stored credentials for this database.
	if (dbCredentials == null) {
	  // Yes: Create the credentials for this database.
	  dbCredentials = new DbCredentials(dataSourceUser, dataSourcePassword,
	      dataSourceClassName);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "dbCredentials = " + dbCredentials);

	  // Store the credentials for this database.
	  dbCredentialsMap.put(dataSourceDbName, dbCredentials);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "dbCredentialsMap = " + dbCredentialsMap);
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "dsConfig = " + dsConfig);
    return dsConfig;
  }

  protected String getDataSourceRootName() {
    return null;
  }

  protected String getDataSourceClassName(Configuration config) {
    return DEFAULT_DATASOURCE_CLASSNAME;
  }

  protected String getDataSourceCreatedDatabase(Configuration config) {
    return DEFAULT_DATASOURCE_CREATEDATABASE;
  }

  protected String getDataSourcePortNumber(Configuration config) {
    if (dbManagerSql.isTypePostgresql()) {
      return DEFAULT_DATASOURCE_PORTNUMBER_PG;
    } else if (dbManagerSql.isTypeMysql()) {
      return DEFAULT_DATASOURCE_PORTNUMBER_MYSQL;
    }

    return DEFAULT_DATASOURCE_PORTNUMBER;
  }

  protected String getDataSourceServerName(Configuration config) {
    return DEFAULT_DATASOURCE_SERVERNAME;
  }

  protected String getDataSourceUser(Configuration config) {
    return DEFAULT_DATASOURCE_USER;
  }

  protected String getDataSourcePassword(Configuration config) {
    return DEFAULT_DATASOURCE_PASSWORD;
  }

  /**
   * Provides the full name of the database to be used.
   * 
   * @param config
   *          A Configuration that includes the simple name of the database.
   * @return a String with the full name of the database.
   */
  protected abstract String getDataSourceDatabaseName(Configuration config);

  /** Return the prefix + simple class name, to be used as the base for the
   * database name.
   */
  protected String getSimpleDbName() {
    return getDatabaseNamePrefix() + this.getClass().getSimpleName();
  }

  /**
   * Provides the full name of the database to be used.
   * 
   * @param simpleDbName
   *          A String with the simple name of the database.
   * @return a String with the full name of the database.
   */
  protected String getFullDataSourceDatabaseName(String simpleDbName) {
    final String DEBUG_HEADER = "getDataSourceDatabaseName(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "simpleDbName = " + simpleDbName);

    // Check whether the Derby database is being used and a full path has not
    // been passed.
    if (dbManagerSql.isTypeDerby()) {
      if (simpleDbName.startsWith(File.separator)) {
	return simpleDbName;
      }
      if (StringUtil.isNullString(derbyDbBaseDir)) {
	// Param not set, resolve relative to configured cache dir:
	String pathFromCache = "db/" + simpleDbName;
	File datasourceDir = ConfigManager.getConfigManager()
	  .findRelDataDir(pathFromCache, false);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
				       + "datasourceDir = " + datasourceDir);

	// Return the data source root directory.
	return datasourceDir.getPath();
      }
      // basedir param set, resolve relative to it.
      return Paths.get(derbyDbBaseDir).resolve(simpleDbName).toString();
    }
    // Not Derby, the full name is just the simple name.
    return simpleDbName;
  }

  /**
   * Provides the name of the database schema to be used.
   * 
   * @param config
   *          A Configuration that includes the name of the database schema.
   * @return a String with the name of the database schema.
   */
  protected abstract String getDataSourceSchemaName(Configuration config);

  /**
   * Sets the Derby database properties.
   */
  private void setDerbyDatabaseConfiguration() {
    final String DEBUG_HEADER = "setDerbyDatabaseConfiguration(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Get the current configuration.
    Configuration currentConfig = ConfigManager.getCurrentConfig();

    // Save the default Derby log append option, if not configured.
    System.setProperty("derby.infolog.append",
	getDerbyInfoLogAppend(currentConfig));
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "derby.infolog.append = "
	  + System.getProperty("derby.infolog.append"));

    // Save the default Derby log query plan option, if not configured.
    System.setProperty("derby.language.logQueryPlan",
	getDerbyLanguageLogQueryPlan(currentConfig));
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "derby.language.logQueryPlan = "
	+ System.getProperty("derby.language.logQueryPlan"));

    // Save the default Derby log statement text option, if not configured.
    System.setProperty("derby.language.logStatementText",
	getDerbyLanguageLogStatementText(currentConfig));
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "derby.language.logStatementText = "
	+ System.getProperty("derby.language.logStatementText"));

    // Save the default Derby log file path, if not configured.
    System.setProperty("derby.stream.error.file",
	getDerbyStreamErrorFile(currentConfig));
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "derby.stream.error.file = "
	+ System.getProperty("derby.stream.error.file"));

    // Save the default Derby log severity level, if not configured.
    System.setProperty("derby.stream.error.logSeverityLevel",
	getDerbyStreamErrorLogSeverityLevel(currentConfig));
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "derby.stream.error.logSeverityLevel = "
	+ System.getProperty("derby.stream.error.logSeverityLevel"));

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  protected String getDerbyInfoLogAppend(Configuration config) {
    return DEFAULT_DERBY_INFOLOG_APPEND;
  }

  protected String getDerbyLanguageLogQueryPlan(Configuration config) {
    return DEFAULT_DERBY_LANGUAGE_LOGQUERYPLAN;
  }

  protected String getDerbyLanguageLogStatementText(Configuration config) {
    return DEFAULT_DERBY_LANGUAGE_LOGSTATEMENTTEXT;
  }

  protected String getDerbyStreamErrorFile(Configuration config) {
    return DEFAULT_DERBY_STREAM_ERROR_FILE;
  }

  protected String getDerbyStreamErrorLogSeverityLevel(Configuration config) {
    return DEFAULT_DERBY_STREAM_ERROR_LOGSEVERITYLEVEL;
  }

  protected abstract boolean getWaitForExternalSetup(Configuration config);

  /**
   * Initializes an external database, if it does not exist already.
   * 
   * @param dsConfig
   *          A Configuration with the datasource configuration.
   * @throws DbException
   *           if the database discovery or initialization processes failed.
   */
  private void initializeExternalDbIfNeeded(Configuration dsConfig)
      throws DbException {
    final String DEBUG_HEADER = "initializeExternalDbIfNeeded(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    boolean success = false;
    int retryCount = 0;
    long retryDelayHere = 10 * retryDelay;

    // Keep trying until success.
    while (!success) {
      try {
	// Check whether the PostgreSQL database is being used.
	if (dbManagerSql.isTypePostgresql()) {
	  // Yes: Initialize the database, if necessary.
	  initializePostgresqlDbIfNeeded(dataSourceConfig);
	  success = true;

	  // No: Check whether the MySQL database is being used.
	} else if (dbManagerSql.isTypeMysql()) {
	  // Yes: Initialize the database, if necessary.
	  initializeMysqlDbIfNeeded(dataSourceConfig);
	  success = true;
	}
      } catch (DbException dbe) {
	// The remote database server is not available: Count the next retry.
	retryCount++;

	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Next retry is " + retryCount + " of " + maxRetryCount);

	// Check whether the next retry would go beyond the specified maximum
	// number of retries.
	if (retryCount > maxRetryCount) {
	  // Yes: Report the failure.
	  log.error("Exception caught", dbe);
	  log.error("Maximum retry count of " + maxRetryCount + " reached.");
	  throw dbe;
	} else {
	  // No: Wait for the specified amount of time before attempting the
	  // next retry.
	  log.debug(DEBUG_HEADER + "Exception caught", dbe);
	  log.debug(DEBUG_HEADER + "Waiting "
	      + TimeUtil.timeIntervalToString(retryDelayHere)
	      + " before retry number " + retryCount + "...");

	  try {
	    Deadline.in(retryDelayHere).sleep();
	  } catch (InterruptedException ie) {
	    // Continue with the next retry.
	  }
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Initializes a PostreSQl database, if it does not exist already.
   * 
   * @param dsConfig
   *          A Configuration with the datasource configuration.
   * @throws DbException
   *           if the database discovery or initialization processes failed.
   */
  private void initializePostgresqlDbIfNeeded(Configuration dsConfig)
      throws DbException {
    final String DEBUG_HEADER = "initializePostgresqlDbIfNeeded(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Create a datasource.
    DataSource ds = createDataSource(dsConfig.get("className"));

    // Initialize the datasource properties.
    initializeDataSourceProperties(dsConfig, ds);

    // Get the configured database name.
    String databaseName = dsConfig.get("databaseName");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "databaseName = " + databaseName);

    // Replace the database name with the standard connectable template.
    try {
      BeanUtils.setProperty(ds, "databaseName", "template1");
    } catch (Throwable t) {
      throw new DbException("Could not initialize the datasource", t);
    }

    // Create the database if it does not exist.
    try {
      dbManagerSql.createPostgreSqlDbIfMissing(ds, databaseName,
	  getWaitForExternalSetup(ConfigManager.getCurrentConfig()));
    } catch (SQLException sqle) {
      String message = "Error creating PostgreSQL database '" + databaseName
	  + "' if missing";
      log.error(message, sqle);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Error creating PostgreSQL database '" + databaseName
	  + "' if missing";
      log.error(message, re);
      throw new DbException(message, re);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Initializes the properties of the datasource using the specified
   * configuration.
   * 
   * @param dsConfig
   *          A Configuration with the datasource configuration.
   * @param ds
   *          A DataSource with the datasource that provides the connection.
   * @throws DbException
   *           if the datasource properties could not be initialized.
   */
  private void initializeDataSourceProperties(Configuration dsConfig,
      DataSource ds) throws DbException {
    final String DEBUG_HEADER = "initializeDataSourceProperties(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    String dsClassName = dsConfig.get("className");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "dsClassName = '" + dsClassName + "'.");

    // Loop through all the configured datasource property names.
    for (String key : dsConfig.keySet()) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "key = '" + key + "'.");

      // Check whether it is an applicable datasource property.
      if (isApplicableDataSourceProperty(key)) {
	// Yes: Get the value of the property.
	String value = dsConfig.get(key);
	if (log.isDebug3() && !"password".equals(key))
	  log.debug3(DEBUG_HEADER + "value = '" + value + "'.");

	// Set the property value in the datasource.
	try {
	  BeanUtils.setProperty(ds, key, value);
	} catch (Throwable t) {
	  throw new DbException("Cannot set value '" + value
	      + "' for property '" + key
	      + "' for instance of datasource class '" + dsClassName, t);
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides an indication of whether a property is applicable to a datasource.
   * 
   * @param name
   *          A String with the name of the property.
   * @return <code>true</code> if the named property is applicable to a
   *         datasource, <code>false</code> otherwise.
   */
  private boolean isApplicableDataSourceProperty(String name) {
    final String DEBUG_HEADER = "isApplicableDataSourceProperty(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "name = '" + name + "'.");

    // Handle the names of properties that are always applicable.
    if ("serverName".equals(name)
	|| "dataSourceName".equals(name) || "databaseName".equals(name)
	|| "user".equals(name)
	|| "description".equals(name)) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true.");
      return true;
    }

    // Handle the names of properties applicable to the Derby database being
    // used.
    if (dbManagerSql.isTypeDerby()
	&& ("createDatabase".equals(name) || "shutdownDatabase".equals(name)
	    || "portNumber".equals(name) || "password".equals(name))) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true.");
      return true;

      // Handle the names of properties applicable to the PostgreSQL database
      // being used.
    } else if (dbManagerSql.isTypePostgresql()
	&& ("initialConnections".equals(name) || "maxConnections".equals(name)
	    || "portNumber".equals(name) || "password".equals(name)
	    || "schemaName".equals(name))) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true.");
      return true;

      // Handle the names of properties applicable to the MySQL database being
      // used.
    } else if (dbManagerSql.isTypeMysql() && "port".equals(name)) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = true.");
      return true;
    }

    // Any other named property is not applicable.
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = false.");
    return false;
  }

  /**
   * Initializes a MySQl database, if it does not exist already.
   * 
   * @param dsConfig
   *          A Configuration with the datasource configuration.
   * @throws DbException
   *           if the database discovery or initialization processes failed.
   */
  private void initializeMysqlDbIfNeeded(Configuration dsConfig)
      throws DbException {
    final String DEBUG_HEADER = "initializeMysqlDbIfNeeded(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Create a datasource.
    DataSource ds = createDataSource(dsConfig.get("className"));

    // Initialize the datasource properties.
    initializeDataSourceProperties(dsConfig, ds);

    // Get the configured database name.
    String databaseName = dsConfig.get("databaseName");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "databaseName = " + databaseName);

    // Replace the database name with the standard connectable mysql database.
    try {
      BeanUtils.setProperty(ds, "databaseName", "information_schema");
    } catch (Throwable t) {
      throw new DbException("Could not initialize the datasource", t);
    }

    // Create the database if it does not exist.
    try {
      dbManagerSql.createMySqlDbIfMissing(ds, databaseName,
	  getWaitForExternalSetup(ConfigManager.getCurrentConfig()));
    } catch (SQLException sqle) {
      String message = "Error creating MySQL database '" + databaseName
	  + "' if missing";
      log.error(message, sqle);
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Error creating MySQL database '" + databaseName
	  + "' if missing";
      log.error(message, re);
      throw new DbException(message, re);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Starts the Derby NetworkServerControl and waits for it to be ready.
   * 
   * @param cds
   *          A ClientDataSource with the client datasource that provides the
   *          connection.
   * @throws DbException
   *           if the network server control could not be started.
   */
  private void startDerbyNetworkServerControl(ClientDataSource cds)
      throws DbException {
    final String DEBUG_HEADER = "startDerbyNetworkServerControl(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Get the configured server name.
    String serverName = cds.getServerName();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "serverName = '" + serverName + "'.");

    // Determine the IP address of the server.
    InetAddress inetAddr;
    
    try {
      inetAddr = InetAddress.getByName(serverName);
    } catch (UnknownHostException uhe) {
      throw new DbException("Cannot determine the IP address of server '"
	  + serverName + "'", uhe);
    }

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "inetAddr = " + inetAddr + ".");

    // Get the configured server port number.
    int serverPort = cds.getPortNumber();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "serverPort = " + serverPort + ".");

    // Create the network server control.
    try {
      networkServerControl = new NetworkServerControl(inetAddr, serverPort);
    } catch (Exception e) {
      throw new DbException("Cannot create the Network Server Control", e);
    }

    // Start the network server control.
    try {
      networkServerControl.start(null);
    } catch (Exception e) {
      throw new DbException("Cannot start the Network Server Control", e);
    }

    // Wait for the network server control to be ready.
    for (int i = 0; i < 40; i++) { // At most 20 seconds.
      try {
	networkServerControl.ping();
	log.debug(DEBUG_HEADER + "Remote access to Derby database enabled");
	return;
      } catch (Exception e) {
	// Control is not ready: wait and try again.
	try {
	  Deadline.in(500).sleep(); // About 1/2 second.
	} catch (InterruptedException ie) {
	  break;
	}
      }
    }

    // At this point we give up.
    throw new DbException("Cannot enable remote access to Derby database");
  }

  /**
   * Turns on user authentication and authorization on a Derby database.
   * 
   * @param dsConfig
   *          A Configuration with the datasource configuration.
   * @param cds
   *          A ClientDataSource with the client datasource that provides the
   *          connection.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void setUpDerbyAuthentication(Configuration dsConfig,
      ClientDataSource cds) throws DbException {
    final String DEBUG_HEADER = "setUpDerbyAuthentication(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Turn on user authentication and authorization on the Derby database and
    // get an indication of whether the database needs to be shut down to make
    // static properties take effect.
    boolean requiresCommit = false;
    String user = null;

    try {
      user = dsConfig.get("user");
      requiresCommit = dbManagerSql.setUpDerbyAuthentication(user);
    } catch (SQLException sqle) {
      throw new DbException("Cannot set up Derby authentication for user '" +
	  user + "'", sqle);
    } catch (RuntimeException re) {
      throw new DbException("Cannot set up Derby authentication for user '" +
	  user + "'", re);
    }

    // Check whether the database needs to be shut down to make static
    // properties take effect.
    if (requiresCommit) {
      // Yes: Shut down the database.
      shutdownDerbyDb(dsConfig);

      // Initialize the datasource properties.
      initializeDataSourceProperties(dsConfig, cds);

      // Restart the network server control.
      startDerbyNetworkServerControl(cds);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Turns off user authentication and authorization on a Derby database.
   * 
   * @param dsConfig
   *          A Configuration with the datasource configuration.
   * @param ds
   *          A DataSource with the client datasource that provides the
   *          connection.
   * @throws DbException
   *           if any problem occurred accessing the database.
   */
  private void removeDerbyAuthentication(Configuration dsConfig, DataSource ds)
      throws DbException {
    final String DEBUG_HEADER = "removeDerbyAuthentication(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Turn off user authentication and authorization on the Derby database and
    // check whether the database needs to be shut down to make static
    // properties take effect.
    try {
      if (dbManagerSql.removeDerbyAuthentication()) {
	// Yes: Shut down the database.
	shutdownDerbyDb(dsConfig);

	// Initialize the datasource properties.
	initializeDataSourceProperties(dsConfig, ds);
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot remove Derby authentication", sqle);
    } catch (RuntimeException re) {
      throw new DbException("Cannot remove Derby authentication", re);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Shuts down the Derby database.
   * 
   * @throws SQLException
   *           if there are problems shutting down the database.
   */
  private void shutdownDerbyDb(Configuration dsConfig) throws DbException {
    final String DEBUG_HEADER = "shutdownDerbyDb(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Modify the datasource configuration for shutdown.
    Configuration shutdownConfig = dsConfig.copy();
    shutdownConfig.remove("createDatabase");
    shutdownConfig.put("shutdownDatabase", "shutdown");

    // Create the shutdown datasource.
    DataSource ds = createDataSource(shutdownConfig.get("className"));

    // Initialize the shutdown datasource properties.
    initializeDataSourceProperties(shutdownConfig, ds);

    // Get a connection, which will shutdown the Derby database.
    try {
      dbManagerSql.getConnection(ds, false);
    } catch (SQLException sqle) {
      // Check whether it is the expected exception.
      if (SHUTDOWN_SUCCESS_STATE_CODE.equals(sqle.getSQLState())) {
	// Yes.
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "Expected exception caught", sqle);
      } else {
	// No: Report the problem.
	log.error("Unexpected exception caught shutting down database", sqle);
      }
    } catch (RuntimeException re) {
      // Report the problem.
      log.error("Unexpected exception caught shutting down database", re);
    }

    if (log.isDebug())
      log.debug(DEBUG_HEADER + "Derby database has been shutdown.");
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits until a database accepts a connection.
   * 
   * @param databaseName
   *          A String with the name of the database.
   */
  private void waitForConnection(String databaseName) {
    final String DEBUG_HEADER = "waitForConnection(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "databaseName = " + databaseName);

    Connection conn = null;

    // Wait until a connection to the database is acquired.
    while (conn == null) {
      try {
	conn = JdbcBridge.getConnection(dataSource, maxRetryCount, retryDelay,
	    false);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Database '" + 
	    databaseName + "' accepted a connection.");
      } catch (SQLException sqle) {
	if (log.isDebug()) log.debug(DEBUG_HEADER + "Database '"
	    + databaseName + "' refuses connections. "
	    + waitForExternalSetupMessage);

	try {
	  Thread.sleep(waitForExternalSetupInterval);
	} catch (InterruptedException ie)
	{
	  // Expected.
	}
      } finally {
	// Roll back the connection.
	try {
	  conn.rollback();
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "Connection rolled back.");
	} catch (SQLException sqle) {
	  // Ignore.
	} catch (RuntimeException re) {
	  // Ignore.
	}

	// Close the connection.
	try {
	  if ((conn != null) && !conn.isClosed()) {
	    conn.close();
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Connection closed.");
	  }
	} catch (SQLException sqle) {
	  // Ignore.
	} catch (RuntimeException re) {
	  // Ignore.
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits until the metadata of a database is available.
   * 
   * @param databaseName
   *          A String with the name of the database.
   */
  private void waitForMetadata(String databaseName) {
    final String DEBUG_HEADER = "waitForMetadata(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "databaseName = " + databaseName);

    DatabaseMetaData metadata = null;

    // Wait until the database metadata is acquired.
    while (metadata == null) {
      Connection conn = null;

      try {
	// Get a connection to the database.
	conn = JdbcBridge.getConnection(dataSource, maxRetryCount, retryDelay,
	    false);

	// Get the database metadata.
	metadata = JdbcBridge.getMetadata(conn);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Database '" + 
	    databaseName + "' metadata is available.");
      } catch (SQLException sqle) {
	if (log.isDebug()) log.debug(DEBUG_HEADER + "Database '"
	    + databaseName + "' metadata is not available. "
	    + waitForExternalSetupMessage);

	try {
	  Thread.sleep(waitForExternalSetupInterval);
	} catch (InterruptedException ie)
	{
	  // Expected.
	}
      } finally {
	// Roll back the connection.
	try {
	  conn.rollback();
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "Connection rolled back.");
	} catch (SQLException sqle) {
	  // Ignore.
	} catch (RuntimeException re) {
	  // Ignore.
	}

	// Close the connection.
	try {
	  if ((conn != null) && !conn.isClosed()) {
	    conn.close();
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Connection closed.");
	  }
	} catch (SQLException sqle) {
	  // Ignore.
	} catch (RuntimeException re) {
	  // Ignore.
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits until the version table of a Derby database is available.
   * 
   * @param databaseName
   *          A String with the name of the database.
   */
  private void waitForDerbyVersionTable(String databaseName) {
    final String DEBUG_HEADER = "waitForDerbyVersionTable(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "databaseName = " + databaseName);

    ResultSet rs = null;

    // Wait until the version table is available.
    while (rs == null) {
      Connection conn = null;

      try {
	// Get a connection to the database.
	conn = JdbcBridge.getConnection(dataSource, maxRetryCount, retryDelay,
	    false);

	// Get the version table metadata.
	rs = JdbcBridge.getStandardTables(conn, null, dataSourceUser,
	    VERSION_TABLE.toUpperCase());
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Database "
				       + getSimpleDbName() + " table '" +
				       VERSION_TABLE + "' is available.");
      } catch (SQLException sqle) {
	if (log.isDebug()) log.debug(DEBUG_HEADER + "Database "
				     + getSimpleDbName() + " table '"
				     + VERSION_TABLE + "' is not available. "
				     + waitForExternalSetupMessage);

	try {
	  Thread.sleep(waitForExternalSetupInterval);
	} catch (InterruptedException ie)
	{
	  // Expected.
	}
      } finally {
	// Roll back the connection.
	try {
	  conn.rollback();
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "Connection rolled back.");
	} catch (SQLException sqle) {
	  // Ignore.
	} catch (RuntimeException re) {
	  // Ignore.
	}

	// Close the connection.
	try {
	  if ((conn != null) && !conn.isClosed()) {
	    conn.close();
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Connection closed.");
	  }
	} catch (SQLException sqle) {
	  // Ignore.
	} catch (RuntimeException re) {
	  // Ignore.
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Updates the database to a given version, if necessary.
   * 
   * @param targetDbVersion
   *          An int with the database version that is the target of the update.
   * 
   * @throws DbException
   *           if any problem occurred updating the database.
   */
  private void updateDatabaseIfNeeded(int targetDbVersion) throws DbException {
    final String DEBUG_HEADER = "updateDatabaseIfNeeded(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "targetDbVersion = " + targetDbVersion);

    // Get the indication of whether the startup code should wait for the
    // external setup of the database.
    boolean waitForExternalSetup =
	getWaitForExternalSetup(ConfigManager.getCurrentConfig());
    if (log.isDebug3()) log.debug3(DEBUG_HEADER
	+ "waitForExternalSetup = " + waitForExternalSetup);

    Connection conn = null;

    try {
      conn = dbManagerSql.getConnection();

      // Check whether the version table does not exist.
      if (!dbManagerSql.tableExists(conn, VERSION_TABLE)) {
	// Yes: Check whether it should wait for the table to be created
	// externally.
	if (waitForExternalSetup) {
	  // Yes: Wait until the table is created externally.
	  waitForVersionTable(conn);
	} else {
	  // No.
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + VERSION_TABLE + " table does not exist.");
	  // Create the table (without the subsystem column).
	  dbManagerSql.createVersionTable(conn);

	  // Add the subsystem column.
	  dbManagerSql.addVersionSubsystemColumn(conn);

	  // Record the DbManager subsystem version in the database.
	  int count = recordDbVersion(conn, DB_VERSION_SUBSYSTEM, 1);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
	}
      } else {
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + VERSION_TABLE + " table exists.");
      }

      // Check whether the subsystem column of the version table does not exist.
      if (!dbManagerSql.columnExists(conn, VERSION_TABLE, SUBSYSTEM_COLUMN)) {
	// Yes: Check whether it should wait for the column to be created
	// externally.
	if (waitForExternalSetup) {
	  // Yes: Wait until the column is created externally.
	  waitForVersionSubsystemColumn(conn);
	} else {
	  // No.
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + SUBSYSTEM_COLUMN
	    + " column in " + VERSION_TABLE + " table does not exist.");

	  // Add the subsystem column.
	  dbManagerSql.addVersionSubsystemColumn(conn);

	  // Record the DbManager subsystem version in the database.
	  int count = recordDbVersion(conn, DB_VERSION_SUBSYSTEM, 1);
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
	}
      } else {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + SUBSYSTEM_COLUMN
	    + " column in " + VERSION_TABLE + " table exists.");
      }

      String subsystem = getVersionSubsystemName();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "subsystem = " + subsystem);

      // Find the current database version.
      int existingDbVersion = dbManagerSql.getHighestNumberedDatabaseVersion(
	  conn, subsystem);

      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "existingDbVersion = "
	  + existingDbVersion);

      // Check whether the existing database is newer than what this version of
      // the daemon expects.
      if (targetDbVersion < existingDbVersion) {
	// Yes: Disable the use of the database and report the problem.
	throw new DbException("Existing database is version "
	    + existingDbVersion
	    + ", which is higher than the target database version "
	    + targetDbVersion
	    + " for this daemon. Possibly caused by daemon downgrade.");
      }

      // Check whether it should wait for the database to be updated externally.
      if (waitForExternalSetup) {
	// Yes: Wait for the database to be updated externally.
	existingDbVersion = waitForDatabaseUpdate(conn, subsystem,
	    existingDbVersion, targetDbVersion);
      }

      // Check whether the database is not updated externally and any previously
      // started threaded database updates need to be checked for completion.
      if (!waitForExternalSetup && existingDbVersion >= 2) {
	// Yes: Get all the version updates recorded in the database.
	List<Integer> recordedVersions =
	    dbManagerSql.getSortedDatabaseVersions(conn, subsystem);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "recordedVersions = " + recordedVersions);

	// Check whether all the previous database updates need to be recorded
	// in the database (The original update method only recorded the version
	// of the highest-numbered update performed.
	if (recordedVersions.size() == 1) {
	  // Yes: Loop through all the versions to be recorded.
	  for (int version = 2; version < existingDbVersion; version++) {
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "Recording version " + version + "...");

	    // Record the version in the database.
	    int count = recordDbVersion(conn, subsystem, version);
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
	  }
	} else {
	  // No: Perform any unfinished updates.
	  performUnfinishedUpdates(conn, recordedVersions);
	}
      }

      // Check whether the database needs to be updated beyond the existing
      // database version.
      if (targetDbVersion > existingDbVersion) {
	// Yes.
	if (log.isDebug3()) {
	  log.debug3(DEBUG_HEADER + "Database " + getSimpleDbName()
		     + " needs to be updated from existing version " + existingDbVersion
		     + " to new version " + targetDbVersion);
	}
	// Update the database.
	int lastRecordedVersion =
	    updateDatabase(conn, existingDbVersion, targetDbVersion);
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "lastRecordedVersion = "
		+ lastRecordedVersion);

	List<String> pendingUpdates = getPendingUpdates();
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pendingUpdates = '"
		+ pendingUpdates + "'");

	if (pendingUpdates.size() > 0) {
	  if (lastRecordedVersion > existingDbVersion) {
	    log.info("Database " + getSimpleDbName()
		     + " has been updated to version " + lastRecordedVersion
		     + ". Pending updates: " + pendingUpdates);
	  } else {
	    log.info("Database " + getSimpleDbName()
		     + " remains at version " + lastRecordedVersion
		     + ". Pending updates: " + pendingUpdates);
	  }
	} else {
	  if (lastRecordedVersion > existingDbVersion) {
	    log.info("Database " + getSimpleDbName()
		     + " has been updated to version " + lastRecordedVersion);
	  } else {
	    log.info("Database " + getSimpleDbName()
		     + " remains at version " + lastRecordedVersion);
	  }
	}
      } else {
	// No: Nothing more to do.
	List<String> pendingUpdates = getPendingUpdates();
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "pendingUpdates = '"
		+ pendingUpdates + "'");

	if (pendingUpdates.size() > 0) {
	  log.info("Database " + getSimpleDbName()
		   + " is up-to-date at version " + existingDbVersion
		   + ". Pending updates: " + pendingUpdates);
	} else {
	  log.info("Database " + getSimpleDbName()
		   + " is up-to-date at version " + existingDbVersion);
	}
      }
    } catch (SQLException sqle) {
      throw new DbException("Cannot update the database to target version "
	  + targetDbVersion, sqle);
    } catch (RuntimeException re) {
      throw new DbException("Cannot update the database to target version "
	  + targetDbVersion, re);
    } finally {
      DbManagerSql.safeRollbackAndClose(conn);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits until the version table of a database exists.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  private void waitForVersionTable(Connection conn) throws SQLException {
    final String DEBUG_HEADER = "waitForVersionTable(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Wait until the database version table exists.
    while (!dbManagerSql.tableExists(conn, VERSION_TABLE)) {
      if (log.isDebug()) log.debug(DEBUG_HEADER + "Table '" + VERSION_TABLE
	  + "' does not exist. " + waitForExternalSetupMessage);

      try {
	Thread.sleep(waitForExternalSetupInterval);
      } catch (InterruptedException ie)
      {
	// Expected.
      }
    }

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + VERSION_TABLE + " table exists.");

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits until the subsystem column of the version table exists.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  private void waitForVersionSubsystemColumn(Connection conn)
      throws SQLException {
    final String DEBUG_HEADER = "waitForVersionSubsystemColumn(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Starting...");

    // Wait until the subsystem column of the version table exists.
    while (!dbManagerSql.columnExists(conn, VERSION_TABLE, SUBSYSTEM_COLUMN)) {
      if (log.isDebug()) log.debug(DEBUG_HEADER + SUBSYSTEM_COLUMN
	  + " column in " + VERSION_TABLE + " table does not exist. "
	  + waitForExternalSetupMessage);

      try {
	Thread.sleep(waitForExternalSetupInterval);
      } catch (InterruptedException ie)
      {
	// Expected.
      }
    }

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + SUBSYSTEM_COLUMN
	+ " column in " + VERSION_TABLE + " table exists.");

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits until the database has been updated to the target version.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param subsystem
   *          A String with the subsystem name to e used in the version table.
   * @param currentDbVersion
   *          An int with the existing database version.
   * @param targetDbVersion
   *          An int with the database version that is the target of the update.
   * @throws SQLException
   *           if any problem occurred updating the database.
   */
  private int waitForDatabaseUpdate(Connection conn, String subsystem,
      int currentDbVersion, int targetDbVersion) throws SQLException {
    final String DEBUG_HEADER = "waitForDatabaseUpdate(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "subsystem = " + subsystem);
      log.debug2(DEBUG_HEADER + "currentDbVersion = " + currentDbVersion);
      log.debug2(DEBUG_HEADER + "targetDbVersion = " + targetDbVersion);
    }

    // Wait while the current version is smaller than the target version.
    while (targetDbVersion > currentDbVersion) {
      if (log.isDebug()) log.debug(DEBUG_HEADER + "targetDbVersion = "
	  + targetDbVersion + " > currentDbVersion = " + currentDbVersion + ". "
	  + waitForExternalSetupMessage);

      try {
	Thread.sleep(waitForExternalSetupInterval);
      } catch (InterruptedException ie)
      {
	// Expected.
      }

      // Get the current version.
      currentDbVersion =
	  dbManagerSql.getHighestNumberedDatabaseVersion(conn, subsystem);
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "currentDbVersion = " + currentDbVersion);
    return currentDbVersion;
  }

  /**
   * Performs database updates that have not been recorded as finished.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param recordedVersions
   *          A List<Integer> with the identifiers of the recorded database
   *          updates.
   * @throws DbException
   *           if any problem occurred updating the database.
   */
  private void performUnfinishedUpdates(Connection conn,
      List<Integer> recordedVersions) throws DbException {
    final String DEBUG_HEADER = "performUnfinishedUpdates(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "recordedVersions = " + recordedVersions);

    // The first database update that should be recorded is number 2, because
    // the version table did not exist before.
    int previousVersion = 1;

    // Loop through all the recorded versions.
    for (int recordedVersion : recordedVersions) {
      if (log.isDebug3()) {
	log.debug3(DEBUG_HEADER + "recordedVersion = " + recordedVersion);
	log.debug3(DEBUG_HEADER + "previousVersion = " + previousVersion);
      }

      // Check whether there is a version skipped in the record of database
      // updates.
      if (recordedVersion - previousVersion > 1) {
	// Yes.
	if (log.isDebug3()) log.debug3(DEBUG_HEADER
	    + "Skipped recorded versions between " + (previousVersion + 1)
	    + " and " + (recordedVersion - 1) + " both inclusive.");

	// Loop through all the skipped versions.
	for (int version = previousVersion; version < recordedVersion - 1;
	    version++) {
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER+ "Updating database from version " + version
		+ " to version " + (version + 1));

	  // Update the database from version dbVersion to version
	  // dbVersion + 1.
	  updateDatabase(conn, version, version + 1);
	  if (log.isDebug3())
	    log.debug3("Database " + getSimpleDbName()
		       + " has been updated to version " + (version + 1));
	}
      }

      previousVersion = recordedVersion;
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Updates the database to the target version.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param existingDatabaseVersion
   *          An int with the existing database version.
   * @param finalDatabaseVersion
   *          An int with the version of the database to which the database is
   *          to be updated.
   * @return an int with the highest update version recorded in the database.
   * @throws DbException
   *           if any problem occurred updating the database.
   */
  protected int updateDatabase(Connection conn, int existingDatabaseVersion,
      int finalDatabaseVersion) throws DbException {
    final String DEBUG_HEADER = "updateDatabase(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "existingDatabaseVersion = "
	  + existingDatabaseVersion);
      log.debug2(DEBUG_HEADER + "finalDatabaseVersion = "
	  + finalDatabaseVersion);
    }

    String subsystem = getVersionSubsystemName();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "subsystem = " + subsystem);

    int lastRecordedVersion = existingDatabaseVersion;

    // Loop through all the versions to be updated to reach the targeted
    // version.
    for (int from = existingDatabaseVersion; from < finalDatabaseVersion;
	from++) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Updating from version " + from + "...");

      // Assume failure.
      boolean success = false;

      try {
	// Perform the appropriate update for this version.
	updateDatabaseToVersion(conn, from + 1);

	success = true;
      } catch (SQLException sqle) {
	throw new DbException("Error updating database from version " + from,
	    sqle);
      } catch (RuntimeException re) {
	throw new DbException("Error updating database from version " + from,
	    re);
      } finally {
	// Check whether the partial update was successful.
	if (success) {
	  // Yes: Check whether the updated database is at least at version 2.
	  if (from > 0) {
	    // Yes: Check whether this update will not be recorded in the
	    // database by an asynchronous process when it finishes. 
	    if (skipAsynchronousUpdates
		|| Arrays.binarySearch(asynchronousUpdates, from + 1) < 0) {
	      // Yes: Record the current database version in the database.
	      lastRecordedVersion = from + 1;
	      recordDbVersion(conn, subsystem, lastRecordedVersion);
	      if (log.isDebug())
		log.debug("Database " + getSimpleDbName()
			  + " updated to version " + lastRecordedVersion);
	    }
	  } else {
	    // No: Record the current database version in the database.
	    lastRecordedVersion = from + 1;
	    recordDbVersion(conn, subsystem, lastRecordedVersion);
	    if (log.isDebug())
	      log.debug("Database " + getSimpleDbName()
			+ " updated to version " + lastRecordedVersion);

	    // Commit this partial update.
	    try {
	      DbManagerSql.commitOrRollback(conn, log);
	    } catch (SQLException sqle) {
	      String message = "Cannot commit the connection";
	      log.error(message, sqle);
	      DbManagerSql.safeRollbackAndClose(conn);
	      throw new DbException(message, sqle);
	    } catch (RuntimeException re) {
	      String message = "Cannot commit the connection";
	      log.error(message, re);
	      DbManagerSql.safeRollbackAndClose(conn);
	      throw new DbException(message, re);
	    }
	  }
	} else {
	  // No: Do not continue with subsequent updates.
	  break;
	}
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "lastRecordedVersion = "
	+ lastRecordedVersion);
    return lastRecordedVersion;
  }

  protected void updateDatabaseToVersion(Connection conn, int databaseVersion)
      throws SQLException {
  }

  /**
   * Records in the database a database subsystem version.
   * 
   * @param conn
   *          A Connection with the database connection to be used.
   * @param subsystem
   *          A String with database subsystem name.
   * @param version
   *          An int with version to be recorded.
   * @return an int with the number of database rows recorded.
   * @throws SQLException
   *           if any problem occurred recording the database version.
   */
  private int recordDbVersion(Connection conn, String subsystem, int version)
      throws DbException {
    final String DEBUG_HEADER = "recordDbVersion(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "subsystem = " + subsystem);
      log.debug2(DEBUG_HEADER + "version = " + version);
    }

    int count = -1;

    try {
      count = dbManagerSql.addDbVersion(conn, subsystem, version);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "count = " + count);
      DbManagerSql.commitOrRollback(conn, log);
    } catch (SQLException sqle) {
      String message = "Cannot record updated database subsystem " + subsystem
	  + " version " + version;
      log.error(message);
      try {
	DbManagerSql.rollback(conn, log);
      } catch (SQLException sqle2) {
	String message2 = "Cannot roll back the connection";
	log.error(message2, sqle2);
	DbManagerSql.safeRollbackAndClose(conn);
	throw new DbException(message2, sqle2);
      } catch (RuntimeException re) {
	String message2 = "Cannot roll back the connection";
	log.error(message2, re);
	DbManagerSql.safeRollbackAndClose(conn);
	throw new DbException(message2, re);
      }
      throw new DbException(message, sqle);
    } catch (RuntimeException re) {
      String message = "Cannot record updated database subsystem " + subsystem
	  + " version " + version;
      log.error(message);
      try {
	DbManagerSql.rollback(conn, log);
      } catch (SQLException sqle) {
	String message2 = "Cannot roll back the connection";
	log.error(message2, sqle);
	DbManagerSql.safeRollbackAndClose(conn);
	throw new DbException(message2, sqle);
      } catch (RuntimeException re2) {
	String message2 = "Cannot roll back the connection";
	log.error(message2, re2);
	DbManagerSql.safeRollbackAndClose(conn);
	throw new DbException(message2, re2);
      }
      throw new DbException(message, re);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "count = " + count);
    return count;
  }

  /**
   * Records the existence of a spawned thread. Useful to avoid ugly but
   * harmless exceptions when running tests.
   * 
   * @param thread A Thread with the thread to be recorded.
   */
  public synchronized void recordThread(Thread thread) {
    final String DEBUG_HEADER = "recordThread(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "thread = '" + thread + "'");

    threads.add(thread);

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Removes the record of a spawned thread. Useful to avoid ugly but harmless
   * exceptions when running tests.
   * 
   * @param name A String with the name to be cleaned up.
   */
  public synchronized void cleanUpThread(String name) {
    final String DEBUG_HEADER = "cleanUpThread(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "name = '" + name + "'");
    Thread namedThread = null;

    for (Thread thread : threads) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "thread = '" + thread + "'");

      if (name.equals(thread.getName())) {
	namedThread = thread;
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "namedThread = '" + namedThread + "'");
	break;
      }
    }

    if (namedThread != null) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Removing namedThread = '"
	  + namedThread + "'...");
      threads.remove(namedThread);
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Done.");
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Waits for all recorded threads to finish. Useful to avoid ugly but harmless
   * exceptions when running tests.
   * 
   * @param timeout A long with the number of millisecons to wait at most for
   * threads to die.
   */
  public synchronized void waitForThreadsToFinish(long timeout) {
    final String DEBUG_HEADER = "waitForThreadsToFinish(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "timeout = " + timeout);

    for (Thread thread : threads) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Waiting for thread = '" + thread + "'...");
      if (thread.isAlive()) {
	try {
	  thread.join(timeout);
	} catch (InterruptedException ie) {
	  // Do Nothing.
	}
      }

      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Done.");
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the numbers of the database updates that are still pending.
   * 
   * @return a List<String> with the database updates that are still pending.
   */
  private synchronized List<String> getPendingUpdates() {
    final String DEBUG_HEADER = "getPendingUpdates(): ";
    List<String> result = new ArrayList<String>();

    for (Thread thread : threads) {
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "thread = '" + thread + "'");

      String name = thread.getName();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "name = '" + name + "'");
      String from = name.substring(9, name.indexOf("To"));
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "from = '" + from + "'");
      String to =
	  name.substring(name.indexOf("To") + 2, name.indexOf("Migrator"));
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "to = '" + to + "'");
      result.add(from + " -> " + to);
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the credentials map used to verify authentication when accessing a
   * Derby database that requires it.
   * 
   * @return a Map<String, DbCredentials> with the credentials map.
   */
  static Map<String, DbCredentials> getDbCredentialsMap() {
    return dbCredentialsMap;
  }

  /**
   * Provides the subsystem name to be used in the version table.
   * 
   * @return A String with the subsystem name to be used in the version table.
   */
  protected String getVersionSubsystemName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Provides the prefix for database names.
   * 
   * @return A String with the prefix for database names.
   */
  public String getDatabaseNamePrefix() {
    return databaseNamePrefix;
  }
}
