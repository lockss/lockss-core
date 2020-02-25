/*

 Copyright (c) 2018-2020 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.metadata;

import static org.lockss.metadata.SqlConstants.*;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.Test;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.db.DbManagerSql;
import org.lockss.db.SqlConstants;
import org.lockss.test.*;
import org.lockss.util.Logger;

/**
 * Test class for org.lockss.metadata.MetadataDbManager.
 */
public class TestMetadataDbManager extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();
  private static String TABLE_CREATE_SQL =
      "create table testtable (id bigint NOT NULL, name varchar(512))";

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private MetadataDbManager metadataDbManager;
  private String dbPort;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Get the temporary directory used during the test.
    tempDirPath = setUpDiskSpace();

    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);
    dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_PORTNUMBER,
				  dbPort);
  }

  /**
   * Tests PostgreSQL database creation with defaults.
   */
  @Test
  public void testCreatePgsqlDb1() {
    createPgsqlDb();

    assertEquals("LockssMetadataDbManager", metadataDbManager
	.getDataSourceDatabaseName(ConfigManager.getCurrentConfig()));

    assertEquals("LOCKSS", metadataDbManager
	.getDataSourceUser(ConfigManager.getCurrentConfig()));

    assertEquals("LOCKSS", metadataDbManager
	.getDataSourceSchemaName(ConfigManager.getCurrentConfig()));
  }

  /**
   * Tests PostgreSQL database creation with database name and user.
   */
  @Test
  public void testCreatePgsqlDb2() {
    ConfigurationUtil.addFromArgs(
	MetadataDbManager.PARAM_DATASOURCE_DATABASENAME, "TestDbManager",
	MetadataDbManager.PARAM_DATASOURCE_USER, "otherUser");

    createPgsqlDb();

    assertEquals("TestDbManager", metadataDbManager
	.getDataSourceDatabaseName(ConfigManager.getCurrentConfig()));

    assertEquals("otherUser", metadataDbManager
	.getDataSourceUser(ConfigManager.getCurrentConfig()));

    assertEquals("otherUser", metadataDbManager
	.getDataSourceSchemaName(ConfigManager.getCurrentConfig()));
  }

  /**
   * Tests PostgreSQL database creation with schema name and user.
   */
  @Test
  public void testCreatePgsqlDb3() {
    ConfigurationUtil.addFromArgs(
	DbManager.PARAM_DATABASE_NAME_PREFIX, "SomePrefix",
	MetadataDbManager.PARAM_DATASOURCE_USER, "otherUser",
	MetadataDbManager.PARAM_DATASOURCE_SCHEMA_NAME, "anotherSchema");

    createPgsqlDb();

    assertEquals("SomePrefixMetadataDbManager", metadataDbManager
	.getDataSourceDatabaseName(ConfigManager.getCurrentConfig()));

    assertEquals("otherUser", metadataDbManager
	.getDataSourceUser(ConfigManager.getCurrentConfig()));

    assertEquals("anotherSchema", metadataDbManager
	.getDataSourceSchemaName(ConfigManager.getCurrentConfig()));
  }

  /**
   * Creates a PostgreSQL database.
   */
  protected void createPgsqlDb() {
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"org.postgresql.ds.PGSimpleDataSource",
	MetadataDbManager.PARAM_MAX_RETRY_COUNT, "0",
	MetadataDbManager.PARAM_RETRY_DELAY, "0");

    metadataDbManager = new MetadataDbManager(true);
    metadataDbManager.initService(getMockLockssDaemon());
    metadataDbManager.startService();
  }

  /**
   * Tests table creation with a database name.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testCreateTable() throws Exception {
    ConfigurationUtil.addFromArgs(
	MetadataDbManager.PARAM_DATASOURCE_DATABASENAME, "TestDbManager");

    createTable();

    Configuration config = ConfigManager.getCurrentConfig();

    assertMatchesRE("/locksstest.*\\.tmp/db/TestDbManager$",
	metadataDbManager.getDataSourceDatabaseName(config));

    assertEquals("LOCKSS", metadataDbManager.getDataSourceUser(config));
  }

  /**
   * Tests table creation with the client datasource.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testCreateTable2() throws Exception {
    ConfigurationUtil.addFromArgs(
	DbManager.PARAM_START_DERBY_NETWORK_SERVER_CONTROL, "true");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"org.apache.derby.jdbc.ClientDataSource",
	MetadataDbManager.PARAM_DATASOURCE_PASSWORD, "somePassword");

    createTable();

    Configuration config = ConfigManager.getCurrentConfig();

    assertMatchesRE("/locksstest.*\\.tmp/db/LockssMetadataDbManager$",
	metadataDbManager.getDataSourceDatabaseName(config));

    assertEquals("LOCKSS", metadataDbManager.getDataSourceUser(config));
  }

  /**
   * Tests that setting enabled param to false works.
   */
  @Test
  public void testDisabled() {
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DBMANAGER_ENABLED,
	"false");
    // Create the database manager.
    metadataDbManager = getTestDbManager(tempDirPath);
    assertFalse(metadataDbManager.isReady());

    Configuration config = ConfigManager.getCurrentConfig();

    assertMatchesRE("/locksstest.*\\.tmp/db/LockssMetadataDbManager$",
	metadataDbManager.getDataSourceDatabaseName(config));

    assertEquals("LOCKSS", metadataDbManager.getDataSourceUser(config));

    try {
      metadataDbManager.getConnection();
      fail("getConnection() should throw");
    } catch (DbException sqle) {
    }
  }

  /**
   * Tests a misconfigured datasource.
   */
  @Test
  public void testNotReady() {
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"java.lang.String");
    // Create the database manager.
    metadataDbManager = getTestDbManager(tempDirPath);
    assertFalse(metadataDbManager.isReady());

    try {
      metadataDbManager.getConnection();
      fail("getConnection() should throw");
    } catch (DbException sqle) {
    }
  }

  @Override
  public void tearDown() throws Exception {
    metadataDbManager.stopService();
    theDaemon.stopDaemon();
    super.tearDown();
  }

  /**
   * Creates a table and verifies that it exists.
   * 
   * @throws Exception
   *           if there are problems.
   */
  protected void createTable() throws Exception {
    // Create the database manager.
    metadataDbManager = getTestDbManager(tempDirPath);
    assertTrue(metadataDbManager.isReady());
    DbManagerSql dbManagerSql = metadataDbManager.getDbManagerSql();

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);
    assertFalse(dbManagerSql.tableExists(conn, "testtable"));
    assertTrue(dbManagerSql
	.createTableIfMissing(conn, "testtable", TABLE_CREATE_SQL));
    assertTrue(dbManagerSql.tableExists(conn, "testtable"));
    dbManagerSql.logTableSchema(conn, "testtable");
    assertFalse(dbManagerSql
	.createTableIfMissing(conn, "testtable", TABLE_CREATE_SQL));
  }

  /**
   * Tests an empty database before updating.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testEmptyDbSetup() throws Exception {
    initializeTestDbManager(0, 0);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(metadataDbManager.getDbManagerSql().tableExists(conn,
	SqlConstants.VERSION_TABLE));
    assertEquals(1, countVersions(conn));
  }

  /**
   * Tests version 1 set up.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testV1Setup() throws Exception {
    initializeTestDbManager(1, 1);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(metadataDbManager.getDbManagerSql().tableExists(conn,
	SqlConstants.VERSION_TABLE));

    PreparedStatement ps = metadataDbManager.prepareStatement(conn,
	"select count(*) from " + SqlConstants.VERSION_TABLE);

    assertEquals(DbManager.DEFAULT_FETCH_SIZE, ps.getFetchSize());
    assertEquals(1, countVersions(conn));
  }

  /**
   * Tests the update of the database from version 0 to version 1.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testV0ToV1Update() throws Exception {
    initializeTestDbManager(0, 1);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(metadataDbManager.getDbManagerSql().tableExists(conn,
	SqlConstants.VERSION_TABLE));
    assertEquals(2, countVersions(conn));
  }

  /**
   * Tests the update of the database from version 0 to version 2.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testV0ToV2Update() throws Exception {
    initializeTestDbManager(0, 2);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(metadataDbManager.getDbManagerSql().tableExists(conn,
	SqlConstants.VERSION_TABLE));
    assertEquals(3, countVersions(conn));
  }

  /**
   * Tests the update of the database from version 1 to version 2.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testV1ToV2Update() throws Exception {
    initializeTestDbManager(1, 2);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(metadataDbManager.getDbManagerSql().tableExists(conn,
	SqlConstants.VERSION_TABLE));
    assertEquals(1, countVersions(conn));
  }

  /**
   * Initializes a database manager with a database with an initial version
   * updated to a target version.
   * 
   * @param initialVersion
   *          An int with the initial version.
   * @param targetVersion
   *          An Int with the target database.
   */
  private void initializeTestDbManager(int initialVersion, int targetVersion) {
    // Set the database log.
    System.setProperty("derby.stream.error.file",
		       new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create the database manager.
    metadataDbManager = new MetadataDbManager(true);
    theDaemon.setMetadataDbManager(metadataDbManager);
    metadataDbManager.initService(theDaemon);
    assertTrue(metadataDbManager.setUpDatabase(initialVersion));
    metadataDbManager.setTargetDatabaseVersion(targetVersion);
    metadataDbManager.startService();
  }

  /**
   * Counts rows in the version table.
   * 
   * @param conn
   *          A Connection to the database.
   * @return an int with the count of rows in the version table.
   */
  private int countVersions(Connection conn) {
    int count = -1;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      ps = metadataDbManager.prepareStatement(conn,
	  "select count(*) from " + SqlConstants.VERSION_TABLE);

      rs = ps.executeQuery();
      if (rs.next()) {
	count = rs.getInt(1);
      }
    } catch (Exception e) {
    } finally {
      MetadataDbManager.safeCloseResultSet(rs);
      MetadataDbManager.safeCloseStatement(ps);
    }

    return count;
  }

  /**
   * Tests authentication with the embedded data source.
   */
  @Test
  public void testAuthenticationEmbedded() {
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"org.apache.derby.jdbc.EmbeddedDataSource");

    metadataDbManager = getTestDbManager(tempDirPath);

    String dbUrlRoot =
      String.format("jdbc:derby://localhost:%s/%s/db/MetadataDbManager",
		    dbPort, tempDirPath);

    try {
      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
      DriverManager.getConnection(dbUrlRoot);
      fail("getConnection() should throw");
    } catch (Exception e) {
    }

    String dbUrl = dbUrlRoot + ";user=LOCKSS";

    try {
      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
      DriverManager.getConnection(dbUrl);
      fail("getConnection() should throw");
    } catch (Exception e) {
    }

    dbUrl = dbUrlRoot + ";user=LOCKSS;password=somePassword";

    try {
      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
      DriverManager.getConnection(dbUrl);
      fail("getConnection() should throw");
    } catch (Exception e) {
    }
  }

  /**
   * Tests set up with missing credentials.
   */
  @Test
  public void testMissingCredentialsSetUp() {
    ConfigurationUtil.addFromArgs(
	DbManager.PARAM_START_DERBY_NETWORK_SERVER_CONTROL, "true");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"org.apache.derby.jdbc.ClientDataSource");

    metadataDbManager = getTestDbManager(tempDirPath);
    assertTrue(metadataDbManager.isReady());
  }

  /**
   * Tests set up with missing user.
   */
  @Test
  public void testMissingUserSetUp() {
    ConfigurationUtil.addFromArgs(
	DbManager.PARAM_START_DERBY_NETWORK_SERVER_CONTROL, "true");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"org.apache.derby.jdbc.ClientDataSource");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_USER, "");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_PASSWORD,
	"somePassword");

    metadataDbManager = getTestDbManager(tempDirPath);
    assertFalse(metadataDbManager.isReady());
  }

  /**
   * Tests set up with missing password.
   */
  @Test
  public void testMissingPasswordSetUp() {
    ConfigurationUtil.addFromArgs(
	DbManager.PARAM_START_DERBY_NETWORK_SERVER_CONTROL, "true");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_CLASSNAME,
	"org.apache.derby.jdbc.ClientDataSource");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_USER,
	"LOCKSS");
    ConfigurationUtil.addFromArgs(MetadataDbManager.PARAM_DATASOURCE_PASSWORD,
	"");

    metadataDbManager = getTestDbManager(tempDirPath);
    assertFalse(metadataDbManager.isReady());
  }

  /**
   * Tests the provider functionality.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testProvider() throws Exception {
    metadataDbManager = getTestDbManager(tempDirPath);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();

    // Add a provider with no LOCKSS identifier.
    Long providerSeq1 =
	metadataDbManager.findOrCreateProvider(conn, null, "providerName1");
    checkProvider(conn, providerSeq1, null, "providerName1");

    // Add the new LOCKSS identifier to the same provider.
    Long providerSeq2 = metadataDbManager.findOrCreateProvider(conn,
	"providerLid1", "providerName1");
    assertEquals(providerSeq1, providerSeq2);
    checkProvider(conn, providerSeq2, "providerLid1", "providerName1");

    // Add a new provider with a LOCKSS identifier.
    Long providerSeq3 = metadataDbManager.findOrCreateProvider(conn,
	"providerLid2", "providerName2");
    assertNotEquals(providerSeq1, providerSeq3);
    checkProvider(conn, providerSeq3, "providerLid2", "providerName2");

    Long providerSeq4 = metadataDbManager.getMetadataDbManagerSqlBeforeReady()
	.getAuProvider(conn, "bogusP", "bogusK");
    assertNull(providerSeq4);

    DbManager.commitOrRollback(conn, log);
    DbManager.safeCloseConnection(conn);
  }

  private void checkProvider(Connection conn, Long providerSeq,
      String expectedLid, String expectedName) throws Exception {
    String query = "select " + PROVIDER_NAME_COLUMN
	+ "," + PROVIDER_LID_COLUMN
	+ " from " + PROVIDER_TABLE
	+ " where " + PROVIDER_SEQ_COLUMN + " = ?";
    PreparedStatement stmt = metadataDbManager.prepareStatement(conn, query);
    stmt.setLong(1, providerSeq);
    ResultSet resultSet = metadataDbManager.executeQuery(stmt);

    assertTrue(resultSet.next());
    assertEquals(expectedLid, resultSet.getString(PROVIDER_LID_COLUMN));
    assertEquals(expectedName, resultSet.getString(PROVIDER_NAME_COLUMN));
    assertFalse(resultSet.next());
  }

  /**
   * Tests the update of the database from version 0 to version 28.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void testV0ToV28Update() throws Exception {
    initializeTestDbManager(0, 28);
    assertTrue(metadataDbManager.isReady());

    Connection conn = metadataDbManager.getConnection();
    assertNotNull(conn);

    assertEquals(29, countVersions(conn));

    String query = "select " + MD_ITEM_TYPE_SEQ_COLUMN
	+ " from " + MD_ITEM_TYPE_TABLE
	+ " where " + TYPE_NAME_COLUMN + " = ?";
    PreparedStatement stmt = metadataDbManager.prepareStatement(conn, query);
    stmt.setString(1, MD_ITEM_TYPE_FILE);
    ResultSet resultSet = metadataDbManager.executeQuery(stmt);

    assertTrue(resultSet.next());
    assertEquals(11, resultSet.getLong(MD_ITEM_TYPE_SEQ_COLUMN));
  }
}
