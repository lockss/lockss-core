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

import org.junit.Test;
import org.lockss.db.SqlConstants;
import org.lockss.metadata.MetadataDbManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class TestRepositoryDbManager extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private RepositoryDbManager repositoryDbManager;
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

  @Override
  public void tearDown() throws Exception {
    repositoryDbManager.stopService();
    theDaemon.stopDaemon();
    super.tearDown();
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
    repositoryDbManager = new RepositoryDbManager();
    repositoryDbManager.initService(theDaemon);

    assertTrue(repositoryDbManager.setUpDatabase(initialVersion));
    repositoryDbManager.setTargetDatabaseVersion(targetVersion);
    repositoryDbManager.startService();

    theDaemon.setRepositoryDbManager(repositoryDbManager);
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
      ps = repositoryDbManager.prepareStatement(conn,
          "select count(*) from " + SqlConstants.VERSION_TABLE);

      rs = ps.executeQuery();
      if (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (Exception e) {
    } finally {
      RepositoryDbManager.safeCloseResultSet(rs);
      RepositoryDbManager.safeCloseStatement(ps);
    }

    return count;
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
    assertTrue(repositoryDbManager.isReady());

    Connection conn = repositoryDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(repositoryDbManager.getDbManagerSql().tableExists(conn,
        SqlConstants.VERSION_TABLE));

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
    assertTrue(repositoryDbManager.isReady());

    Connection conn = repositoryDbManager.getConnection();
    assertNotNull(conn);
    assertTrue(repositoryDbManager.getDbManagerSql().tableExists(conn,
        SqlConstants.VERSION_TABLE));

    assertEquals(2, countVersions(conn));
  }
}
