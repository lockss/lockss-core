/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

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
package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.StringUtil;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

/**
 * Tests for {@link SQLArtifactIndex}.
 *
 * <p>This test class uses a shared embedded PostgreSQL instance for efficiency:
 * <ol>
 *   <li>{@code @BeforeAll setUpClass()} - Starts the shared embedded PostgreSQL instance once
 *       before any tests run.</li>
 *   <li>{@code makeArtifactIndex()} - Creates a unique database for each test within the shared
 *       PostgreSQL instance and initializes {@link SQLArtifactIndexDbManager}.</li>
 *   <li>{@code @AfterAll tearDownClass()} - Stops the shared PostgreSQL instance after all
 *       tests complete.</li>
 * </ol>
 */
public class TestSQLArtifactIndex extends AbstractArtifactIndexTest<SQLArtifactIndex> {
  private static L4JLogger log = L4JLogger.getLogger();

  /** Shared embedded PostgreSQL instance for all tests in this class */
  private static EmbeddedPostgres embeddedPg;

  private SQLArtifactIndexDbManager idxDbManager;
  private MockLockssDaemon theDaemon;
  private String tempDirPath;

  /**
   * Start the shared PostgreSQL instance once before any tests run.
   */
  @BeforeAll
  public static void setUpClass() throws Exception {
    EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();
    String extemp = System.getProperty("org.lockss.executableTempDir");
    if (!StringUtil.isNullString(extemp)) {
      builder.setOverrideWorkingDirectory(new File(extemp));
    }
    embeddedPg = builder.start();
    log.info("Started embedded PostgreSQL on port " + embeddedPg.getPort());
  }

  /**
   * Stop the shared PostgreSQL instance after all tests complete.
   */
  @AfterAll
  public static void tearDownClass() throws Exception {
    if (embeddedPg != null) {
      embeddedPg.close();
      embeddedPg = null;
    }
  }

  /**
   * Creates a new database with the given name and returns a DataSource for it.
   */
  private static DataSource createDatabase(String dbName) throws SQLException {
    int port = embeddedPg.getPort();
    String host = "localhost";
    String adminDb = "postgres";
    String username = "postgres";

    // Connect to admin database and create the new database
    String adminUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, adminDb);
    try (Connection adminConn = DriverManager.getConnection(adminUrl, username, "");
         Statement stmt = adminConn.createStatement()) {
      String createQuery = String.format("CREATE DATABASE %s TEMPLATE template0", dbName);
      stmt.executeUpdate(createQuery);
    }

    // Return a DataSource connected to the new database
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[]{host});
    ds.setPortNumbers(new int[]{port});
    ds.setDatabaseName(dbName);
    ds.setUser(username);
    return ds;
  }

  @Override
  protected SQLArtifactIndex makeArtifactIndex() throws Exception {
    // Construct mock LOCKSS daemon
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    // Get the temporary directory used during the test
    tempDirPath = setUpDiskSpace();

    initializePostgreSQL();

    return new SQLArtifactIndex();
  }

  protected void initializePostgreSQL() throws Exception {
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres",
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.enabled", "false",
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0");
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, PGSimpleDataSource.class.getCanonicalName());

    // Create a unique database for this test
    String dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
    DataSource ds = createDatabase(dbName);

    // Initialize the DbManager with the test database
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.setTestingDataSource(ds);
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  @Override
  public void testInitIndex() throws Exception {
    // Intentionally left blank
  }

  @Override
  public void testShutdownIndex() throws Exception {
    // Intentionally left blank
  }
}
