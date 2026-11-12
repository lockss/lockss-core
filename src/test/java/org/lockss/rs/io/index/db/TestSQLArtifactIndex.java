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
import org.junit.jupiter.api.Test;
import org.lockss.config.Configuration;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.AbstractArtifactIndexTest;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.model.AuSize;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
    embeddedPg = startEmbeddedPostgres();
  }

  /**
   * Stop the shared PostgreSQL instance after all tests complete.
   */
  @AfterAll
  public static void tearDownClass() throws Exception {
    stopEmbeddedPostgre();
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
    // Create a unique database for this test
    String dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

    // Configure DbManager settings
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, "org.postgresql.ds.PGSimpleDataSource",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_DATABASENAME, dbName,
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_SERVERNAME, "localhost",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PORTNUMBER, String.valueOf(embeddedPg.getPort()));

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    // Initialize the DbManager with the test database
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(7);
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

  /**
   * Regression test for issue #661: {@code archival_unit_size} rows were keyed by
   * {@code NamespacedAuid.key(namespace, auid)}, a "|"-delimited string concatenation with no
   * escaping. Two distinct (namespace, auid) pairs that produce the same concatenated string --
   * e.g. ("a|b", "c") and ("a", "b|c"), both "a|b|c" -- collided in the AU size table. Storing
   * namespace and auid as separate columns removes the collision.
   */
  @Test
  public void testAuSizeIsScopedByNamespaceNotDelimitedConcatenation() throws Exception {
    SQLArtifactIndex sqlIndex = index;

    String namespace1 = "a|b";
    String auid1 = "c";

    String namespace2 = "a";
    String auid2 = "b|c";

    AuSize size1 = new AuSize();
    size1.setTotalLatestVersions(100L);
    size1.setTotalAllVersions(200L);
    size1.setTotalWarcSize(300L);

    AuSize size2 = new AuSize();
    size2.setTotalLatestVersions(1000L);
    size2.setTotalAllVersions(2000L);
    size2.setTotalWarcSize(3000L);

    sqlIndex.updateAuSize(namespace1, auid1, size1);
    sqlIndex.updateAuSize(namespace2, auid2, size2);

    assertEquals(size1, sqlIndex.findAuSize(namespace1, auid1));
    assertEquals(size2, sqlIndex.findAuSize(namespace2, auid2));

    // Invalidating one pair's AU size must not affect the other's entry
    sqlIndex.invalidateAuSize(namespace1, auid1);

    assertNull(sqlIndex.findAuSize(namespace1, auid1));
    assertEquals(size2, sqlIndex.findAuSize(namespace2, auid2));
  }
}
