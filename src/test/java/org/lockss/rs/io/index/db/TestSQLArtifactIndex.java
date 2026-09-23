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
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
    idxDbManager.setTargetDatabaseVersion(5);
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
   * {@code reindexArtifacts()} must invalidate the cached AU size of every AU touched
   * by the batch, not just the first artifact's: a temporary WARC's reindex interleaves
   * artifacts from more than one AU, so relying on the first artifact alone left every
   * other AU's cached size stale (issue #736).
   */
  @Test
  public void testReindexArtifactsInvalidatesAllAuSizesNotJustFirst() throws Exception {
    String ns = "multi-au-ns";
    String auid1 = "multi-au-auid-1";
    String auid2 = "multi-au-auid-2";

    // Seed a cached size for both AUs.
    AuSize seeded = new AuSize();
    seeded.setTotalLatestVersions(111L);
    seeded.setTotalAllVersions(222L);
    seeded.setTotalWarcSize(333L);
    index.updateAuSize(ns, auid1, seeded);
    index.updateAuSize(ns, auid2, seeded);

    assertNotNull(index.findAuSize(ns, auid1));
    assertNotNull(index.findAuSize(ns, auid2));

    List<Artifact> batch = new ArrayList<>();
    batch.add(new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid1)
        .setUrl("http://example.com/multi-au/1")
        .setVersion(1)
        .setStorageUrl(URI.create("storage_url_1"))
        .setContentLength(1)
        .setContentDigest("digest_1")
        .setCommitted(true)
        .setCollectionDate(1000L)
        .getArtifact());
    batch.add(new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid2)
        .setUrl("http://example.com/multi-au/2")
        .setVersion(1)
        .setStorageUrl(URI.create("storage_url_2"))
        .setContentLength(1)
        .setContentDigest("digest_2")
        .setCommitted(true)
        .setCollectionDate(1000L)
        .getArtifact());

    index.reindexArtifacts(batch);

    assertNull(index.findAuSize(ns, auid1),
        "AU1's cached size must be invalidated");
    assertNull(index.findAuSize(ns, auid2),
        "AU2's cached size must be invalidated too, not just the first artifact's AU");
  }
}
