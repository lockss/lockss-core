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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.VersionsEnum;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Tests for {@link SQLArtifactIndexDbManager} and {@link SQLArtifactIndexManagerSql}.
 *
 * <p>This test class verifies core artifact index functionality including:
 * <ul>
 *   <li><b>Core CRUD Operations</b> - Adding, retrieving, updating, committing, and deleting
 *       artifacts ({@code testAddArtifact}, {@code testGetArtifact}, {@code testCommitArtifact},
 *       {@code testUpdateStorageUrl}, {@code testDeleteArtifact}, etc.)</li>
 *   <li><b>Collection Operations</b> - Listing namespaces and AUIDs
 *       ({@code testGetNamespaces}, {@code testFindAuids})</li>
 *   <li><b>Query Methods</b> - Finding artifacts by various criteria including namespace, AUID,
 *       URL, URL prefix, and version filters ({@code testFindLatestArtifactsOfAllUrlsWithNamespaceAndAuid},
 *       {@code testFindArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid}, etc.)</li>
 *   <li><b>Namespace Isolation</b> - Verifying queries correctly filter by namespace</li>
 *   <li><b>AUID Isolation</b> - Verifying queries correctly filter by AUID</li>
 *   <li><b>Uncommitted Artifact Handling</b> - Verifying committed vs uncommitted artifact
 *       filtering behavior</li>
 *   <li><b>Other Query Logic</b> - Edge cases like special character URLs and empty prefixes</li>
 * </ul>
 *
 * <h3>Embedded PostgreSQL Lifecycle</h3>
 * <p>This test class uses the shared embedded PostgreSQL instance from
 * {@link LockssTestCase4} for efficiency. Each test gets a unique database
 * within the shared PostgreSQL instance for isolation.
 */
public class TestSQLArtifactIndexDbManager extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  /** Shared embedded PostgreSQL instance for all tests in this class */
  private static EmbeddedPostgres embeddedPg;

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private SQLArtifactIndexDbManager idxDbManager;
  /** The unique per-test database name, set in {@link #setUp()}. Used by the
   *  reindex-batching benchmark to open a raw connection to the same database
   *  (see {@code trySetSynchronousCommitOn()}). */
  private String dbName;

  @BeforeClass
  public static void setUpClass() throws Exception {
    embeddedPg = startEmbeddedPostgres();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    stopEmbeddedPostgre();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    // Create a unique database for this test
    dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

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

    // Initialize the DbManager with a unique database for this test
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(5);
    idxDbManager.startService();
    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  @Override
  public void tearDown() throws Exception {
    if (idxDbManager != null) {
      idxDbManager.stopService();
    }
    theDaemon.stopDaemon();
    super.tearDown();
  }

  @Test
  public void testAddArtifact() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("test")
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Add artifact
    idxdb.addArtifact(spec.getArtifact());

    // Assert against artifact spec
    spec.assertArtifactCommon(idxdb.getArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void testUpsertArtifactForReindex() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("test")
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Add artifact
    idxdb.upsertArtifactForReindex(spec.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    Artifact art = idxdb.getArtifact(spec.getArtifactUuid());
    // Assert against artifact spec
    spec.assertArtifactCommon(art);
    assertFalse(art.isCommitted());

    idxdb.upsertArtifactForReindex(spec.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);
    art = idxdb.getArtifact(spec.getArtifactUuid());
    spec.assertArtifactCommon(art);
    assertFalse(art.isCommitted());

    // Change storage URL & committed
    spec.setStorageUrl(URI.create("updated"));
    spec.setCommitted(true);
    idxdb.upsertArtifactForReindex(spec.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);
    art = idxdb.getArtifact(spec.getArtifactUuid());
    spec.assertArtifactCommon(art);
    assertTrue(art.isCommitted());
  }

  /**
   * Regression test for the bulk {@code addArtifacts} path: every artifact must
   * survive across multiple internal batch commits (no batch silently dropped),
   * and re-presenting the same artifacts (a retried finishBulkStore) must be
   * idempotent rather than failing on the UUID unique constraint or creating
   * duplicates.
   */
  @Test
  public void testAddArtifactsAcrossBatchesIsCompleteAndIdempotent() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "batch_ns";
    String auid = "batch_auid";

    // More than two ARTIFACT_INSERT_BATCH_SIZE (1000) rows so the per-batch
    // flush+commit runs several times, followed by a partial trailing batch.
    int n = 2500;
    List<ArtifactSpec> specs = new ArrayList<>(n);
    List<Artifact> artifacts = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(ns)
          .setAuid(auid)
          .setUrl("https://example.com/batch/" + i)
          .setVersion(1)
          .setStorageUrl(URI.create("tmp/" + i))
          .setContentLength(1024)
          .setContentDigest("digest-" + i)
          .setCollectionDate(1234L)
          .setCommitted(false);
      specs.add(spec);
      artifacts.add(spec.getArtifact());
    }

    idxdb.addArtifacts(artifacts);

    // No batch was silently dropped.
    for (ArtifactSpec spec : specs) {
      assertNotNull("Artifact missing after bulk add: " + spec.getArtifactUuid(),
          idxdb.getArtifact(spec.getArtifactUuid()));
    }
    assertEquals(n, countAllVersions(idxdb, ns, auid));

    // Retried finishBulkStore: same UUIDs, now committed with permanent URLs.
    List<Artifact> reflush = new ArrayList<>(n);
    for (ArtifactSpec spec : specs) {
      spec.setStorageUrl(URI.create("perm/" + spec.getArtifactUuid()));
      spec.setCommitted(true);
      reflush.add(spec.getArtifact());
    }
    idxdb.addArtifacts(reflush);

    // No duplicates, and the upsert refreshed the committed flag.
    assertEquals(n, countAllVersions(idxdb, ns, auid));
    for (ArtifactSpec spec : specs) {
      Artifact art = idxdb.getArtifact(spec.getArtifactUuid());
      assertNotNull(art);
      assertTrue("Re-flush should have marked committed: " + spec.getArtifactUuid(),
          art.isCommitted());
    }
  }

  /**
   * The geometric urls-table ANALYZE cadence must handle an index that is
   * already populated when a fresh manager starts: the row estimate is seeded
   * from pg_class.reltuples, and subsequent bulk adds still land every artifact.
   */
  @Test
  public void testUrlEstimateSeedsFromNonEmptyIndex() throws Exception {
    SQLArtifactIndexManagerSql first = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "seed_ns";
    String auid = "seed_auid";

    // Populate enough rows that the geometric cadence runs ANALYZE on urls,
    // which sets pg_class.reltuples for the table.
    int n = 2500;
    List<ArtifactSpec> specs = new ArrayList<>(n);
    List<Artifact> artifacts = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(ns)
          .setAuid(auid)
          .setUrl("https://example.com/seed/" + i)
          .setVersion(1)
          .setStorageUrl(URI.create("tmp/" + i))
          .setContentLength(1024)
          .setContentDigest("digest-" + i)
          .setCollectionDate(1234L);
      specs.add(spec);
      artifacts.add(spec.getArtifact());
    }
    first.addArtifacts(artifacts);

    // A fresh manager (as after a restart) must see a non-empty urls estimate.
    SQLArtifactIndexManagerSql restarted = new SQLArtifactIndexManagerSql(idxDbManager);
    assertTrue("urls reltuples should be seeded > 0 from a populated index",
        restarted.estimatedRowCount("urls") > 0);

    // And it must still add every new artifact correctly.
    List<ArtifactSpec> more = new ArrayList<>();
    List<Artifact> moreArts = new ArrayList<>();
    for (int i = 0; i < 1500; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(ns)
          .setAuid(auid)
          .setUrl("https://example.com/seed/more/" + i)
          .setVersion(1)
          .setStorageUrl(URI.create("tmp/more/" + i))
          .setContentLength(1024)
          .setContentDigest("d-" + i)
          .setCollectionDate(1234L);
      more.add(spec);
      moreArts.add(spec.getArtifact());
    }
    restarted.addArtifacts(moreArts);

    for (ArtifactSpec spec : more) {
      assertNotNull("Artifact missing after add on restarted manager: " + spec.getArtifactUuid(),
          restarted.getArtifact(spec.getArtifactUuid()));
    }
    assertEquals(n + more.size(), countAllVersions(restarted, ns, auid));
  }

  private static int countAllVersions(SQLArtifactIndexManagerSql idxdb,
                                      String ns, String auid) throws Exception {
    int found = 0;
    for (Artifact ignored :
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, true)) {
      found++;
    }
    return found;
  }

  @Test
  public void testGetArtifact() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("test")
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Sanity check / enforce contract
    assertNull(idxdb.getArtifact(spec.getArtifactUuid()));
    assertNull(idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true));

    // Add artifact to database
    idxdb.addArtifact(spec.getArtifact());

    Artifact byUuid = idxdb.getArtifact(spec.getArtifactUuid());
    Artifact byTuple = idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true);

    spec.assertArtifactCommon(byUuid);
    spec.assertArtifactCommon(byTuple);
  }

  @Test
  public void testGetLatestArtifact() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "test_namespace";
    String auid = "test_auid";
    String url = "test_url";

    ArtifactSpec spec1 = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(1)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    ArtifactSpec spec2 = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(2)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Sanity check / enforce contract
    assertNull(idxdb.getLatestArtifact(ns, auid, url, false));
    assertNull(idxdb.getLatestArtifact(ns, auid, url, true));

    // Add artifacts to database
    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());

    // Assert no max committed artifact but max uncommitted is v2
    assertNull(idxdb.getLatestArtifact(ns, auid, url, false));
    spec2.assertArtifactCommon(idxdb.getLatestArtifact(ns, auid, url, true));

    // Commit v1 artifact
    idxdb.commitArtifact(spec1.getArtifactUuid());
    spec1.setCommitted(true);

    // Assert latest committed is v1 and latest uncommitted remains v2
    spec1.assertArtifactCommon(idxdb.getLatestArtifact(ns, auid, url, false));
    spec2.assertArtifactCommon(idxdb.getLatestArtifact(ns, auid, url, true));

    // Commit v2 artifact
    idxdb.commitArtifact(spec2.getArtifactUuid());
    spec2.setCommitted(true);

    // Assert latest committed and latest uncommitted is v2
    spec2.assertArtifactCommon(idxdb.getLatestArtifact(ns, auid, url, false));
    spec2.assertArtifactCommon(idxdb.getLatestArtifact(ns, auid, url, true));
  }

  @Test
  public void testCommitArtifact() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url_1"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Add artifact to database
    idxdb.addArtifact(spec.getArtifact());

    // Assert artifact not committed
    Artifact pre = idxdb.getArtifact(spec.getArtifactUuid());
    assertFalse(pre.isCommitted());

    // Commit artifact
    idxdb.commitArtifact(spec.getArtifactUuid());

    // Assert artifact now committed
    Artifact post = idxdb.getArtifact(spec.getArtifactUuid());
    assertTrue(post.isCommitted());
  }

  @Test
  public void testUpdateStorageUrl() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url_1"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Add artifact to database
    idxdb.addArtifact(spec.getArtifact());

    // Assert URL pre-update
    Artifact pre = idxdb.getArtifact(spec.getArtifactUuid());
    assertEquals("storage_url_1", pre.getStorageUrl());

    // Update URL
    idxdb.updateStorageUrl(spec.getArtifactUuid(), "storage_url_2");

    // Assert URL pre-update
    Artifact post = idxdb.getArtifact(spec.getArtifactUuid());
    assertEquals("storage_url_2", post.getStorageUrl());
  }

  @Test
  public void testDeleteArtifact() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Sanity check
    assertNull(idxdb.getArtifact(spec.getArtifactUuid()));

    // Add artifact to database
    idxdb.addArtifact(spec.getArtifact());

    // Assert artifact pre-delete
    Artifact pre = idxdb.getArtifact(spec.getArtifactUuid());
    spec.assertArtifactCommon(pre);

    // Delete artifact
    idxdb.deleteArtifact(spec.getArtifactUuid());

    // Assert null post-delete
    assertNull(idxdb.getArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void testGetNamespaces() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "test_namespace";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Sanity check
    assertEmpty(idxdb.getNamespaces());

    // Add artifact to database
    idxdb.addArtifact(spec.getArtifact());

    assertIterableEquals(List.of(ns), idxdb.getNamespaces());
  }

  @Test
  public void testFindAuids() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "test_namespace";
    String auid = "test_auid";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Sanity check
    assertEmpty(idxdb.findAuids(ns));

    // Add artifact to database
    idxdb.addArtifact(spec.getArtifact());

    assertIterableEquals(List.of(auid), idxdb.findAuids(ns));
  }


  private static ArtifactSpec makeArtifactSpec(String ns, String auid, String url, int version) {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(version)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(TimeBase.nowMs());

    log.debug2("spec = " + spec);
    return spec;
  }

  private static ArtifactSpec makeArtifactSpecWithSize(String ns, String auid, String url, int version, int size) {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(version)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(size)
        .setContentDigest("digest")
        .setCollectionDate(TimeBase.nowMs());

    log.debug2("spec = " + spec);
    return spec;
  }

  private List<Artifact> getArtifactsFromSpecs(ArtifactSpec... specs) {
    List<Artifact> expected = Stream.of(specs)
        .map(ArtifactSpec::getArtifact)
        .toList();

    return expected;
  }

  @Test
  public void testFindLatestArtifactsOfAllUrlsWithNamespaceAndAuid() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "a1";

    // Sanity check
    assertEmpty(idxdb.findAuids(ns));

    ArtifactSpec[] specs = {
        makeArtifactSpec("ns1", "a1", "url1", 1),
        makeArtifactSpec("ns1", "a1", "url1", 2),
        makeArtifactSpec("ns1", "a1", "url2", 1),
        makeArtifactSpec("ns1", "a2", "url1", 1),
        makeArtifactSpec("ns2", "a1", "url1", 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    // Assert results match expected when including uncommitted artifacts
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[2]);
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    idxdb.commitArtifact(specs[0].getArtifactUuid());
    idxdb.commitArtifact(specs[2].getArtifactUuid());
    specs[0].setCommitted(true);
    specs[2].setCommitted(true);

    // Assert results match expected when excluding uncommitted artifacts
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[2]);
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);
      assertIterableEquals(expected, result);
    }

    // Assert results match expected when including uncommitted artifacts
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[2]);
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testFindArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "a1";

    // Sanity check
    assertEmpty(idxdb.findAuids(ns));

    ArtifactSpec[] specs = {
        makeArtifactSpec("ns1", "a1", "url1", 1),
        makeArtifactSpec("ns1", "a1", "url1", 2),
        makeArtifactSpec("ns1", "a1", "url2", 1),
        makeArtifactSpec("ns1", "a1", "url1", 3),
        makeArtifactSpec("ns1", "a2", "url1", 1),
        makeArtifactSpec("ns2", "a1", "url1", 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    // Assert all versions of all URLs for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[1], specs[0], specs[2]);
      Iterable<Artifact> result = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    idxdb.commitArtifact(specs[0].getArtifactUuid());
    idxdb.commitArtifact(specs[3].getArtifactUuid());
    idxdb.commitArtifact(specs[4].getArtifactUuid());
    specs[0].setCommitted(true);
    specs[3].setCommitted(true);
    specs[4].setCommitted(true);

    // Assert we get back the correct committed artifacts for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[0]);
      Iterable<Artifact> result = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);
      assertIterableEquals(expected, result);
    }

    // Assert all versions of all URLs for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[1], specs[0], specs[2]);
      Iterable<Artifact> result = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testFindArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "a1";
    String url = "url1";

    // Sanity check
    assertEmpty(idxdb.findAuids(ns));

    ArtifactSpec[] specs = {
        makeArtifactSpec("ns1", "a1", "url1", 1),
        makeArtifactSpec("ns1", "a1", "url1", 2),
        makeArtifactSpec("ns1", "a1", "url2", 1),
        makeArtifactSpec("ns1", "a1", "url1", 3),
        makeArtifactSpec("ns1", "a2", "url1", 1),
        makeArtifactSpec("ns2", "a1", "url1", 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url));

    // Commit artifacts
    idxdb.commitArtifact(specs[0].getArtifactUuid());
    idxdb.commitArtifact(specs[3].getArtifactUuid());
    idxdb.commitArtifact(specs[4].getArtifactUuid());
    specs[0].setCommitted(true);
    specs[3].setCommitted(true);
    specs[4].setCommitted(true);

    // Assert we get back the correct committed artifacts for the namespace and AUID
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[0]);
      Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testFindArtifactsAllCommittedVersionsOfUrlFromAllAuids() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "ns1";
    String ns2 = "ns2";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String url1 = "url1";
    String url2 = "url2";

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, auid1, url1, 1),
        makeArtifactSpec(ns1, auid1, url1, 2),
        makeArtifactSpec(ns1, auid1, url1, 3),
        makeArtifactSpec(ns1, auid2, url1, 4),
        makeArtifactSpec(ns2, auid1, url1, 5),
        makeArtifactSpec(ns1, auid1, url2, 6),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty results (no committed artifacts)
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, VersionsEnum.ALL));
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, VersionsEnum.LATEST));

    // Commit artifacts
    commitSpecs(idxdb, specs, 0, 3, 4, 5);

    // Assert with ALL
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, VersionsEnum.ALL);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(idxdb, specs, 1);

    // Assert with LATEST
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, VersionsEnum.LATEST);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testFindArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "ns1";
    String ns2 = "ns2";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String url1 = "url1";
    String url2 = "url2";

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, auid1, url1, 1),
        makeArtifactSpec(ns1, auid1, url1, 2),
        makeArtifactSpec(ns1, auid1, url1, 3),
        makeArtifactSpec(ns1, auid2, url1, 4),
        makeArtifactSpec(ns2, auid1, url1, 5),
        makeArtifactSpec(ns1, auid1, url2, 6),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty results (no committed artifacts)
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, VersionsEnum.ALL));
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, VersionsEnum.LATEST));

    // Commit artifacts
    commitSpecs(idxdb, specs, 0, 3, 4, 5);

    // Assert with ALL
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, VersionsEnum.ALL);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(idxdb, specs, 1);

    // Assert with LATEST
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, VersionsEnum.LATEST);
      assertIterableEquals(expected, result);
    }
  }

  private static void commitSpecs(SQLArtifactIndexManagerSql idxdb, ArtifactSpec specs[], int... idx) throws DbException {
    for (int i : idx) {
      idxdb.commitArtifact(specs[i].getArtifactUuid());
      specs[i].setCommitted(true);
    }
  }

  private static void deleteSpecs(SQLArtifactIndexManagerSql idxdb, ArtifactSpec specs[], int... idx) throws DbException {
    for (int i : idx) {
      idxdb.deleteArtifact(specs[i].getArtifactUuid());
      specs[i].setDeleted(true);
    }
  }

  @Test
  public void testFindArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "a1";
    String url = "url";

    ArtifactSpec[] specs = {
        makeArtifactSpec("ns1", "a1", "url1", 1),
        makeArtifactSpec("ns1", "a1", "url1", 2),
        makeArtifactSpec("ns1", "a1", "url2", 1),
        makeArtifactSpec("ns1", "a1", "url1", 3),
        makeArtifactSpec("ns1", "a2", "url1", 1),
        makeArtifactSpec("ns2", "a1", "url1", 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty result (no committed artifacts)
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, url));

    // Commit artifacts
    idxdb.commitArtifact(specs[0].getArtifactUuid());
    idxdb.commitArtifact(specs[3].getArtifactUuid());
    specs[0].setCommitted(true);
    specs[3].setCommitted(true);

    // Assert we get back the correct committed artifacts for the namespace and AUID
    List<Artifact> expected = getArtifactsFromSpecs(specs[3], specs[0]);
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, url);
    assertIterableEquals(expected, result);
  }

  @Test
  public void testFindArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "ns1";
    String ns2 = "ns2";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String url1 = "url1";
    String url2 = "url2";

    ArtifactSpec[] specs = {
        makeArtifactSpec(ns1, auid1, url1, 1), //0
        makeArtifactSpec(ns1, auid1, url1, 2),
        makeArtifactSpec(ns1, auid1, url1, 3),
        makeArtifactSpec(ns1, auid2, url1, 4), // 3
        makeArtifactSpec(ns2, auid1, url1, 5), // 4
        makeArtifactSpec(ns1, auid1, url2, 6), // 5
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    // Assert empty results (no committed artifacts)
    assertEmpty(idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid1, url1));
    assertEmpty(idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid1, url1));

    // Commit artifacts
    commitSpecs(idxdb, specs, 0, 3, 4, 5);

    // Assert results with (ns1, auid1, url1)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0]);
      Iterable<Artifact> result =
          idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid1, url1);
      assertIterableEquals(expected, result);
    }

    // Assert results with (ns1, auid2, url1)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid2, url1);
      assertIterableEquals(expected, result);
    }

    // Assert results with (ns2, auid1, url1)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[4]);
      Iterable<Artifact> result =
          idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns2, auid1, url1);
      assertIterableEquals(expected, result);
    }

    // Assert results with (ns1, auid1, url2)
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[5]);
      Iterable<Artifact> result =
          idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid1, url2);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(idxdb, specs, 1);

    // Assert result with second set of commits
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1]);
      Iterable<Artifact> result =
          idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid1, url1);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testGetSizeOfArtifacts() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "ns1";
    String auid1 = "auid1";
    String url1 = "url1";
    String url2 = "url2";

    ArtifactSpec[] specs = {
        makeArtifactSpecWithSize(ns1, auid1, url1, 1, 1),
        makeArtifactSpecWithSize(ns1, auid1, url1, 2, 1),
        makeArtifactSpecWithSize(ns1, auid1, url2, 1, 1),
    };

    // Add artifacts to database
    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
    }

    assertEquals(0, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.ALL));
    assertEquals(0, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.LATEST));

    commitSpecs(idxdb, specs, 0, 2);
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.ALL));
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.LATEST));

    commitSpecs(idxdb, specs, 1);
    assertEquals(3, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.ALL));
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.LATEST));

    deleteSpecs(idxdb, specs, 0);
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.ALL));
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, VersionsEnum.LATEST));
  }

  // ============================================================================
  // Namespace Isolation Tests
  // ============================================================================

  @Test
  public void testFindLatestArtifacts_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String auid = "auid1";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, auid, "http://example.com/path", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, auid, "http://example.com/path", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    // Query ns1 only
    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns1, auid, false));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  @Test
  public void testFindAllVersions_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String auid = "auid1";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, auid, "http://example.com/path", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, auid, "http://example.com/path", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns1, auid, false));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  @Test
  public void testFindVersionsOfUrl_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String auid = "auid1";
    String url = "http://example.com/shared";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, auid, url, 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, auid, url, 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns1, auid, url));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  @Test
  public void testFindUrlAllAuids_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String url = "http://example.com/shared";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, "auid1", url, 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, "auid1", url, 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url, VersionsEnum.ALL));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  @Test
  public void testFindByPrefixAllAuids_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String prefix = "http://example.com/prefix/";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, "auid1", prefix + "file", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, "auid1", prefix + "file", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, prefix, VersionsEnum.ALL));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  @Test
  public void testFindLatestByPrefix_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String auid = "auid1";
    String prefix = "http://example.com/prefix/";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, auid, prefix + "file", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, auid, prefix + "file", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid, prefix));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  @Test
  public void testFindAllVersionsByPrefix_NamespaceIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String auid = "auid1";
    String prefix = "http://example.com/prefix/";

    ArtifactSpec spec1 = makeArtifactSpec(ns1, auid, prefix + "file", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns2, auid, prefix + "file", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns1, auid, prefix));

    assertEquals(1, artifacts.size());
    assertEquals(ns1, artifacts.get(0).getNamespace());
  }

  // ============================================================================
  // AUID Isolation Tests
  // ============================================================================

  @Test
  public void testFindLatestArtifacts_AuidIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";

    ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, "http://example.com/path", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, "http://example.com/path", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid1, false));

    assertEquals(1, artifacts.size());
    assertEquals(auid1, artifacts.get(0).getAuid());
  }

  @Test
  public void testFindAllVersions_AuidIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";

    ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, "http://example.com/path", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, "http://example.com/path", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid1, false));

    assertEquals(1, artifacts.size());
    assertEquals(auid1, artifacts.get(0).getAuid());
  }

  @Test
  public void testFindVersionsOfUrl_AuidIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String url = "http://example.com/shared";

    ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, url, 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, url, 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid1, url));

    assertEquals(1, artifacts.size());
    assertEquals(auid1, artifacts.get(0).getAuid());
  }

  @Test
  public void testFindLatestByPrefix_AuidIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String prefix = "http://example.com/shared/";

    ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, prefix + "file", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, prefix + "file", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid1, prefix));

    assertEquals(1, artifacts.size());
    assertEquals(auid1, artifacts.get(0).getAuid());
  }

  @Test
  public void testFindAllVersionsByPrefix_AuidIsolation() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String prefix = "http://example.com/prefix/";

    ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, prefix + "file", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, prefix + "file", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid1, prefix));

    assertEquals(1, artifacts.size());
    assertEquals(auid1, artifacts.get(0).getAuid());
  }

  // ============================================================================
  // Uncommitted Artifact Tests
  // ============================================================================

  @Test
  public void testFindLatestArtifacts_IncludeUncommitted() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/path";

    // v1 committed, v2 uncommitted
    ArtifactSpec specV1 = makeArtifactSpec(ns, auid, url, 1);
    ArtifactSpec specV2 = makeArtifactSpec(ns, auid, url, 2);

    idxdb.addArtifact(specV1.getArtifact());
    idxdb.addArtifact(specV2.getArtifact());
    idxdb.commitArtifact(specV1.getArtifactUuid());

    // Excluding uncommitted - should get v1
    List<Artifact> committed = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));
    assertEquals(1, committed.size());
    assertEquals(Integer.valueOf(1), committed.get(0).getVersion());

    // Including uncommitted - should get v2
    List<Artifact> all = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, true));
    assertEquals(1, all.size());
    assertEquals(Integer.valueOf(2), all.get(0).getVersion());
  }

  @Test
  public void testFindAllVersions_IncludeUncommitted() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/path";

    // v1 committed, v2 uncommitted
    ArtifactSpec specV1 = makeArtifactSpec(ns, auid, url, 1);
    ArtifactSpec specV2 = makeArtifactSpec(ns, auid, url, 2);

    idxdb.addArtifact(specV1.getArtifact());
    idxdb.addArtifact(specV2.getArtifact());
    idxdb.commitArtifact(specV1.getArtifactUuid());

    // Excluding uncommitted
    List<Artifact> committed = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));
    assertEquals(1, committed.size());

    // Including uncommitted
    List<Artifact> all = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, true));
    assertEquals(2, all.size());
  }

  @Test
  public void testFindVersionsOfUrl_OnlyCommitted() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/test-url";

    // v1 and v2 committed, v3 uncommitted
    ArtifactSpec specV1 = makeArtifactSpec(ns, auid, url, 1);
    ArtifactSpec specV2 = makeArtifactSpec(ns, auid, url, 2);
    ArtifactSpec specV3 = makeArtifactSpec(ns, auid, url, 3);

    idxdb.addArtifact(specV1.getArtifact());
    idxdb.addArtifact(specV2.getArtifact());
    idxdb.addArtifact(specV3.getArtifact());
    idxdb.commitArtifact(specV1.getArtifactUuid());
    idxdb.commitArtifact(specV2.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url));

    assertEquals(2, artifacts.size());
    for (Artifact a : artifacts) {
      assertTrue(a.getVersion() <= 2);
    }
  }

  @Test
  public void testFindUrlAllAuids_LatestVersionOnly() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String url = "http://example.com/shared-url";

    // Create 2 AUIDs with 3 versions each
    for (String auid : new String[]{"auid1", "auid2"}) {
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // ALL versions
    List<Artifact> all = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));
    assertEquals(6, all.size());

    // LATEST version only
    List<Artifact> latest = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.LATEST));
    assertEquals(2, latest.size());
    for (Artifact a : latest) {
      assertEquals(Integer.valueOf(3), a.getVersion());
    }
  }

  @Test
  public void testFindByPrefixAllAuids_LatestVersionOnly() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String prefix = "http://example.com/data/";

    // Create artifacts with multiple versions
    for (int v = 1; v <= 3; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, "auid1", prefix + "file", v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // ALL versions
    List<Artifact> all = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));
    assertEquals(3, all.size());

    // LATEST version only
    List<Artifact> latest = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.LATEST));
    assertEquals(1, latest.size());
    assertEquals(Integer.valueOf(3), latest.get(0).getVersion());
  }

  @Test
  public void testFindAllVersionsByPrefix_OnlyCommitted() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/test/";

    // v1 committed, v2 uncommitted
    ArtifactSpec specV1 = makeArtifactSpec(ns, auid, prefix + "file", 1);
    ArtifactSpec specV2 = makeArtifactSpec(ns, auid, prefix + "file", 2);

    idxdb.addArtifact(specV1.getArtifact());
    idxdb.addArtifact(specV2.getArtifact());
    idxdb.commitArtifact(specV1.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals(1, artifacts.size());
    assertEquals(Integer.valueOf(1), artifacts.get(0).getVersion());
  }

  // ============================================================================
  // Other Query Logic Tests
  // ============================================================================

  @Test
  public void testFindLatestArtifacts_SpecialCharacterUrls() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "auid1";

    String[] urls = {
        "http://example.com/path/with spaces/file.html",
        "http://example.com/path/with?query=param",
        "http://example.com/path/with#fragment",
        "http://example.com/unicode/资源/file"
    };

    for (String url : urls) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals(urls.length, artifacts.size());
  }

  @Test
  public void testFindAllVersionsByPrefix_EmptyPrefix() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns1";
    String auid = "auid1";

    ArtifactSpec spec1 = makeArtifactSpec(ns, auid, "http://example.com/path1", 1);
    ArtifactSpec spec2 = makeArtifactSpec(ns, auid, "http://other.com/path2", 1);

    idxdb.addArtifact(spec1.getArtifact());
    idxdb.addArtifact(spec2.getArtifact());
    idxdb.commitArtifact(spec1.getArtifactUuid());
    idxdb.commitArtifact(spec2.getArtifactUuid());

    // Empty prefix should match all
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, ""));

    assertEquals(2, artifacts.size());
  }

  private static <T> List<T> toList(Iterable<T> iterable) {
    List<T> list = new ArrayList<>();
    for (T item : iterable) {
      list.add(item);
    }
    return list;
  }

  //
  // Component D: version-conflict resolution on the reindex path.
  //
  // The artifacts table's only unique index is on artifact uuid, so two
  // artifacts can claim the same (namespace, auid, url, version) under
  // different uuids. UPSERT_ARTIFACT_FOR_REINDEX_QUERY's ON CONFLICT (uuid)
  // cannot see that, so before Component D a reindex simply added a second
  // row. These tests run against the embedded PostgreSQL instance shared by
  // this class, i.e. against the real query, not a mock.
  //

  private static final String CONFLICT_NS = "conflict_ns";
  private static final String CONFLICT_AUID = "conflict_auid";
  private static final String CONFLICT_URL = "http://example.com/conflict.gif";

  /** An artifact at the fixed conflict tuple, with the given crawl time. */
  private static ArtifactSpec makeConflictSpec(long collectionDate) {
    return new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(CONFLICT_NS)
        .setAuid(CONFLICT_AUID)
        .setUrl(CONFLICT_URL)
        .setVersion(1)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCommitted(true)
        .setCollectionDate(collectionDate);
  }

  /**
   * PreferEarliest: reindexing an artifact whose collection date precedes that
   * of an artifact already holding the tuple keeps the incoming one and
   * removes the incumbent, leaving exactly one row.
   */
  @Test
  public void testReindexVersionConflictPreferEarliest() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    // The later artifact is already indexed (e.g. stored by a re-crawl).
    ArtifactSpec later = makeConflictSpec(1754813456762L);
    idxdb.addArtifact(later.getArtifact());

    // The earlier artifact arrives from the WARC being reindexed.
    ArtifactSpec earlier = makeConflictSpec(1330125997952L);
    idxdb.upsertArtifactForReindex(earlier.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
    assertNotNull(idxdb.getArtifact(earlier.getArtifactUuid()));
    assertNull(idxdb.getArtifact(later.getArtifactUuid()));
    assertEquals(1330125997952L,
        idxdb.getArtifact(CONFLICT_NS, CONFLICT_AUID, CONFLICT_URL, 1, true)
            .getCollectionDate());
  }

  /**
   * PreferLatest: the incumbent with the later collection date wins, the
   * incoming artifact is not inserted, and exactly one row remains.
   */
  @Test
  public void testReindexVersionConflictPreferLatest() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec later = makeConflictSpec(1754813456762L);
    idxdb.addArtifact(later.getArtifact());

    ArtifactSpec earlier = makeConflictSpec(1330125997952L);
    idxdb.upsertArtifactForReindex(earlier.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferLatest);

    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
    assertNotNull(idxdb.getArtifact(later.getArtifactUuid()));
    assertNull(idxdb.getArtifact(earlier.getArtifactUuid()));
  }

  /**
   * PreferLatest, incoming artifact wins: the incumbent is deleted.
   */
  @Test
  public void testReindexVersionConflictPreferLatestIncomingWins() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec earlier = makeConflictSpec(1330125997952L);
    idxdb.addArtifact(earlier.getArtifact());

    ArtifactSpec later = makeConflictSpec(1754813456762L);
    idxdb.upsertArtifactForReindex(later.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferLatest);

    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
    assertNotNull(idxdb.getArtifact(later.getArtifactUuid()));
    assertNull(idxdb.getArtifact(earlier.getArtifactUuid()));
  }

  /**
   * Equal crawl times: the tie-break keeps the existing row, under either
   * policy, and no duplicate is created.
   */
  @Test
  public void testReindexVersionConflictEqualCrawlTimeKeepsExisting() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      ArtifactSpec existing = makeConflictSpec(1330125997952L);
      idxdb.addArtifact(existing.getArtifact());

      ArtifactSpec incoming = makeConflictSpec(1330125997952L);
      idxdb.upsertArtifactForReindex(incoming.getArtifact(), policy);

      assertEquals("policy " + policy,
          1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertNotNull("policy " + policy,
          idxdb.getArtifact(existing.getArtifactUuid()));
      assertNull("policy " + policy,
          idxdb.getArtifact(incoming.getArtifactUuid()));

      // Clean up for the next policy.
      idxdb.deleteArtifact(existing.getArtifactUuid());
    }
  }

  /**
   * No conflict: the ordinary reindex insert is untouched, including its
   * pre-existing ON CONFLICT (uuid) behaviour when the same artifact is
   * presented again.
   */
  @Test
  public void testReindexWithoutVersionConflictInsertsNormally() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = makeConflictSpec(1330125997952L);
    idxdb.upsertArtifactForReindex(spec.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
    spec.assertArtifactCommon(idxdb.getArtifact(spec.getArtifactUuid()));

    // A different version of the same URL is not a conflict.
    ArtifactSpec v2 = makeConflictSpec(1330125997952L);
    v2.setVersion(2);
    idxdb.upsertArtifactForReindex(v2.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(2, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

    // A different URL is not a conflict either.
    ArtifactSpec otherUrl = makeConflictSpec(1330125997952L);
    otherUrl.setUrl("http://example.com/other.gif");
    idxdb.upsertArtifactForReindex(otherUrl.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(3, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
  }

  /**
   * Recovery must converge and stay converged: reindexing both artifacts of a
   * damaged pair, twice, in either order, leaves the policy's winner and only
   * the policy's winner.
   *
   * <p>This is what the "delete the incoming artifact's own row when the
   * incumbent wins" branch buys: without it, PreferLatest would leave the
   * losing (earlier) artifact's row in place forever, since its insert is
   * merely skipped.
   */
  @Test
  public void testReindexVersionConflictIsIdempotent() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      // A pre-existing damaged index: both artifacts already present.
      ArtifactSpec earlier = makeConflictSpec(1330125997952L);
      ArtifactSpec later = makeConflictSpec(1754813456762L);
      idxdb.addArtifact(earlier.getArtifact());
      idxdb.addArtifact(later.getArtifact());
      assertEquals(msg, 2, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      ArtifactSpec winner =
          policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
              ? earlier : later;
      ArtifactSpec loser =
          policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
              ? later : earlier;

      // Two full reindex passes, each presenting both artifacts.
      for (int pass = 1; pass <= 2; pass++) {
        idxdb.upsertArtifactForReindex(earlier.getArtifact(), policy);
        idxdb.upsertArtifactForReindex(later.getArtifact(), policy);

        assertEquals(msg + " pass " + pass,
            1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
        assertNotNull(msg + " pass " + pass,
            idxdb.getArtifact(winner.getArtifactUuid()));
        assertNull(msg + " pass " + pass,
            idxdb.getArtifact(loser.getArtifactUuid()));
      }

      idxdb.deleteArtifact(winner.getArtifactUuid());
    }
  }

  /**
   * More than two rows on the tuple -- possible in an index damaged before
   * this code existed -- collapse to the single policy winner.
   */
  @Test
  public void testReindexVersionConflictWithSeveralExistingRows() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec a = makeConflictSpec(3000L);
    ArtifactSpec b = makeConflictSpec(2000L);
    ArtifactSpec c = makeConflictSpec(4000L);
    idxdb.addArtifact(a.getArtifact());
    idxdb.addArtifact(b.getArtifact());
    idxdb.addArtifact(c.getArtifact());
    assertEquals(3, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

    // Incoming is earlier than all three, so PreferEarliest keeps it alone.
    ArtifactSpec incoming = makeConflictSpec(1000L);
    idxdb.upsertArtifactForReindex(incoming.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
    assertNotNull(idxdb.getArtifact(incoming.getArtifactUuid()));
  }

  /**
   * The other side of the same case: the incoming artifact loses, and the one
   * remaining row is the policy's pick among the existing rows.
   */
  @Test
  public void testReindexVersionConflictLosingIncomingCollapsesExisting() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec a = makeConflictSpec(3000L);
    ArtifactSpec b = makeConflictSpec(2000L);
    ArtifactSpec c = makeConflictSpec(4000L);
    idxdb.addArtifact(a.getArtifact());
    idxdb.addArtifact(b.getArtifact());
    idxdb.addArtifact(c.getArtifact());

    ArtifactSpec incoming = makeConflictSpec(9000L);
    idxdb.upsertArtifactForReindex(incoming.getArtifact(),
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
    assertNotNull(idxdb.getArtifact(b.getArtifactUuid()));
    assertNull(idxdb.getArtifact(a.getArtifactUuid()));
    assertNull(idxdb.getArtifact(c.getArtifactUuid()));
    assertNull(idxdb.getArtifact(incoming.getArtifactUuid()));
  }

    /**
   * An artifact at the fixed conflict tuple, with an explicit uuid and the
   * default content digest.
   *
   * <p>The digest is FIXED here, and that is load-bearing for Component D.4:
   * every caller of this overload presents the same digest, so an equal
   * crawl_time between two such specs is an equal-date/equal-digest pair and
   * the D.4 term blocks the update unless it is a committed upgrade. Use
   * {@link #makeSameUuidSpec(String, boolean, String, long, String)} when a
   * test needs the digests to differ.
   */
  private static ArtifactSpec makeSameUuidSpec(String uuid, boolean committed,
                                               String storageUrl,
                                               long collectionDate) {
    return makeSameUuidSpec(uuid, committed, storageUrl, collectionDate,
                            "digest");
  }

  /** An artifact at the fixed conflict tuple, with an explicit uuid and an
   *  explicit content digest. */
  private static ArtifactSpec makeSameUuidSpec(String uuid, boolean committed,
                                               String storageUrl,
                                               long collectionDate,
                                               String contentDigest) {
    return new ArtifactSpec()
        .setArtifactUuid(uuid)
        .setNamespace(CONFLICT_NS)
        .setAuid(CONFLICT_AUID)
        .setUrl(CONFLICT_URL)
        .setVersion(1)
        .setStorageUrl(URI.create(storageUrl))
        .setContentLength(1)
        .setContentDigest(contentDigest)
        .setCommitted(committed)
        .setCollectionDate(collectionDate);
  }

  /**
   * Sets an existing row's committed column to SQL NULL. The column is nullable
   * in the DDL, and {@link org.lockss.util.rest.repo.model.Artifact} coerces a
   * null committed to false, so this cannot be done through the normal API.
   */
  private void setCommittedNull(String uuid) throws Exception {
    Connection conn = idxDbManager.getConnection();

    try (PreparedStatement ps = conn.prepareStatement(
             "UPDATE artifacts SET committed = NULL WHERE uuid = ?")) {
      ps.setString(1, uuid);
      assertEquals(1, ps.executeUpdate());
      // DbManager connections are not autocommit.
      conn.commit();
    } finally {
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * Reads the committed column as a nullable Boolean; {@code getArtifact()}
   * maps SQL NULL to false, so it cannot distinguish the two.
   */
  private Boolean readCommittedRaw(String uuid) throws Exception {
    Connection conn = idxDbManager.getConnection();

    try (PreparedStatement ps = conn.prepareStatement(
             "SELECT committed FROM artifacts WHERE uuid = ?")) {
      ps.setString(1, uuid);

      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        boolean value = rs.getBoolean(1);
        return rs.wasNull() ? null : Boolean.valueOf(value);
      }
    } finally {
      DbManager.safeCloseConnection(conn);
    }
  }

  /**
   * THE REGRESSION THIS CHANGE EXISTS FOR.
   *
   * <p>A committed artifact with a permanent storage URL is already indexed.
   * Reindexing then presents the SAME uuid from a temporary WARC whose journal
   * entry is missing -- committed=false, temporary storage URL. The row must
   * keep committed=true AND the permanent storage URL: an artifact coming back
   * is preferable to an artifact disappearing.
   *
   * <p>Holds under either policy, because the guard is on committed, not on
   * the collection date (which is identical for both records here).
   */
  @Test
  public void testReindexDoesNotDowngradeCommittedOrClobberStorageUrl()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();

      // Already indexed from a permanent WARC.
      idxdb.addArtifact(makeSameUuidSpec(uuid, true,
          "file:///data/permanent/artifacts.warc?offset=100", 1330125997952L)
          .getArtifact());

      // The same artifact re-encountered in a temporary WARC whose journal
      // entry is missing: WarcArtifactDataStore's UNKNOWN branch.
      idxdb.upsertArtifactForReindex(makeSameUuidSpec(uuid, false,
          "file:///data/tmp/warcs/artifacts.warc?offset=7", 1330125997952L)
          .getArtifact(), policy);

      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      Artifact art = idxdb.getArtifact(uuid);
      assertNotNull(msg, art);
      assertTrue(msg + ": committed must not be downgraded true -> false",
          art.isCommitted());
      assertEquals(msg + ": permanent storage URL must not be clobbered",
          "file:///data/permanent/artifacts.warc?offset=100",
          art.getStorageUrl());

      idxdb.deleteArtifact(uuid);
    }
  }

  /**
   * ANTI-REGRESSION FOR THE MOST IMPORTANT DETAIL OF THE GUARD, as narrowed by
   * Component D.4.
   *
   * <p>The crawl_time comparison is {@code <=} / {@code >=}, NOT {@code <} /
   * {@code >}: for one and the same artifact both records carry the same
   * collectionDate, so a strict comparison would refuse every same-artifact
   * update. Refreshing storage_url on an equal date is the documented purpose
   * of the reindex upsert (commit e8e22774, "Reindex updates existing storage
   * URL and/or committed if present").
   *
   * <p>D.4 narrowed WHEN that refresh happens: on an equal date it happens only
   * if the digests DIFFER. This test is the surviving half of the original
   * {@code testReindexEqualCrawlTimeStillRefreshesStorageUrl} -- same assertion,
   * with the fixture changed so the incoming digest differs. If anyone ever
   * "tightens" the crawl_time comparison to a strict one, storage-URL refresh
   * stops working even for changed content, and this test is what catches it.
   *
   * <p>Its counterpart is
   * {@link #testReindexEqualCrawlTimeEqualDigestDoesNotChangeStorageUrl()}.
   */
  @Test
  public void testReindexEqualCrawlTimeDifferingDigestRefreshesStorageUrl()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();
      long crawlTime = 1330125997952L;

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, true, "old_storage_url", crawlTime, "digest_a")
              .getArtifact());

      // Same artifact, same collection date, DIFFERENT bytes, new WARC.
      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(uuid, true, "new_storage_url", crawlTime, "digest_b")
              .getArtifact(),
          policy);

      Artifact art = idxdb.getArtifact(uuid);
      assertEquals(msg + ": equal crawl_time with a DIFFERING digest MUST still"
              + " refresh storage_url",
          "new_storage_url", art.getStorageUrl());
      assertTrue(msg, art.isCommitted());
      assertEquals(msg, crawlTime, art.getCollectionDate());
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      idxdb.deleteArtifact(uuid);
    }
  }

  /**
   * THE COMPONENT D.4 RULE, in its smallest form.
   *
   * <p>The other half of the original
   * {@code testReindexEqualCrawlTimeStillRefreshesStorageUrl}, whose expectation
   * D.4 inverts. Before D.4 an equal crawl_time refreshed storage_url
   * unconditionally; now it does so only when the digests differ. Equal date +
   * equal digest is "the same bytes, presented from somewhere else", and this
   * layer cannot tell a better somewhere-else from a worse one, so it keeps
   * what it has.
   *
   * <p>Nothing else about the row may move either -- committed and crawl_time
   * are in the same SET list, so if the guard let the statement through they
   * would be rewritten too.
   */
  @Test
  public void testReindexEqualCrawlTimeEqualDigestDoesNotChangeStorageUrl()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();
      long crawlTime = 1330125997952L;

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, true, "old_storage_url", crawlTime, "same_digest")
              .getArtifact());

      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(uuid, true, "new_storage_url", crawlTime, "same_digest")
              .getArtifact(),
          policy);

      Artifact art = idxdb.getArtifact(uuid);
      assertEquals(msg + ": equal crawl_time AND equal digest must NOT change"
              + " storage_url",
          "old_storage_url", art.getStorageUrl());
      assertTrue(msg, art.isCommitted());
      assertEquals(msg, crawlTime, art.getCollectionDate());
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      idxdb.deleteArtifact(uuid);
    }
  }

  /**
   * PreferEarliest: a strictly later incoming collection date does not
   * overwrite the row at all; a strictly earlier one does, and carries
   * crawl_time with it rather than leaving the row half-applied.
   */
  @Test
  public void testReindexSameUuidPreferEarliest() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    SQLArtifactIndex.VersionConflictResolution policy =
        SQLArtifactIndex.VersionConflictResolution.PreferEarliest;

    String uuid = UUID.randomUUID().toString();
    idxdb.addArtifact(
        makeSameUuidSpec(uuid, true, "original_url", 5000L).getArtifact());

    // Strictly later: blocked, and blocked wholly.
    idxdb.upsertArtifactForReindex(
        makeSameUuidSpec(uuid, true, "later_url", 6000L).getArtifact(), policy);

    Artifact art = idxdb.getArtifact(uuid);
    assertEquals("later crawl_time must not overwrite under PreferEarliest",
        "original_url", art.getStorageUrl());
    assertEquals(5000L, art.getCollectionDate());

    // Strictly earlier: applied, including crawl_time.
    idxdb.upsertArtifactForReindex(
        makeSameUuidSpec(uuid, true, "earlier_url", 4000L).getArtifact(), policy);

    art = idxdb.getArtifact(uuid);
    assertEquals("earlier_url", art.getStorageUrl());
    assertEquals("crawl_time must be updated too, not left stale",
        4000L, art.getCollectionDate());
    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
  }

  /**
   * PreferLatest: the mirror image of
   * {@link #testReindexSameUuidPreferEarliest()}.
   */
  @Test
  public void testReindexSameUuidPreferLatest() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    SQLArtifactIndex.VersionConflictResolution policy =
        SQLArtifactIndex.VersionConflictResolution.PreferLatest;

    String uuid = UUID.randomUUID().toString();
    idxdb.addArtifact(
        makeSameUuidSpec(uuid, true, "original_url", 5000L).getArtifact());

    // Strictly earlier: blocked, and blocked wholly.
    idxdb.upsertArtifactForReindex(
        makeSameUuidSpec(uuid, true, "earlier_url", 4000L).getArtifact(), policy);

    Artifact art = idxdb.getArtifact(uuid);
    assertEquals("earlier crawl_time must not overwrite under PreferLatest",
        "original_url", art.getStorageUrl());
    assertEquals(5000L, art.getCollectionDate());

    // Strictly later: applied, including crawl_time.
    idxdb.upsertArtifactForReindex(
        makeSameUuidSpec(uuid, true, "later_url", 6000L).getArtifact(), policy);

    art = idxdb.getArtifact(uuid);
    assertEquals("later_url", art.getStorageUrl());
    assertEquals("crawl_time must be updated too, not left stale",
        6000L, art.getCollectionDate());
    assertEquals(1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
  }

  /**
   * The guard is one-directional: committed false -> true is still applied.
   * Only the true -> false downgrade is refused.
   *
   * <p>Component D.4 note: the fixture's two crawl_times are EQUAL (5000) and
   * its two digests are EQUAL (the {@code makeSameUuidSpec} default), so the
   * only thing letting this update through the D.4 term is that term's
   * committed disjunct. That is deliberate and load-bearing -- this test is the
   * coverage for the disjunct, and it fails if the disjunct is dropped. Do not
   * "tidy" the fixture by giving the two specs different digests or dates.
   */
  @Test
  public void testReindexCommittedUpgradeStillAllowed() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, false, "uncommitted_url", 5000L).getArtifact());

      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(uuid, true, "committed_url", 5000L).getArtifact(),
          policy);

      Artifact art = idxdb.getArtifact(uuid);
      assertTrue(msg + ": false -> true must still be applied", art.isCommitted());
      assertEquals(msg, "committed_url", art.getStorageUrl());

      idxdb.deleteArtifact(uuid);
    }
  }

  /**
   * Proves the COALESCE on the existing row's committed column.
   *
   * <p>committed is nullable (BOOLEAN with no NOT NULL). Without the COALESCE,
   * a NULL makes {@code NOT (NULL AND ...)} evaluate to NULL, the ON CONFLICT
   * WHERE clause is not satisfied, and EVERY update against such a row is
   * silently skipped.
   *
   * <p>Component D.4 changed this fixture, not its expectation. The two specs
   * used to share the default digest; with equal crawl_times and no committed
   * upgrade (NULL -> false is not one), D.4's term would now block the update
   * for a reason that has nothing to do with the COALESCE under test. The
   * digests therefore differ, which satisfies D.4 outright and leaves the
   * COALESCE as the only thing that can block the statement.
   */
  @Test
  public void testReindexNullCommittedDoesNotBlockUpdate() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, true, "old_url", 5000L, "digest_a").getArtifact());
      setCommittedNull(uuid);
      assertNull(msg + ": fixture must actually hold SQL NULL",
          readCommittedRaw(uuid));

      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(uuid, false, "new_url", 5000L, "digest_b").getArtifact(),
          policy);

      // getArtifact() maps NULL to false, so assert on storage_url: the update
      // must have fired.
      assertEquals(msg + ": a NULL committed must not block the update",
          "new_url", idxdb.getArtifact(uuid).getStorageUrl());
      assertEquals(msg, Boolean.FALSE, readCommittedRaw(uuid));

      idxdb.deleteArtifact(uuid);
    }
  }

  //
  // Component D.3: committedness dominates the collection-date policy.
  //
  // An uncommitted artifact must never displace a committed one. An
  // uncommitted row is transient -- the artifact in the temporary WARC will
  // either be committed or the temporary WARC will be garbage-collected -- so
  // overwriting a committed artifact on its strength trades a permanent loss
  // for a situation that resolves itself.
  //
  // Committedness selects the WINNER only. Component D's convergence guarantee
  // is untouched and asserted throughout: exactly ONE row remains on the tuple
  // after every call, whatever the committed classes involved, because two rows
  // on one tuple are the non-determinism Component D exists to remove.
  //
  // A losing uncommitted incoming artifact is not inserted at all. Reindex
  // walks permanent WARCs before temporary ones, so the uncommitted temp-WARC
  // record arrives after the committed permanent one; inserting it would
  // re-create the duplicate just deleted, and D.2's ON CONFLICT guard cannot
  // stop that (it only sees the same-uuid path).
  //
  // Note for anyone reading these assertions: getArtifact(uuid) does NOT filter
  // on committed (GET_ARTIFACT_BY_UUID_QUERY has no committed predicate), and
  // countAllVersions() passes includeUncommitted=true, so both see uncommitted
  // rows.
  //

  /** An artifact at the fixed conflict tuple, under a fresh uuid. */
  private static ArtifactSpec makeConflictSpec(boolean committed,
                                               String storageUrl,
                                               long collectionDate) {
    return makeSameUuidSpec(UUID.randomUUID().toString(), committed,
                            storageUrl, collectionDate);
  }

  /** A crawl time the given policy strictly prefers over {@code base}. */
  private static long preferredOver(
      SQLArtifactIndex.VersionConflictResolution policy, long base) {
    return policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
        ? base - 1000L : base + 1000L;
  }

  /** A crawl time the given policy strictly rejects in favour of {@code base}. */
  private static long rejectedFor(
      SQLArtifactIndex.VersionConflictResolution policy, long base) {
    return policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
        ? base + 1000L : base - 1000L;
  }

  private static List<Artifact> conflictTupleRows(SQLArtifactIndexManagerSql idxdb)
      throws Exception {
    return toList(idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(
        CONFLICT_NS, CONFLICT_AUID, true));
  }

  private static int countCommitted(SQLArtifactIndexManagerSql idxdb)
      throws Exception {
    int found = 0;
    for (Artifact art : conflictTupleRows(idxdb)) {
      if (art.isCommitted()) {
        found++;
      }
    }
    return found;
  }

  /** Removes every row at the conflict tuple, committed or not. */
  private static void clearConflictTuple(SQLArtifactIndexManagerSql idxdb)
      throws Exception {
    for (Artifact art : conflictTupleRows(idxdb)) {
      idxdb.deleteArtifact(art.getUuid());
    }
    assertEquals(0, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
  }

  /**
   * D.3 case 1. A committed row already holds the tuple; the reindex presents a
   * DIFFERENT uuid, uncommitted, with a collection date the policy would
   * otherwise prefer. The committed row must survive untouched -- same
   * storage URL, same crawl time -- and the uncommitted candidate must not be
   * inserted.
   *
   * <p>Before D.3 the uncommitted candidate won on crawl_time and the committed
   * row was deleted.
   */
  @Test
  public void testUncommittedIncomingDoesNotDisplaceCommittedRow()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      ArtifactSpec committed =
          makeConflictSpec(true, "file:///data/permanent.warc?offset=100", 5000L);
      idxdb.addArtifact(committed.getArtifact());

      ArtifactSpec incoming = makeConflictSpec(false,
          "file:///data/tmp/warcs/tmp.warc?offset=7", preferredOver(policy, 5000L));
      idxdb.upsertArtifactForReindex(incoming.getArtifact(), policy);

      Artifact survivor = idxdb.getArtifact(committed.getArtifactUuid());
      assertNotNull(msg + ": the committed row must not be deleted", survivor);
      assertTrue(msg, survivor.isCommitted());
      assertEquals(msg + ": the committed row must keep its own storage URL",
          "file:///data/permanent.warc?offset=100", survivor.getStorageUrl());
      assertEquals(msg + ": the committed row must keep its own crawl time",
          5000L, survivor.getCollectionDate());

      assertNull(msg + ": an uncommitted candidate must not be inserted over a"
              + " committed row",
          idxdb.getArtifact(incoming.getArtifactUuid()));
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      clearConflictTuple(idxdb);
    }
  }

  /**
   * D.3 case 2. The mirror image: an uncommitted row holds the tuple and the
   * reindex presents a committed artifact under a different uuid, with a
   * collection date the policy would otherwise reject. Committedness dominates,
   * so the committed artifact wins and is inserted -- and the losing
   * uncommitted row is deleted like any other loser, leaving one row.
   *
   * <p>Before D.3 the incoming artifact lost on crawl_time and was not inserted
   * at all.
   */
  @Test
  public void testCommittedIncomingBeatsUncommittedRowWhateverTheCrawlTime()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      ArtifactSpec uncommitted = makeConflictSpec(false, "temp_url", 5000L);
      idxdb.addArtifact(uncommitted.getArtifact());

      ArtifactSpec incoming = makeConflictSpec(true, "permanent_url",
          rejectedFor(policy, 5000L));
      idxdb.upsertArtifactForReindex(incoming.getArtifact(), policy);

      Artifact inserted = idxdb.getArtifact(incoming.getArtifactUuid());
      assertNotNull(msg + ": a committed candidate must win whatever the"
              + " crawl time", inserted);
      assertTrue(msg, inserted.isCommitted());
      assertEquals(msg, "permanent_url", inserted.getStorageUrl());

      assertNull(msg + ": the losing uncommitted row must be deleted",
          idxdb.getArtifact(uncommitted.getArtifactUuid()));

      assertEquals(msg + ": exactly one row per tuple",
          1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertEquals(msg, 1, countCommitted(idxdb));

      clearConflictTuple(idxdb);
    }
  }

  /**
   * D.3 case 3, as specified. The incoming artifact is presented UNCOMMITTED --
   * WarcArtifactDataStore's temp-WARC UNKNOWN branch, taken when a journal
   * entry is missing -- but its own row in the index is already committed. Its
   * effective committed state is therefore committed, so an uncommitted
   * competitor with a policy-preferred crawl time cannot displace it.
   *
   * <p>Before D.3 the competitor won and {@code deleteArtifactRow(incomingUuid)}
   * removed the committed own row outright.
   */
  @Test
  public void testIncomingPresentedUncommittedButAlreadyCommittedIsNotDisplaced()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();

      idxdb.addArtifact(makeSameUuidSpec(uuid, true,
          "file:///data/permanent.warc?offset=100", 5000L).getArtifact());

      ArtifactSpec competitor = makeConflictSpec(false, "other_temp_url",
          preferredOver(policy, 5000L));
      idxdb.addArtifact(competitor.getArtifact());

      // Same uuid, re-encountered in a temp WARC with no journal entry.
      idxdb.upsertArtifactForReindex(makeSameUuidSpec(uuid, false,
          "file:///data/tmp/warcs/tmp.warc?offset=7", 5000L).getArtifact(),
          policy);

      Artifact own = idxdb.getArtifact(uuid);
      assertNotNull(msg + ": an already-committed artifact must not be deleted"
              + " because it was re-presented as uncommitted", own);
      assertTrue(msg + ": committed must not be downgraded true -> false",
          own.isCommitted());
      assertEquals(msg + ": permanent storage URL must not be clobbered",
          "file:///data/permanent.warc?offset=100", own.getStorageUrl());

      assertNull(msg + ": the losing uncommitted competitor is deleted",
          idxdb.getArtifact(competitor.getArtifactUuid()));

      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertEquals(msg, 1, countCommitted(idxdb));

      clearConflictTuple(idxdb);
    }
  }

  /**
   * D.3 case 3 again, with a COMMITTED competitor in the mix as well as an
   * uncommitted one, so that the effective-committed rule is exercised against
   * a real in-class arbitration rather than against an empty class.
   *
   * <p>Treating the incoming artifact as uncommitted -- ignoring that its own
   * row is committed -- puts it outside the winner's (committed) class, the
   * committed competitor wins, and the own row is deleted as a loser. That is
   * exactly the data loss D.2 refuses on the same-uuid path.
   *
   * <p>With the rule: the own row is arbitrated as committed, wins its class on
   * crawl_time, both competitors are deleted, and it keeps committed=true and
   * its permanent storage URL -- the D.2 guard refusing the downgrade that the
   * re-presentation would otherwise apply.
   */
  @Test
  public void testEffectiveCommittedStateComesFromTheOwnRowToo()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();

      idxdb.addArtifact(makeSameUuidSpec(uuid, true,
          "file:///data/permanent.warc?offset=100", 5000L).getArtifact());

      // Committed, but on the wrong side of the policy from 5000.
      ArtifactSpec committedRival = makeConflictSpec(true, "rival_url",
          rejectedFor(policy, 5000L));
      idxdb.addArtifact(committedRival.getArtifact());

      ArtifactSpec uncommittedRival = makeConflictSpec(false, "spare_temp_url",
          preferredOver(policy, 5000L));
      idxdb.addArtifact(uncommittedRival.getArtifact());

      // Presented uncommitted from a temp WARC with no journal entry.
      idxdb.upsertArtifactForReindex(makeSameUuidSpec(uuid, false,
          "file:///data/tmp/warcs/tmp.warc?offset=7", 5000L).getArtifact(),
          policy);

      Artifact own = idxdb.getArtifact(uuid);
      assertNotNull(msg + ": the own row is already committed and must be"
              + " arbitrated as committed, not deleted", own);
      assertTrue(msg, own.isCommitted());
      assertEquals(msg + ": the D.2 guard must still refuse the downgrade",
          "file:///data/permanent.warc?offset=100", own.getStorageUrl());

      assertNull(msg + ": the losing committed rival must be deleted",
          idxdb.getArtifact(committedRival.getArtifactUuid()));
      assertNull(msg + ": the losing uncommitted rival must be deleted too",
          idxdb.getArtifact(uncommittedRival.getArtifactUuid()));

      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertEquals(msg, 1, countCommitted(idxdb));

      clearConflictTuple(idxdb);
    }
  }

  @Test
  public void testTwoCommittedRowsArbitrateOnCrawlTime() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      ArtifactSpec earlier = makeConflictSpec(true, "earlier_url", 4000L);
      ArtifactSpec later = makeConflictSpec(true, "later_url", 6000L);
      idxdb.addArtifact(earlier.getArtifact());
      idxdb.addArtifact(later.getArtifact());

      ArtifactSpec winner =
          policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
              ? earlier : later;
      ArtifactSpec loser =
          policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
              ? later : earlier;

      // A committed incoming artifact that neither policy prefers over both.
      ArtifactSpec incoming = makeConflictSpec(true, "incoming_url", 5000L);
      idxdb.upsertArtifactForReindex(incoming.getArtifact(), policy);

      assertNotNull(msg + ": the policy's pick among the committed rows stays",
          idxdb.getArtifact(winner.getArtifactUuid()));
      assertNull(msg + ": the losing committed row is deleted",
          idxdb.getArtifact(loser.getArtifactUuid()));
      assertNull(msg + ": the incoming artifact lost and was not inserted",
          idxdb.getArtifact(incoming.getArtifactUuid()));

      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertEquals(msg, 1, countCommitted(idxdb));

      clearConflictTuple(idxdb);
    }
  }

 @Test
  public void testAllUncommittedRowsArbitrateOnCrawlTime() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      // (a) The incoming artifact wins: both existing rows are deleted.
      ArtifactSpec a = makeConflictSpec(false, "a_url", 4000L);
      ArtifactSpec b = makeConflictSpec(false, "b_url", 6000L);
      idxdb.addArtifact(a.getArtifact());
      idxdb.addArtifact(b.getArtifact());

      long winnerCrawl =
          policy == SQLArtifactIndex.VersionConflictResolution.PreferEarliest
              ? 1000L : 9000L;
      ArtifactSpec winner = makeConflictSpec(false, "winner_url", winnerCrawl);
      idxdb.upsertArtifactForReindex(winner.getArtifact(), policy);

      assertNotNull(msg, idxdb.getArtifact(winner.getArtifactUuid()));
      assertNull(msg, idxdb.getArtifact(a.getArtifactUuid()));
      assertNull(msg, idxdb.getArtifact(b.getArtifactUuid()));
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertEquals(msg, 0, countCommitted(idxdb));

      // (b) The incumbent wins: the incoming artifact's own row, left behind by
      // an earlier run, is deleted so that the tuple converges.
      String loserUuid = UUID.randomUUID().toString();
      long loserCrawl = rejectedFor(policy, winnerCrawl);
      idxdb.addArtifact(
          makeSameUuidSpec(loserUuid, false, "loser_url", loserCrawl).getArtifact());
      assertEquals(msg, 2, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(loserUuid, false, "loser_url", loserCrawl).getArtifact(),
          policy);

      assertNull(msg + ": a losing uncommitted own row is still deleted",
          idxdb.getArtifact(loserUuid));
      assertNotNull(msg, idxdb.getArtifact(winner.getArtifactUuid()));
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      clearConflictTuple(idxdb);
    }
  }

  /**
   * D.3 case 6. The mixed case: one committed row plus two uncommitted rows,
   * and a committed incoming artifact the policy prefers. Exactly one row
   * remains -- the incoming one -- with the losing committed row and BOTH
   * uncommitted rows deleted.
   */
  @Test
  public void testMixedTupleCollapsesToOneRow()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      ArtifactSpec committed = makeConflictSpec(true, "old_committed_url", 5000L);
      ArtifactSpec temp1 = makeConflictSpec(false, "temp1_url",
          preferredOver(policy, 5000L));
      ArtifactSpec temp2 = makeConflictSpec(false, "temp2_url",
          preferredOver(policy, preferredOver(policy, 5000L)));
      idxdb.addArtifact(committed.getArtifact());
      idxdb.addArtifact(temp1.getArtifact());
      idxdb.addArtifact(temp2.getArtifact());
      assertEquals(msg, 3, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      ArtifactSpec incoming = makeConflictSpec(true, "new_committed_url",
          preferredOver(policy, 5000L));
      idxdb.upsertArtifactForReindex(incoming.getArtifact(), policy);

      assertNotNull(msg, idxdb.getArtifact(incoming.getArtifactUuid()));
      assertNull(msg + ": the losing committed row is deleted",
          idxdb.getArtifact(committed.getArtifactUuid()));
      assertNull(msg + ": losing uncommitted rows are deleted too",
          idxdb.getArtifact(temp1.getArtifactUuid()));
      assertNull(msg + ": losing uncommitted rows are deleted too",
          idxdb.getArtifact(temp2.getArtifactUuid()));

      assertEquals(msg + ": exactly one row per tuple",
          1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      assertEquals(msg, 1, countCommitted(idxdb));

      clearConflictTuple(idxdb);
    }
  }

  /**
   * D.3 end to end, in the order a real reindex produces: the permanent WARC's
   * committed record first, then the temporary WARC's uncommitted record under
   * a DIFFERENT uuid and with a collection date the policy would prefer.
   *
   * <p>This is the case the "return false, do not insert" rule exists for. If
   * the losing uncommitted record were inserted rather than skipped, it would
   * re-create the duplicate the pass had just collapsed, and D.2's ON CONFLICT
   * guard could not intervene -- the uuids differ, so that guard never fires.
   *
   * <p>Run twice, because a recovery tool has to be stable: the second pass
   * must leave the same single row, same uuid, same storage URL, same crawl
   * time. No churn.
   */
  @Test
  public void testPermanentThenTempOrderingIsStableAcrossRuns() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;

      ArtifactSpec permanent =
          makeConflictSpec(true, "file:///data/permanent.warc?offset=100", 5000L);
      ArtifactSpec temp = makeConflictSpec(false,
          "file:///data/tmp/warcs/tmp.warc?offset=7", preferredOver(policy, 5000L));

      for (int pass = 1; pass <= 2; pass++) {
        String passMsg = msg + " pass " + pass;

        // WarcArtifactDataStore.reindexArtifacts walks permanent WARCs first.
        idxdb.upsertArtifactForReindex(permanent.getArtifact(), policy);
        idxdb.upsertArtifactForReindex(temp.getArtifact(), policy);

        assertEquals(passMsg + ": exactly one row per tuple",
            1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

        Artifact survivor = idxdb.getArtifact(permanent.getArtifactUuid());
        assertNotNull(passMsg + ": the committed permanent record must survive",
            survivor);
        assertTrue(passMsg, survivor.isCommitted());
        assertEquals(passMsg + ": storage URL must not churn",
            "file:///data/permanent.warc?offset=100", survivor.getStorageUrl());
        assertEquals(passMsg + ": crawl time must not churn",
            5000L, survivor.getCollectionDate());

        assertNull(passMsg + ": the uncommitted temp record must never be"
                + " inserted, or the duplicate comes straight back",
            idxdb.getArtifact(temp.getArtifactUuid()));
      }

      clearConflictTuple(idxdb);
    }
  }

  //
  // Component D.4: on an equal collection date, the incoming artifact
  // overwrites the existing row only if the digests differ (or it is a
  // committed upgrade).
  //
  // WarcArtifactDataStore.CopyArtifactTask copies a record into a permanent
  // WARC, then updates the INDEX (setStorageUrl + updateStorageUrl), and only
  // THEN writes the WarcArtifactState.COPIED journal entry. Crash between those
  // last two steps and the index holds the permanent storage URL while the temp
  // WARC's journal does not say COPIED. On restart the isCopied skip in
  // reindexArtifacts does not fire, the temp-WARC record is re-presented with
  // the SAME uuid, the SAME crawl_time and the SAME digest, and D.2's
  // non-strict crawl_time comparison let it through -- reverting storage_url to
  // the TEMPORARY WARC, which is the artifact becoming unreadable as soon as
  // that WARC is reclaimed.
  //
  // These tests exercise the SQL layer directly, like every other Component D
  // test in this class: they present the two records to upsertArtifactForReindex
  // in the order a reindex would, rather than driving
  // WarcArtifactDataStore.reindexArtifacts over real WARC files.
  //

  private static final String PERMANENT_URL =
      "file:///data/permanent/artifacts.warc?offset=100";
  private static final String TEMP_URL =
      "file:///data/tmp/warcs/artifacts.warc?offset=7";

  /**
   * THE REGRESSION COMPONENT D.4 EXISTS FOR.
   *
   * <p>Existing row: committed, permanent storage URL -- CopyArtifactTask got as
   * far as updating the index. Incoming: the temp-WARC re-presentation of the
   * very same record, same uuid, same crawl_time, same digest, temporary
   * storage URL. The permanent URL must survive.
   *
   * <p>Run for both values of the incoming {@code committed} flag, because they
   * are stopped by DIFFERENT guards and only one of them is new:
   *
   * <ul>
   *   <li>{@code committed=false} (journal entry missing, WarcArtifactDataStore's
   *       UNKNOWN branch) is stopped by D.2's never-downgrade-committed clause.
   *       It already worked.</li>
   *   <li>{@code committed=true} (the commit WAS recorded before the crash) is
   *       NOT stopped by that clause -- true -> true is no downgrade -- nor by
   *       the crawl_time comparison, which is non-strict. It is the case that
   *       actually needed D.4, and it is the case that fails when the D.4 term
   *       is neutered.</li>
   * </ul>
   */
  @Test
  public void testTempRepresentationDoesNotRevertPermanentStorageUrl()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {
      for (boolean incomingCommitted : new boolean[] {true, false}) {

        String msg = "policy " + policy + ", incoming committed="
            + incomingCommitted;
        String uuid = UUID.randomUUID().toString();
        long crawlTime = 1330125997952L;

        // CopyArtifactTask step 2 completed: the index already points at the
        // permanent WARC.
        idxdb.addArtifact(
            makeSameUuidSpec(uuid, true, PERMANENT_URL, crawlTime, "sha1:abc")
                .getArtifact());

        // Step 3 never ran, so reindex re-presents the temp-WARC record.
        // Same uuid, same crawl_time, same digest.
        idxdb.upsertArtifactForReindex(
            makeSameUuidSpec(uuid, incomingCommitted, TEMP_URL, crawlTime,
                             "sha1:abc").getArtifact(), policy);

        Artifact art = idxdb.getArtifact(uuid);
        assertNotNull(msg, art);
        assertEquals(msg + ": storage_url must NOT revert to the temporary WARC",
            PERMANENT_URL, art.getStorageUrl());
        assertTrue(msg + ": committed must not be downgraded", art.isCommitted());
        assertEquals(msg, crawlTime, art.getCollectionDate());
        assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

        idxdb.deleteArtifact(uuid);
      }
    }
  }

  /**
   * D.4 does not block a real change: an equal collection date with DIFFERING
   * digests still overwrites, under either policy.
   *
   * <p>The same rule from the storage-URL angle is
   * {@link #testReindexEqualCrawlTimeDifferingDigestRefreshesStorageUrl()};
   * this one additionally pins that the whole SET list is applied.
   */
  @Test
  public void testEqualCrawlTimeDifferingDigestStillOverwrites()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, true, "old_url", 5000L, "sha1:old").getArtifact());

      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(uuid, true, "new_url", 5000L, "sha1:new").getArtifact(),
          policy);

      Artifact art = idxdb.getArtifact(uuid);
      assertEquals(msg + ": a differing digest on an equal date must overwrite",
          "new_url", art.getStorageUrl());
      assertTrue(msg, art.isCommitted());
      assertEquals(msg, 5000L, art.getCollectionDate());
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      idxdb.deleteArtifact(uuid);
    }
  }

 @Test
  public void testPreferredCrawlTimeOverwritesEvenWithEqualDigests()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();
      long preferred = preferredOver(policy, 5000L);

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, true, "old_url", 5000L, "sha1:same").getArtifact());

      idxdb.upsertArtifactForReindex(
          makeSameUuidSpec(uuid, true, "new_url", preferred, "sha1:same")
              .getArtifact(),
          policy);

      Artifact art = idxdb.getArtifact(uuid);
      assertEquals(msg + ": a strictly preferred date must overwrite whatever"
              + " the digest", "new_url", art.getStorageUrl());
      assertEquals(msg + ": crawl_time must be repaired too",
          preferred, art.getCollectionDate());
      assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

      idxdb.deleteArtifact(uuid);
    }
  }

  /**
   * D.4 loosens nothing: a strictly policy-REJECTED collection date is still
   * blocked outright, whether or not the digests differ. The three D.4
   * disjuncts are ANDed with the crawl_time comparison, not ORed into it.
   */
  @Test
  public void testRejectedCrawlTimeStillBlockedWhateverTheDigest()
      throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {
      for (String incomingDigest : new String[] {"sha1:same", "sha1:other"}) {

        String msg = "policy " + policy + ", incoming digest " + incomingDigest;
        String uuid = UUID.randomUUID().toString();
        long rejected = rejectedFor(policy, 5000L);

        idxdb.addArtifact(
            makeSameUuidSpec(uuid, true, "old_url", 5000L, "sha1:same")
                .getArtifact());

        idxdb.upsertArtifactForReindex(
            makeSameUuidSpec(uuid, true, "new_url", rejected, incomingDigest)
                .getArtifact(),
            policy);

        Artifact art = idxdb.getArtifact(uuid);
        assertEquals(msg + ": a strictly worse date must stay blocked",
            "old_url", art.getStorageUrl());
        assertEquals(msg, 5000L, art.getCollectionDate());
        assertEquals(msg, 1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));

        idxdb.deleteArtifact(uuid);
      }
    }
  }

  /**
   * A recovery tool must be stable: replaying the winning resolution leaves the
   * row exactly as it was. Permanent record first, then the temp-WARC
   * re-presentation -- the order reindexArtifacts walks them -- twice over, with
   * the whole row read back each pass.
   *
   * <p>D.4 makes this cheaper than it was: on the second pass the permanent
   * record's own re-presentation is now a no-op at the SQL level too, since its
   * date and digest match the row it would rewrite.
   */
  @Test
  public void testD4ResolutionIsIdempotent() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    for (SQLArtifactIndex.VersionConflictResolution policy :
             SQLArtifactIndex.VersionConflictResolution.values()) {

      String msg = "policy " + policy;
      String uuid = UUID.randomUUID().toString();
      long crawlTime = 1330125997952L;

      idxdb.addArtifact(
          makeSameUuidSpec(uuid, true, PERMANENT_URL, crawlTime, "sha1:abc")
              .getArtifact());

      for (int pass = 1; pass <= 2; pass++) {
        String passMsg = msg + " pass " + pass;

        idxdb.upsertArtifactForReindex(
            makeSameUuidSpec(uuid, true, PERMANENT_URL, crawlTime, "sha1:abc")
                .getArtifact(), policy);
        idxdb.upsertArtifactForReindex(
            makeSameUuidSpec(uuid, true, TEMP_URL, crawlTime, "sha1:abc")
                .getArtifact(), policy);

        Artifact art = idxdb.getArtifact(uuid);
        assertNotNull(passMsg, art);
        assertEquals(passMsg + ": storage URL must not churn",
            PERMANENT_URL, art.getStorageUrl());
        assertTrue(passMsg, art.isCommitted());
        assertEquals(passMsg + ": crawl time must not churn",
            crawlTime, art.getCollectionDate());
        assertEquals(passMsg + ": exactly one row per tuple",
            1, countAllVersions(idxdb, CONFLICT_NS, CONFLICT_AUID));
      }

      idxdb.deleteArtifact(uuid);
    }
  }

  //
  // E.2: a `return` inside the `finally` block used to discard any DbException
  // thrown in the try block and return 0, which callers could not tell from
  // "no rows matched".
  //

  @Test
  public void testUpdateStorageUrlPropagatesDbException() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = makeConflictSpec(1234L);
    idxdb.addArtifact(spec.getArtifact());

    // Break the database so getConnection() inside the try block fails.
    idxDbManager.stopService();
    idxDbManager = null;

    try {
      idxdb.updateStorageUrl(spec.getArtifactUuid(), "new_storage_url");
      fail("updateStorageUrl() should have thrown DbException");
    } catch (DbException expected) {
      // Expected: the failure must not be reported as "0 rows updated".
    }
  }

  @Test
  public void testDeleteArtifactPropagatesDbException() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpec spec = makeConflictSpec(1234L);
    idxdb.addArtifact(spec.getArtifact());

    idxDbManager.stopService();
    idxDbManager = null;

    try {
      idxdb.deleteArtifact(spec.getArtifactUuid());
      fail("deleteArtifact() should have thrown DbException");
    } catch (DbException expected) {
      // Expected: the failure must not be reported as "0 rows deleted".
    }
  }

  //
  // E.3: upsertArtifactsForReindex() batching. The per-artifact isolation
  // guarantee from R7 (42ecccfa, "give both reindex paths per-item
  // isolation") must survive batching: one bad artifact in the middle of a
  // buffered batch must not lose the batch's other, good artifacts, whether
  // the batch is tiny or a large fraction of ARTIFACT_INSERT_BATCH_SIZE.
  //

  /**
   * An artifact whose storage URL is longer than
   * {@code SqlConstants.MAX_ARTIFACT_STORAGE_URL_COLUMN} (1024 chars), which
   * throws a {@code SQLException} (string data right truncation) deep inside
   * {@code upsertArtifactForReindex()}'s {@code executeUpdate()} -- caught
   * there and rethrown as a {@code DbException}, the same shape any other
   * per-artifact DB-level failure takes on the reindex path (42ecccfa,
   * "give both reindex paths per-item isolation").
   *
   * <p>{@code Artifact}'s constructor validates a null version at
   * construction time (rejecting the more literal repro of the historical
   * incident), so this is the reliable way to make a single artifact in a
   * batch fail deep enough to exercise the batch's isolation, rather than
   * fail before it is even a valid {@code Artifact}.
   */
  private static ArtifactSpec makeBadArtifactSpec(String namespace, String auid, String url) {
    return new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(namespace)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(1)
        .setStorageUrl(URI.create(new String(new char[2000]).replace('\0', 'x')))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCommitted(true)
        .setCollectionDate(1L);
  }

  /** {@code count} artifacts under a shared namespace/AUID, each at a
   *  distinct URL and version 1, so none conflicts with any other. */
  private static List<Artifact> makeGoodReindexArtifacts(String namespace, String auid,
      String urlPrefix, int count) {
    List<Artifact> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(namespace)
          .setAuid(auid)
          .setUrl(urlPrefix + i)
          .setVersion(1)
          .setStorageUrl(URI.create("storage_url_" + i))
          .setContentLength(1)
          .setContentDigest("digest_" + i)
          .setCommitted(true)
          .setCollectionDate(1000L + i);
      result.add(spec.getArtifact());
    }
    return result;
  }

  /**
   * A small batch (5 good artifacts + 1 with a null version in the middle):
   * the 5 good ones must be indexed and exactly 1 failure reported.
   */
  @Test
  public void testUpsertArtifactsForReindexIsolatesBadArtifactSmallBatch() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "isolation-small-ns";
    String auid = "isolation-small-auid";

    List<Artifact> good = makeGoodReindexArtifacts(ns, auid, "http://example.com/small/", 5);

    List<Artifact> batch = new ArrayList<>();
    batch.addAll(good.subList(0, 3));
    batch.add(makeBadArtifactSpec(ns, auid, "http://example.com/small/bad").getArtifact());
    batch.addAll(good.subList(3, 5));

    SQLArtifactIndexManagerSql.ReindexUpsertOutcome outcome =
        idxdb.upsertArtifactsForReindex(batch,
            SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(6, outcome.getAttempted());
    assertEquals(1, outcome.getFailed());
    assertEquals(batch.get(0), outcome.getFirstArtifact());
    assertEquals(5, countAllVersions(idxdb, ns, auid));

    for (Artifact a : good) {
      assertNotNull("good artifact must survive the batch: " + a.getUri(),
          idxdb.getArtifact(a.getUuid()));
    }
  }

  /**
   * A single batch bigger than the tiny case, but still under
   * ARTIFACT_INSERT_BATCH_SIZE (1000): ~50 artifacts with one bad artifact at
   * roughly the midpoint of the SAME optimistic-attempt batch. Proves the
   * replay-with-a-fresh-connection path actually recovers a partial-batch
   * failure inside a single aborted transaction -- a naive implementation
   * could lose the good artifacts that "looked" successful but were never
   * committed before the batch aborted.
   */
  @Test
  public void testUpsertArtifactsForReindexIsolatesBadArtifactLargerBatch() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "isolation-large-ns";
    String auid = "isolation-large-auid";

    int goodCount = 49;
    List<Artifact> good = makeGoodReindexArtifacts(ns, auid, "http://example.com/large/", goodCount);

    List<Artifact> batch = new ArrayList<>();
    batch.addAll(good.subList(0, 25));
    batch.add(makeBadArtifactSpec(ns, auid, "http://example.com/large/bad").getArtifact());
    batch.addAll(good.subList(25, goodCount));

    assertTrue("batch must be well under ARTIFACT_INSERT_BATCH_SIZE (1000) " +
        "to stay in a single optimistic attempt", batch.size() < 1000);

    SQLArtifactIndexManagerSql.ReindexUpsertOutcome outcome =
        idxdb.upsertArtifactsForReindex(batch,
            SQLArtifactIndex.VersionConflictResolution.PreferEarliest);

    assertEquals(goodCount + 1, outcome.getAttempted());
    assertEquals(1, outcome.getFailed());
    assertEquals(goodCount, countAllVersions(idxdb, ns, auid));

    for (Artifact a : good) {
      assertNotNull("good artifact must survive replay after the optimistic " +
          "attempt aborts: " + a.getUri(), idxdb.getArtifact(a.getUuid()));
    }
  }

  //
  // E.3: benchmark. Real numbers, not the earlier analytical 3.5x-9x guess,
  // for the claim that batching commits is the single biggest win for
  // large-AU reindex (au-reindex-recovery-plan.md, Component E.3).
  //

  /**
   * Best-effort: set {@code synchronous_commit = on} as this test database's
   * default, correcting for zonky {@code EmbeddedPostgres}'s builder default
   * of {@code synchronous_commit = off} (set for test-suite speed, not
   * representativeness -- see {@code EmbeddedPostgres.Builder}'s constructor).
   * Without this correction the OLD per-artifact-commit path does not pay the
   * WAL-fsync-per-commit cost it pays in production, so the measured speedup
   * UNDERSTATES the real one -- it becomes a floor, not an estimate.
   *
   * <p>Executed as {@code ALTER DATABASE ... SET synchronous_commit = on} on a
   * throwaway raw JDBC connection to this test's database, before the
   * connection pool ({@code idxDbManager}) has opened any connections to it,
   * so every pooled connection used by the benchmark below inherits the
   * corrected default.
   *
   * @return true if the setting was applied, false if it could not be (in
   *         which case the benchmark still runs, but the caller must report
   *         the ratio as a floor on an even weaker basis).
   */
  private boolean trySetSynchronousCommitOn() {
    String url = "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName;
    try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
         Statement st = conn.createStatement()) {
      st.execute("ALTER DATABASE \"" + dbName + "\" SET synchronous_commit = on");
      return true;
    } catch (SQLException e) {
      log.warning("Could not set synchronous_commit = on for the benchmark; " +
          "the measured speedup will understate production further than " +
          "already noted", e);
      return false;
    }
  }

  /**
   * Reads PostgreSQL's own count of committed transactions for this test's
   * database, from {@code pg_stat_database.xact_commit}. This is the
   * objective, hard-to-fake evidence for what the benchmark below claims:
   * not "the wall clock says X ms", which proves nothing to a reader and
   * could in principle be noise, but "the database itself recorded this many
   * COMMITs" -- a number that is mechanically tied to how many transactions
   * {@code upsertArtifactForReindex} vs. {@code upsertArtifactsForReindex}
   * actually opened, independent of timing.
   *
   * <p>A raw connection is used (like {@link #trySetSynchronousCommitOn()})
   * rather than one from {@code idxdb}, so reading this counter is not
   * itself counted as one of the transactions it reports on.
   */
  private long readCommittedTransactionCount() throws SQLException {
    String url = "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName;
    try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
         PreparedStatement ps = conn.prepareStatement(
             "SELECT xact_commit FROM pg_stat_database WHERE datname = ?")) {
      ps.setString(1, dbName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("pg_stat_database has no row for " + dbName, rs.next());
        return rs.getLong(1);
      }
    }
  }

  /**
   * Measures OLD (one connection checkout + one COMMIT per artifact, via the
   * single-artifact {@code upsertArtifactForReindex}) against NEW (batched
   * {@code upsertArtifactsForReindex}) on two disjoint sets of the same size
   * on the same embedded PostgreSQL instance, and logs the wall-clock timings
   * and the computed speedup ratio.
   *
   * <p>Distinct (namespace, AUID, URL, version) tuples in both sets --
   * version 1 always, a unique URL per artifact -- so neither set exercises
   * Component D's version-conflict SELECT; this isolates the commit-batching
   * effect being measured from conflict-resolution cost.
   *
   * <p><b>What is actually being compared</b> -- both paths upsert the same
   * number of artifacts through the same underlying per-row logic
   * ({@code upsertArtifactForReindex(Connection, ...)}, unchanged by E.3);
   * the only difference is how many transactions -- and, since each of those
   * is also a fresh physical connection, how many connections -- that work is
   * divided into:
   * <ul>
   *   <li>OLD calls the single-artifact {@code upsertArtifactForReindex(
   *       Artifact, policy)} once per artifact, each call opening its own
   *       connection and ending in its own commit -- {@code total}
   *       connections and commits.</li>
   *   <li>NEW calls the batched {@code upsertArtifactsForReindex(Iterable,
   *       policy)} once, which internally buffers {@code batchSize} artifacts
   *       per held connection and commit -- {@code total / batchSize}
   *       connections and commits.</li>
   * </ul>
   * That difference is confirmed directly, not inferred from timing: this
   * test reads PostgreSQL's own {@code pg_stat_database.xact_commit} counter
   * before and after each phase and asserts the delta matches those expected
   * connection/commit counts. The wall-clock timing and speedup ratio are
   * reported for context, but the commit-count assertions are the actual
   * evidence that the two paths execute differently.
   *
   * <p>The measured commit count is consistently about <b>3x</b> the naive
   * "one commit per artifact" expectation for OLD (e.g. ~36,000 for 12,000
   * artifacts, not ~12,000) -- confirmed, by a throwaway diagnostic that
   * opened and closed {@code idxDbManager.getConnection()} connections with
   * zero application statements on them, to be connection-open overhead: on
   * this harness, roughly 2 commits happen per physical connection before any
   * application statement runs (driver/session setup), on top of the one
   * explicit application-level commit. This does not change what the test
   * demonstrates -- it means OLD's per-artifact connection churn is paying
   * for even more transactions than its own explicit commit implies, and
   * NEW's batching eliminates that per-connection overhead too, not just the
   * explicit commit. The generous {@code * 9L / 10} and {@code * 3L} slop in
   * the assertions below is not about this factor (which is a completely
   * consistent ~3x here); it is headroom for background activity on the
   * shared embedded instance across machines and PostgreSQL versions.
   */
  @Test
  public void testReindexUpsertBatchingBenchmark() throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    boolean correctedSyncCommit = trySetSynchronousCommitOn();

    // 12 batches of 1000: inside the 10-20 batch / 10,000-20,000 artifact
    // range called for, while keeping wall-clock time for the OLD
    // (per-commit) path reasonable under a real (non-tmpfs) WAL fsync.
    final int batchSize = 1000; // must match ARTIFACT_INSERT_BATCH_SIZE
    final int numBatches = 12;
    final int total = batchSize * numBatches;

    List<Artifact> oldSet =
        makeGoodReindexArtifacts("bench-old-ns", "bench-old-auid",
            "http://example.com/bench-old/", total);
    List<Artifact> newSet =
        makeGoodReindexArtifacts("bench-new-ns", "bench-new-auid",
            "http://example.com/bench-new/", total);

    long commitsBeforeOld = readCommittedTransactionCount();
    long oldStartNanos = System.nanoTime();
    for (Artifact artifact : oldSet) {
      idxdb.upsertArtifactForReindex(artifact,
          SQLArtifactIndex.VersionConflictResolution.PreferEarliest);
    }
    long oldMs = (System.nanoTime() - oldStartNanos) / 1_000_000L;
    long oldCommits = readCommittedTransactionCount() - commitsBeforeOld;

    long commitsBeforeNew = readCommittedTransactionCount();
    long newStartNanos = System.nanoTime();
    SQLArtifactIndexManagerSql.ReindexUpsertOutcome outcome =
        idxdb.upsertArtifactsForReindex(newSet,
            SQLArtifactIndex.VersionConflictResolution.PreferEarliest);
    long newMs = (System.nanoTime() - newStartNanos) / 1_000_000L;
    long newCommits = readCommittedTransactionCount() - commitsBeforeNew;

    assertEquals(total, outcome.getAttempted());
    assertEquals(0, outcome.getFailed());
    assertEquals(total, countAllVersions(idxdb, "bench-old-ns", "bench-old-auid"));
    assertEquals(total, countAllVersions(idxdb, "bench-new-ns", "bench-new-auid"));

    double speedup = newMs == 0 ? Double.POSITIVE_INFINITY : oldMs / (double) newMs;

    log.info("Reindex upsert benchmark: " + total + " artifacts/set, batch size "
        + batchSize + ", synchronous_commit corrected to ON: " + correctedSyncCommit
        + " -- OLD (per-artifact commit) = " + oldMs + " ms, " + oldCommits
        + " commits; NEW (batched) = " + newMs + " ms, " + newCommits
        + " commits -- speedup = " + speedup + "x");

    // The hard evidence: OLD must commit roughly once per artifact (allow
    // slack for other backends/autovacuum on the shared embedded instance,
    // but it must be an order of magnitude more than NEW's). NEW must commit
    // roughly once per batch, not once per artifact -- this is what would
    // fail if upsertArtifactsForReindex silently degenerated back into a
    // per-artifact-commit loop.
    assertTrue("OLD should commit close to once per artifact (" + total
        + "); actually committed " + oldCommits + " times",
        oldCommits >= total * 9L / 10);
    assertTrue("NEW should commit close to once per batch (~" + numBatches
        + "); actually committed " + newCommits + " times -- if this is "
        + "anywhere near " + total + ", batching is not happening",
        newCommits <= numBatches * 3L);
    assertTrue("NEW must commit far fewer times than OLD: OLD=" + oldCommits
        + ", NEW=" + newCommits, newCommits * 20L < oldCommits);

    // Weak but meaningful: batching must be faster, without pinning a
    // specific ratio (embedded-Postgres timing varies by machine).
    assertTrue("batched upsert (" + newMs + " ms) should be faster than " +
        "per-artifact-commit upsert (" + oldMs + " ms)", newMs < oldMs);
  }
}
