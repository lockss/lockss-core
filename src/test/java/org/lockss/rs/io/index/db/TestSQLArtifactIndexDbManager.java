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
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.VersionsEnum;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;

import java.net.URI;
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

    // Initialize the DbManager with a unique database for this test
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(4);
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
    idxdb.upsertArtifactForReindex(spec.getArtifact());

    Artifact art = idxdb.getArtifact(spec.getArtifactUuid());
    // Assert against artifact spec
    spec.assertArtifactCommon(art);
    assertFalse(art.isCommitted());

    idxdb.upsertArtifactForReindex(spec.getArtifact());
    art = idxdb.getArtifact(spec.getArtifactUuid());
    spec.assertArtifactCommon(art);
    assertFalse(art.isCommitted());

    // Change storage URL & committed
    spec.setStorageUrl(URI.create("updated"));
    spec.setCommitted(true);
    idxdb.upsertArtifactForReindex(spec.getArtifact());
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
}
