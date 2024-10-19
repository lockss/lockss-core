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

import org.junit.Test;
import org.lockss.db.DbException;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class TestSQLArtifactIndexDbManager extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private SQLArtifactIndexDbManager idxDbManager;
  private String dbPort;

  // FIXME: Refactor tests to use JUnit test lifecycle annotations

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Get the temporary directory used during the test.
    tempDirPath = setUpDiskSpace();

    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);
    dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
    ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
        dbPort);
  }

  @Override
  public void tearDown() throws Exception {
    if (idxDbManager != null)
      idxDbManager.stopService();

    theDaemon.stopDaemon();
    super.tearDown();
  }

  protected void initializePostgreSQL() throws Exception {
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgresx");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.enabled", "true",
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.initialSize", "2");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, PGSimpleDataSource.class.getCanonicalName(),
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    idxDbManager = new SQLArtifactIndexDbManager();
    startEmbeddedPgDbManager(idxDbManager);
    idxDbManager.initService(getMockLockssDaemon());

    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private void initializeDerby() throws IOException {
    // Set the database log.
    System.setProperty("derby.stream.error.file",
        new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create the database manager.
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);

    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private void initializeDatabase() throws Exception {
    initializePostgreSQL();
  }

  @Test
  public void testAddArtifact() throws Exception {
    initializeDatabase();
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
  public void testGetArtifact() throws Exception {
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, ArtifactVersions.ALL));
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, ArtifactVersions.LATEST));

    // Commit artifacts
    commitSpecs(idxdb, specs, 0, 3, 4, 5);

    // Assert with ALL
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, ArtifactVersions.ALL);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(idxdb, specs, 1);

    // Assert with LATEST
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns1, url1, ArtifactVersions.LATEST);
      assertIterableEquals(expected, result);
    }
  }

  @Test
  public void testFindArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace() throws Exception {
    initializeDatabase();
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
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, ArtifactVersions.ALL));
    assertEmpty(idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, ArtifactVersions.LATEST));

    // Commit artifacts
    commitSpecs(idxdb, specs, 0, 3, 4, 5);

    // Assert with ALL
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[0], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, ArtifactVersions.ALL);
      assertIterableEquals(expected, result);
    }

    // Commit artifacts
    commitSpecs(idxdb, specs, 1);

    // Assert with LATEST
    {
      List<Artifact> expected = getArtifactsFromSpecs(specs[1], specs[3]);
      Iterable<Artifact> result =
          idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns1, url1, ArtifactVersions.LATEST);
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
    initializeDatabase();
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
    initializeDatabase();
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
    initializeDatabase();
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

    assertEquals(0, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.ALL));
    assertEquals(0, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.LATEST));

    commitSpecs(idxdb, specs, 0, 2);
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.ALL));
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.LATEST));

    commitSpecs(idxdb, specs, 1);
    assertEquals(3, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.ALL));
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.LATEST));

    deleteSpecs(idxdb, specs, 0);
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.ALL));
    assertEquals(2, idxdb.getSizeOfArtifacts(ns1, auid1, ArtifactVersions.LATEST));
  }
}
