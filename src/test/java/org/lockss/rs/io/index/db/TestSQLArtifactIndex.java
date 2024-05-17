package org.lockss.rs.io.index.db;

import org.junit.Test;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.repository.RepositoryManagerSql;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.UUID;

public class TestSQLArtifactIndex extends LockssTestCase4 {
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
    ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
        dbPort);
  }

  @Override
  public void tearDown() throws Exception {
    if (repositoryDbManager != null)
      repositoryDbManager.stopService();

    theDaemon.stopDaemon();
    super.tearDown();
  }

  private void initializeDatabase() throws Exception {
    // Set the database log.
    System.setProperty("derby.stream.error.file",
        new File(tempDirPath, "derby.log").getAbsolutePath());

    // Create the database manager.
    repositoryDbManager = new RepositoryDbManager();
    repositoryDbManager.initService(theDaemon);
    repositoryDbManager.startService();

    theDaemon.setRepositoryDbManager(repositoryDbManager);
  }

  @Test
  public void testAddArtifact() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("test")
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Add artifact
    repodb.addArtifact(spec.getArtifact());

    // Assert against artifact spec
    spec.assertArtifactCommon(repodb.getArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void testGetArtifact() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("test")
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    // Sanity check / enforce contract
    assertNull(repodb.getArtifact(spec.getArtifactUuid()));
    assertNull(repodb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion()));

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    Artifact byUuid = repodb.getArtifact(spec.getArtifactUuid());
    Artifact byTuple = repodb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion());

    spec.assertArtifactCommon(byUuid);
    spec.assertArtifactCommon(byTuple);
  }

  @Test
  public void testGetLatestArtifact() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

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
    assertNull(repodb.getLatestArtifact(ns, auid, url, false));
    assertNull(repodb.getLatestArtifact(ns, auid, url, true));

    // Add artifacts to database
    repodb.addArtifact(spec1.getArtifact());
    repodb.addArtifact(spec2.getArtifact());

    // Assert no max committed artifact but max uncommitted is v2
    assertNull(repodb.getLatestArtifact(ns, auid, url, false));
    spec2.assertArtifactCommon(repodb.getLatestArtifact(ns, auid, url, true));

    // Commit v1 artifact
    repodb.commitArtifact(spec1.getArtifactUuid());
    spec1.setCommitted(true);

    // Assert latest committed is v1 and latest uncommitted remains v2
    spec1.assertArtifactCommon(repodb.getLatestArtifact(ns, auid, url, false));
    spec2.assertArtifactCommon(repodb.getLatestArtifact(ns, auid, url, true));

    // Commit v2 artifact
    repodb.commitArtifact(spec2.getArtifactUuid());
    spec2.setCommitted(true);

    // Assert latest committed and latest uncommitted is v2
    spec2.assertArtifactCommon(repodb.getLatestArtifact(ns, auid, url, false));
    spec2.assertArtifactCommon(repodb.getLatestArtifact(ns, auid, url, true));
  }

  @Test
  public void testCommitArtifact() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url_1"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    // Assert artifact not committed
    Artifact pre = repodb.getArtifact(spec.getArtifactUuid());
    assertFalse(pre.isCommitted());

    // Commit artifact
    repodb.commitArtifact(spec.getArtifactUuid());

    // Assert artifact now committed
    Artifact post = repodb.getArtifact(spec.getArtifactUuid());
    assertTrue(post.isCommitted());
  }

  @Test
  public void testUpdateStorageUrl() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url_1"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    // Assert URL pre-update
    Artifact pre = repodb.getArtifact(spec.getArtifactUuid());
    assertEquals("storage_url_1", pre.getStorageUrl());

    // Update URL
    repodb.updateStorageUrl(spec.getArtifactUuid(), "storage_url_2");

    // Assert URL pre-update
    Artifact post = repodb.getArtifact(spec.getArtifactUuid());
    assertEquals("storage_url_2", post.getStorageUrl());
  }

  @Test
  public void testDeleteArtifact() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setUrl("url")
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(2);

    // Sanity check
    assertNull(repodb.getArtifact(spec.getArtifactUuid()));

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    // Assert artifact pre-delete
    Artifact pre = repodb.getArtifact(spec.getArtifactUuid());
    spec.assertArtifactCommon(pre);

    // Delete artifact
    repodb.deleteArtifact(spec.getArtifactUuid());

    // Assert null post-delete
    assertNull(repodb.getArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void testGetNamespaces() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

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
    assertEmpty(repodb.getNamespaces());

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    assertIterableEquals(List.of(ns), repodb.getNamespaces());
  }

  @Test
  public void testFindAuids() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

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
    assertEmpty(repodb.findAuids(ns));

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    assertIterableEquals(List.of(auid), repodb.findAuids(ns));
  }

  @Test
  public void testFindLatestArtifactsOfAllUrlsWithNamespaceAndAuid() throws Exception {
    initializeDatabase();
    RepositoryManagerSql repodb = new RepositoryManagerSql(repositoryDbManager);

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
    assertEmpty(repodb.findAuids(ns));

    // Add artifact to database
    repodb.addArtifact(spec.getArtifact());

    assertNull(repodb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));
    List<Artifact> result = repodb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);
    log.info("result = " + result);
  }
}
