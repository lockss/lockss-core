package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.lockss.db.DbException;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.lockss.util.io.ZipUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

public class TestSQLArtifactIndexMetrics extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private SQLArtifactIndexDbManager idxDbManager;
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
    if (idxDbManager != null)
      idxDbManager.stopService();

    theDaemon.stopDaemon();
    super.tearDown();
  }

  protected void initializePostgreSQL(boolean usePopulatedDb) throws Exception {
    int port = startEmbeddedPostgreSQL(usePopulatedDb);

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

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PORTNUMBER, String.valueOf(port));

    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(getMockLockssDaemon());

    idxDbManager.setTargetDatabaseVersion(3);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  protected int startEmbeddedPostgreSQL(boolean usePopulatedDb) throws DbException {
    try {
      EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();

      String extemp = System.getProperty("org.lockss.executableTempDir");
      if (!StringUtil.isNullString(extemp)) {
        builder.setOverrideWorkingDirectory(new File(extemp));
      }

      if (usePopulatedDb) {
        File dataDir = unzipTestDatabase(tempDirPath);
        if (dataDir != null) {
          builder.setDataDirectory(dataDir);
          builder.setCleanDataDirectory(false);
        }
      }

      log.debug("Starting embedded PostgreSQL");
      EmbeddedPostgres embeddedPg = builder.start();
      return embeddedPg.getPort();

    } catch (IOException e) {
      throw new DbException("Can't start embedded PostgreSQL", e);
    }
  }

  private static final String TEST_DB_FILE_SPEC = "/org/lockss/db/test-sqlindex-db.zip";

  protected File unzipTestDatabase(String tempDirPath) {
    try {
      // Extract the database from the zip file, if it exists.
      InputStream dbzip = getResourceAsStream(TEST_DB_FILE_SPEC, false);
      if (dbzip != null) {
        log.info("Unzipping pre-built database files from " +
            TEST_DB_FILE_SPEC);
        File dstDir = new File(tempDirPath, "db");
        ZipUtil.unzip(dbzip, dstDir);

        // Fix permissions u=rwx (0700)
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        Files.setPosixFilePermissions(dstDir.toPath(), perms);

        return dstDir;
      }
    } catch (Exception e) {
      log.debug("Unable to unzip database files from file " + TEST_DB_FILE_SPEC,
          e);
    }
    return null;
  }


  public static class ArtifactSpecGeneratorBuilder {

  }


  public static class ArtifactSpecGenerator implements Iterable<ArtifactSpec> {
    int maxNamespaces;
    int maxAuids;
    int maxUrls;

    public ArtifactSpecGenerator(int namespaces, int auids, int urls) {
      this.maxNamespaces = namespaces;
      this.maxAuids = auids;
      this.maxUrls = urls;
    }

    @NotNull
    @Override
    public Iterator<ArtifactSpec> iterator() {
      return new Iterator<ArtifactSpec>() {
        int nsCount = 0;
        int auidCount = 0;
        int urlCount = 0;
        int totalCount = 0;

        @Override
        public boolean hasNext() {
          return totalCount < (maxNamespaces * maxAuids * maxUrls);
        }

        @Override
        public ArtifactSpec next() {

          String ns = "ns" + nsCount;
          String auid = "auid" + auidCount;
          String url = "url" + urlCount;

          if (++urlCount >= maxUrls) {
            urlCount = 0;
            if (++auidCount >= maxAuids) {
              auidCount = 0;
              nsCount++;
            }
          }

          totalCount++;

          return new ArtifactSpec()
              .setArtifactUuid(ns + ":" + auid + ":" + url)
              .setNamespace(ns)
              .setAuid(auid)
              .setUrl(url)
              .setVersion(1)
              .setStorageUrl(URI.create("test"))
              .setContentLength(1024)
              .setContentDigest("My Digest")
              .setCollectionDate(1234L);
        }
      };
    }
  }

  @Test
  @Disabled
  public void testNamespace() throws Exception {
    initializePostgreSQL(false);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    String ns = "ns0";
    String auid = "auid0";
    String url = "url0";

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(1)
        .setStorageUrl(URI.create("test"))
        .setContentLength(1024)
        .setContentDigest("My Digest")
        .setCollectionDate(1234L);

    idxdb.addArtifact(spec.getArtifact());
    log.info("ns = " + idxdb.getNamespaces());
    idxdb.deleteArtifact(spec.getArtifactUuid());
    log.info("ns = " + idxdb.getNamespaces());
  }

  @Test
  public void runNonDestructiveMetrics() throws Exception {
    initializePostgreSQL(true);

    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(1, 10, 100);

    getArtifactByUuidMetric(specs1);
    getArtifactByTupleMetric(specs1);
    getLatestArtifactMetric(specs1);
    getNamespacesMetric(specs1);

    findAuidsMetric(specs1);
    findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuidMetric(specs1);
    findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuidMetric(specs1);
  }

  @Test
  public void runDestructiveMetrics() throws Exception {
    initializePostgreSQL(false);

    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 100, 100);

    ArtifactSpecGenerator specs2 =
        new ArtifactSpecGenerator(10, 100, 100);

    addArtifactMetric(specs1);
    commitArtifactMetric(specs1);
    updateStorageUrlMetrics(specs1);
    deleteArtifactMetric(specs1);

    addArtifactsMetric(specs2);
  }

  private void addArtifactMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.addArtifact(spec.getArtifact());
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("addArtifacts(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void addArtifactsMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    List<Artifact> artifacts = new ArrayList<>();
    for (ArtifactSpec spec : specs) {
      artifacts.add(spec.getArtifact());
      count++;
      if (count % 1000 == 999) {
        idxdb.addArtifacts(artifacts);
        artifacts.clear();
      }
    }
    idxdb.addArtifacts(artifacts);
    artifacts.clear();

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("addArtifacts(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void commitArtifactMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.commitArtifact(spec.getArtifactUuid());
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("commitArtifact(uuid): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void deleteArtifactMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.deleteArtifact(spec.getArtifactUuid());
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("deleteArtifact(uuid): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void getArtifactByUuidMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.getArtifact(spec.getArtifactUuid());
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("getArtifact(uuid): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void getArtifactByTupleMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true);
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("getArtifact(tuple): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void getLatestArtifactMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.getLatestArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), true);
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("getLatestArtifact(tuple): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void getNamespacesMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (int i = 0; i < 1000; i++) {
      idxdb.getNamespaces();
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("getNamespaces(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void updateStorageUrlMetrics(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.updateStorageUrl(spec.getArtifactUuid(), "XXX");
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("updateStorageUrl(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private void findAuidsMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    Iterator<ArtifactSpec> specIterator = specs.iterator();

    for (int i = 0; i < 1000; i++) {
      idxdb.findAuids(specIterator.next().getNamespace());
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);
    float msPerCall = ((float) (end - start)) / (float) count;

    log.info("findAuids(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec; " + msPerCall + " ms/call");
  }


  private void findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuidMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl());
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);
    float msPerCall = ((float) (end - start)) / (float) count;

    log.info("findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec; " + msPerCall + " ms/call");
  }

  private void findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuidMetric(Iterable<ArtifactSpec> specs) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : specs) {
      List<Artifact> result =
          idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), false);
      count++;
    }

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);
    float msPerCall = ((float) (end - start)) / (float) count;

    log.info("findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(): " + count + " calls in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec; " + msPerCall + " ms/call");
  }
}
