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
import org.lockss.util.rest.repo.model.ArtifactVersions;
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
  public void runAddArtifactMetric() throws Exception {
    initializePostgreSQL(false);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 100, 100);

    runMetric(specs, "addArtifact()", (spec) ->
        idxdb.addArtifact(spec.getArtifact()));
  }

  @Test
  public void runAddArtifactsMetric() throws Exception {
    initializePostgreSQL(false);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 100, 100);

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

  @Test
  public void runCommitArtifactMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "commitArtifact()", (spec) ->
        idxdb.commitArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void runDeleteArtifactMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "deleteArtifact()", (spec) ->
        idxdb.deleteArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void runUpdateStorageUrlMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "updateStorageUrl()", (spec) ->
        idxdb.updateStorageUrl(spec.getArtifactUuid(), "XXX"));
  }

  @Test
  public void runFindLatestArtifactsOfAllUrlsWithNamespaceAndAuidMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(false)", (spec) ->
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), false));

    runMetric(specs1, "findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(true)", (spec) ->
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), true));
  }

  @Test
  public void runGetArtifactByUuidMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getArtifactByUuid()", (spec) ->
      idxdb.getArtifact(spec.getArtifactUuid()));
  }

  @Test
  public void runGetArtifactByTupleMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getArtifactByTuple(false)", (spec) ->
      idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), false));

    runMetric(specs, "getArtifactByTuple(true)", (spec) ->
        idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true));
  }

  @Test
  public void runGetLatestArtifactMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getLatestArtifact(false)", (spec) ->
        idxdb.getLatestArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), false));

    runMetric(specs, "getLatestArtifact(true)", (spec) ->
      idxdb.getLatestArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), true));
  }

  @Test
  public void runGetNamespacesMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getNamespaces()", (spec) ->
      idxdb.getNamespaces());
  }

  @Test
  public void runFindAuidsMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findAuids()", (spec) ->
      idxdb.findAuids(spec.getNamespace()));
  }

  @Test
  public void runFindArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuidMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid()", (spec) ->
      idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl()));
  }

  @Test
  public void runFindArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuidMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid()", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl()));
  }

  @Test
  public void runFindArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ALL)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.ALL));

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(LATEST)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.LATEST));
  }

  @Test
  public void runFindArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace() throws Exception {


    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ALL)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.ALL));

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(LATEST)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.LATEST));
  }

  @Test
  public void runFindArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuidMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid()", (spec) ->
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl()));
  }

  @Test
  public void runFindArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuidMetric() throws Exception {
    initializePostgreSQL(true);
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(false)", (spec) ->
          idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), false));

    runMetric(specs, "findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(true)", (spec) ->
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), true));
  }


  interface ArtifactSpecRunnable {
    void run(ArtifactSpec spec) throws Exception;
  }

  private void runMetric(Iterable<ArtifactSpec> args, String funcSig, ArtifactSpecRunnable runnable) throws Exception {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    long count = 0;
    long start = TimeBase.nowMs();

    for (ArtifactSpec spec : args) {
      runnable.run(spec);

      if (count++ % 1000 == 999) {
        long end = TimeBase.nowMs();
        float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);
        float msPerCall = ((float) (end - start)) / (float) count;

        log.info(funcSig + ": " + count + " iterations in " + (end - start) + " ms; " + artifactsPerSecond + " iters/sec; " + msPerCall + " ms/iter");
      }
    }

    // Display latest metrics if not previously displayed
    if (!((count - 1) % 1000 == 999)) {
      long end = TimeBase.nowMs();
      float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);
      float msPerCall = ((float) (end - start)) / (float) count;

      log.info(funcSig + ": " + count + " iterations in " + (end - start) + " ms; " + artifactsPerSecond + " iters/sec; " + msPerCall + " ms/iter");
    }
  }
}
