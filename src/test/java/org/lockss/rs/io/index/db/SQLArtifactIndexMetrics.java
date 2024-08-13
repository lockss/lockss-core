package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.commons.io.FileUtils;
import org.lockss.db.DbException;
import org.lockss.log.L4JLogger;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.ListUtil;
import org.lockss.util.StringUtil;
import org.lockss.util.io.FileUtil;
import org.lockss.util.io.ZipUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.*;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

public class SQLArtifactIndexMetrics extends LockssTestCase {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final File DEFAULT_SRC_DATA_DIR =
      new File("metricsdb.zip");

  private File srcBaseDir = DEFAULT_SRC_DATA_DIR;
  private String tmpDirPath;
  private String dbPort;

  private MockLockssDaemon theDaemon;
  private SQLArtifactIndexDbManager idxDbManager;
  private SQLArtifactIndexManagerSql idxdb;
  private EmbeddedPostgres embeddedPg;

  public static void main(String[] argv) throws Exception {
    SQLArtifactIndexMetrics metricsRunner = new SQLArtifactIndexMetrics();

    int ix = 0;
    String metricName = null;
    try {
      // Parse arguments
      for (ix = 0; ix < argv.length; ix++) {
        String arg = argv[ix];
        if (arg.equals("-d") || arg.equals("--data")) {
          metricsRunner.setSrcBaseDir(argv[++ix]);
        } else if (arg.equals("-m") || arg.equals("--metric")) {
          metricName = argv[++ix];
        } else {
          log.fatal("Illegal command line: {}", ListUtil.list(argv));
          usage();
        }
      }

      // Run metrics
      if (StringUtil.isNullString(metricName)) {
        metricsRunner.runAllMetrics();
      } else {
        metricsRunner.runMetric(metricName);
      }

      System.exit(0);
    } catch (Exception e) {
      log.fatal("Unexpected error, exiting.", e);
      System.exit(1);
    }
  }

  private void setSrcBaseDir(String srcBaseDir) {
    this.srcBaseDir = new File(srcBaseDir);
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  private static @interface Metric {
    public String value() default "";
    public boolean usePopulatedDb() default true;
  }

  private Map<Metric, Method> getMetricMethodMap() {
    Class<?> clazz = this.getClass();
    Map<Metric, Method> metricMethodMap = new HashMap<>();

    for (Method method : clazz.getMethods()) {
      if (method.isAnnotationPresent(Metric.class)) {
        Metric metricAnnotation = method.getAnnotation(Metric.class);
        metricMethodMap.put(metricAnnotation, method);
      }
    }

    return metricMethodMap;
  }

  private void runAllMetrics() throws Exception {
    for (Map.Entry<Metric, Method> entry : getMetricMethodMap().entrySet()) {
      runDatabaseMetric(entry.getKey(), entry.getValue());
    }
  }

  private void runMetric(String name) throws Exception {
    Map<Metric, Method> mmm = getMetricMethodMap();

    Metric m = mmm.keySet()
        .stream()
        .filter(metric -> metric.value().equals(name))
        .findFirst()
        .orElse(null);

    Method mm = mmm.get(m);

    runDatabaseMetric(m, mm);
 }

  private void runDatabaseMetric(Metric m, Method mm) throws Exception {
    if (mm == null) {
      throw new MetricMethodNotFoundException();
    }

    setUp();
    initializePostgreSQL(m.usePopulatedDb());
    idxdb = new SQLArtifactIndexManagerSql(idxDbManager);
    mm.invoke(this);
    tearDown();
  }

  private static void usage() {
    usage(null);
  }

  private static void usage(String msg) {
    PrintStream o = System.out;
    if (msg != null) {
      o.println(msg);
    }
    o.println("Usage: java SQLArtifactIndexMetrics" +
        " [-d|--data <path>]" +
        " [-m|--metric <path>] ...");
    o.println("  -d, --data <path>    Source PostgreSQL data directory (or zip file of one)");
    o.println("  -m, --metric <name>  Name of metric to run");
    System.exit(2);
  }

  private class MetricMethodNotFoundException extends Exception {
    // Intentionally left blank
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    // Get the temporary directory used during the test.
    tmpDirPath = setUpDiskSpace();

    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);
    dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
    ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
        dbPort);
  }

  @Override
  public void tearDown() throws Exception {
    // Temporary directories are cleaned up in tearDown() but this gives
    // the embedded PostgreSQL framework an opportunity to shutdown and
    // clean up gracefully:
    if (embeddedPg != null)
      embeddedPg.close();

    if (idxDbManager != null)
      idxDbManager.stopService();

    theDaemon.stopDaemon();

    super.tearDown();
  }

  private void initializePostgreSQL(boolean usePopulatedDb) throws Exception {
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

  private int startEmbeddedPostgreSQL(boolean usePopulatedDb) throws DbException {
    try {
      EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();

      String extemp = System.getProperty("org.lockss.executableTempDir");
      if (!StringUtil.isNullString(extemp)) {
        builder.setOverrideWorkingDirectory(new File(extemp));
      }

      if (usePopulatedDb) {
        if (srcBaseDir.exists()) {
          log.debug("Copying PostgreSQL data");

          File dstBaseDir = FileUtil.createTempDir("pgsqldb", null, new File(tmpDirPath));
          File dstDataDir = new File(dstBaseDir, "db");

          if (srcBaseDir.isDirectory()) {
            FileUtils.copyDirectory(srcBaseDir, dstBaseDir);
          } else if (srcBaseDir.isFile()) {
            unzipTestDatabase(srcBaseDir, dstBaseDir);
          }

          fixDataDirectoryPermissions(dstDataDir);

          builder.setOverrideWorkingDirectory(dstBaseDir);
          builder.setDataDirectory(dstDataDir);
          builder.setCleanDataDirectory(false);
        } else {
          log.warn("Could not find PostgreSQL data ({}); using empty DB!", srcBaseDir);
          // Throw?
        }
      }

      log.debug("Starting embedded PostgreSQL");
      embeddedPg = builder.start();
      return embeddedPg.getPort();
    } catch (IOException e) {
      throw new DbException("Can't start embedded PostgreSQL", e);
    }
  }

  // Permissions should be u=rwx (0700) or u=rwx,g=rx (0750).
  private static void fixDataDirectoryPermissions(File dataDir) throws IOException {
    Set<PosixFilePermission> perms = new HashSet<>();
    perms.add(PosixFilePermission.OWNER_READ);
    perms.add(PosixFilePermission.OWNER_WRITE);
    perms.add(PosixFilePermission.OWNER_EXECUTE);
    Files.setPosixFilePermissions(dataDir.toPath(), perms);
  }

  private static void unzipTestDatabase(File srcDataDirZipFile, File dstDataDir) throws IOException {
    // Extract the database from the zip file
    try (InputStream dbzip = new BufferedInputStream(new FileInputStream(srcDataDirZipFile))) {
      log.info("Unzipping pre-built database files from " + srcDataDirZipFile);
      ZipUtil.unzip(dbzip, dstDataDir);
    } catch (Exception e) {
      log.debug("Unable to unzip database files from file: " + srcDataDirZipFile, e);
      throw new IOException("Unable to unzip database", e);
    }
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

  @Metric(value = "addArtifact", usePopulatedDb = false)
  public void runAddArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 100, 100);

    runMetric(specs, "addArtifact()", (spec) ->
        idxdb.addArtifact(spec.getArtifact()));
  }

  @Metric(value = "addArtifacts", usePopulatedDb = false)
  public void runAddArtifactsMetric() throws Exception {
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

  @Metric("commitArtifact")
  public void runCommitArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "commitArtifact()", (spec) ->
        idxdb.commitArtifact(spec.getArtifactUuid()));
  }

  @Metric("deleteArtifact")
  public void runDeleteArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "deleteArtifact()", (spec) ->
        idxdb.deleteArtifact(spec.getArtifactUuid()));
  }

  @Metric("updateStorageUrl")
  public void runUpdateStorageUrlMetric() throws Exception {
    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "updateStorageUrl()", (spec) ->
        idxdb.updateStorageUrl(spec.getArtifactUuid(), "XXX"));
  }

  @Metric("findLatestArtifactsOfAllUrlsWithNamespaceAndAuid")
  public void runFindLatestArtifactsOfAllUrlsWithNamespaceAndAuidMetric() throws Exception {
    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(false)", (spec) ->
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), false));

    runMetric(specs1, "findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(true)", (spec) ->
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), true));
  }

  @Metric("getArtifactByUuid")
  public void runGetArtifactByUuidMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getArtifactByUuid()", (spec) ->
      idxdb.getArtifact(spec.getArtifactUuid()));
  }

  @Metric("getArtifactByTuple")
  public void runGetArtifactByTupleMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getArtifactByTuple(false)", (spec) ->
      idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), false));

    runMetric(specs, "getArtifactByTuple(true)", (spec) ->
        idxdb.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), spec.getVersion(), true));
  }

  @Metric("getLatestArtifact")
  public void runGetLatestArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getLatestArtifact(false)", (spec) ->
        idxdb.getLatestArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), false));

    runMetric(specs, "getLatestArtifact(true)", (spec) ->
      idxdb.getLatestArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl(), true));
  }

  @Metric("getNamespaces")
  public void runGetNamespacesMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "getNamespaces()", (spec) ->
      idxdb.getNamespaces());
  }

  @Metric("findAuids")
  public void runFindAuidsMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findAuids()", (spec) ->
      idxdb.findAuids(spec.getNamespace()));
  }

  @Metric("findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid")
  public void runFindArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuidMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid()", (spec) ->
      idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl()));
  }

  @Metric("findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid")
  public void runFindArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuidMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid()", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl()));
  }

  @Metric("findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace")
  public void runFindArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespaceMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ALL)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.ALL));

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(LATEST)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.LATEST));
  }

  @Metric("findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace")
  public void runFindArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespaceMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ALL)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.ALL));

    runMetric(specs, "findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(LATEST)", (spec) ->
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(spec.getNamespace(), spec.getUrl(), ArtifactVersions.LATEST));
  }

  @Metric("findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid")
  public void runFindArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuidMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid()", (spec) ->
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), spec.getUrl()));
  }

  @Metric("findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid")
  public void runFindArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuidMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(false)", (spec) ->
          idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), false));

    runMetric(specs, "findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(true)", (spec) ->
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(spec.getNamespace(), spec.getAuid(), true));
  }


  private static interface ArtifactSpecRunnable {
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
