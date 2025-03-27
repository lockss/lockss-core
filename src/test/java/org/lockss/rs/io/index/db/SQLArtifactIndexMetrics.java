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
import org.apache.commons.io.FileUtils;
import org.lockss.config.ConfigManager;
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
import org.lockss.util.os.PlatformUtil;
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
import org.junit.Test;

public class SQLArtifactIndexMetrics extends LockssTestCase {
  private static L4JLogger log = L4JLogger.getLogger();

  private static final File DEFAULT_SRC_DATA_DIR =
      new File("metricsdb.zip");

  private File srcBaseDir = DEFAULT_SRC_DATA_DIR;
  private File nonDestructiveBaseDir = null;
  private File currentBaseDir;
  private String tmpDirPath;
  private String dbPort;

  private MockLockssDaemon theDaemon;
  private SQLArtifactIndexDbManager idxDbManager;
  private SQLArtifactIndexManagerSql idxdb;
  private EmbeddedPostgres embeddedPg;

  // FIXME: IntelliJ scans this class for tests (because it extends LockssTestCase?)
  //  and complains loudly when it doesn't find one. Avoid that with a no-op for now:
  @Test
  public void testNothing() throws Exception {
    // Intentionally left blank
  }

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
        } else if (arg.equals("-t") || arg.equals("--tmpdir")) {
          metricsRunner.setTmpDirPath(argv[++ix]);
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
    // this.nonDestructiveBaseDir = this.srcBaseDir;
  }

  private void setTmpDirPath(String tmpDirPath) {
    this.tmpDirPath = tmpDirPath;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  private @interface Metric {
    String value() default "";
    boolean usePopulatedDb() default true;
    boolean isDestructive() default false;
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
    setUp();
    for (Map.Entry<Metric, Method> entry : getMetricMethodMap().entrySet()) {
      runDatabaseMetric(entry.getKey(), entry.getValue());
    }
    tearDown();
  }

  private void runMetric(String name) throws Exception {
    Map<Metric, Method> mmm = getMetricMethodMap();

    Metric m = mmm.keySet()
        .stream()
        .filter(metric -> metric.value().equals(name))
        .findFirst()
        .orElse(null);

    Method mm = mmm.get(m);

    setUp();
    runDatabaseMetric(m, mm);
    tearDown();
  }

  private void runDatabaseMetric(Metric m, Method mm) throws Exception {
    if (mm == null) {
      throw new MetricMethodNotFoundException();
    }

    initializePostgreSQL(m.usePopulatedDb(), m.isDestructive());
    idxdb = new SQLArtifactIndexManagerSql(idxDbManager);
    mm.invoke(this);
    embeddedPg.close();

    if (m.isDestructive() && currentBaseDir != null) {
      FileUtil.delTree(currentBaseDir);
    }
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
    o.println("  -t, --tmpdir <path>  Temporary directory to use for metrics");
    System.exit(2);
  }

  private class MetricMethodNotFoundException extends Exception {
    // Intentionally left blank
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    if (!StringUtil.isNullString(tmpDirPath)) {
      System.setProperty(PlatformUtil.SYSPROP_LOCKSS_TMPDIR, tmpDirPath);
      // Use addFromArgs?
      ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
          tmpDirPath);
    }

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
    FileUtil.delTree(new File(tmpDirPath));

    if (idxDbManager != null)
      idxDbManager.stopService();

    theDaemon.stopDaemon();

    super.tearDown();
  }

  @Override
  protected boolean wantTempTmpDir() {
    return true;
  }

  private void initializePostgreSQL(boolean usePopulatedDb, boolean isDestructive) throws Exception {
    int port = startEmbeddedPostgreSQL(usePopulatedDb, isDestructive);

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

    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private int startEmbeddedPostgreSQL(boolean usePopulatedDb, boolean isDestructive) throws DbException {
    try {
      EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();

      String extemp = System.getProperty("org.lockss.executableTempDir");
      if (!StringUtil.isNullString(extemp)) {
        currentBaseDir = new File(extemp);
      } else {
        currentBaseDir = getTempDir("pgsqldb");
      }

      builder.setOverrideWorkingDirectory(currentBaseDir);

      if (usePopulatedDb) {
        if (srcBaseDir.exists()) {
          File dstBaseDir = nonDestructiveBaseDir;

          if (isDestructive || nonDestructiveBaseDir == null) {
            log.info("Copying PostgreSQL data");

            dstBaseDir = currentBaseDir;

            if (srcBaseDir.isDirectory()) {
              FileUtils.copyDirectory(srcBaseDir, dstBaseDir);
            } else if (srcBaseDir.isFile()) {
              unzipTestDatabase(srcBaseDir, dstBaseDir);
            }

            if (!isDestructive) {
              nonDestructiveBaseDir = dstBaseDir;
            }
          }

          File dstDataDir = new File(dstBaseDir, "db");
          fixDataDirectoryPermissions(dstDataDir);

          builder.setOverrideWorkingDirectory(dstBaseDir);
          builder.setDataDirectory(dstDataDir);
          builder.setCleanDataDirectory(false);
        } else {
          log.error("Could not find PostgreSQL data ({}); using empty DB!", srcBaseDir);
          throw new IOException("Missing PostgreSQL data");
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

  @Metric(value = "addArtifact", usePopulatedDb = false, isDestructive = true)
  public void runAddArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 100, 100);

    runMetric(specs, "addArtifact()", (spec) ->
        idxdb.addArtifact(spec.getArtifact()));
  }

  @Metric(value = "addArtifacts", usePopulatedDb = false, isDestructive = true)
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

  @Metric(value = "commitArtifact", isDestructive = true)
  public void runCommitArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs, "commitArtifact()", (spec) ->
        idxdb.commitArtifact(spec.getArtifactUuid()));
  }

  @Metric(value = "deleteArtifact", isDestructive = true)
  public void runDeleteArtifactMetric() throws Exception {
    ArtifactSpecGenerator specs1 =
        new ArtifactSpecGenerator(10, 10, 100);

    runMetric(specs1, "deleteArtifact()", (spec) ->
        idxdb.deleteArtifact(spec.getArtifactUuid()));
  }

  @Metric(value = "updateStorageUrl", isDestructive = true)
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
        new ArtifactSpecGenerator(1, 1, 100);

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

   @Metric("findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace") // REALLY SLOW!
  public void runFindArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespaceMetric() throws Exception {
    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(1, 1, 1);

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
        new ArtifactSpecGenerator(1, 1, 100);

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
