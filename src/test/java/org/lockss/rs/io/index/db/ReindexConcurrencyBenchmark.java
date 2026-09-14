/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.Test;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Companion to the WARC-read/parse-side scaling benchmark being built independently
 * in {@code laaws-repository-tools}. That tool measures how WARC reading/parsing
 * scales with worker thread count; this class measures how the DB-write half of a
 * parallelized reindex scales instead -- the already-batched reindex commit path
 * ({@link SQLArtifactIndexManagerSql#upsertArtifactsForReindex(Iterable,
 * SQLArtifactIndex.VersionConflictResolution)}, added by E.3, see
 * {@code au-reindex-recovery-plan.md}) under CONCURRENT writers, i.e. multiple
 * threads each doing their own batched reindex upserts against the same database at
 * once. Together the two tell whether a parallelized production reindex would be
 * parse-bound or DB-bound at a given thread count.
 *
 * <p><b>What "concurrent writers" means here.</b> Each of K worker threads
 * independently calls {@code upsertArtifactsForReindex} once, on its own disjoint
 * slice of synthetic artifacts, against the SAME embedded PostgreSQL database
 * instance. Each thread gets its own {@code (namespace, auid)} (fixed namespace,
 * a thread/run-unique auid), so no two threads' artifacts ever share a
 * {@code (namespace, auid, url, version)} tuple -- there is no cross-thread
 * version-conflict-resolution contention to muddy the pure commit-concurrency
 * measurement. This models K reindex worker threads each processing a different
 * WARC/AU concurrently and periodically flushing a batch to the shared index.
 *
 * <p><b>Chunking choice.</b> Each thread calls {@code upsertArtifactsForReindex}
 * exactly ONCE with its entire slice (not split into several calls), rather than
 * re-implementing a "worker flush cadence" on top of it. The method's own internal
 * {@code ARTIFACT_INSERT_BATCH_SIZE} (1000) batching already does the flushing;
 * calling it once per thread per K-run is the simplest faithful model of "a worker
 * hands its whole current backlog to the index," and avoids conflating this
 * benchmark's own chunking decisions with the effect being measured.
 *
 * <p>This class is deliberately NOT named {@code Test*} so the default Surefire
 * discovery pattern skips it -- a full sweep (multiple thread counts x repetitions)
 * takes a while and has no business running as part of routine {@code mvn test}. It
 * embeds a JUnit4 no-op test only so IDEs that scan {@link LockssTestCase4}
 * subclasses for tests don't complain about finding none (see the same pattern in
 * {@code SQLArtifactIndexMetrics}). Run it directly:
 *
 * <pre>
 * java -cp &lt;test-classpath&gt; org.lockss.rs.io.index.db.ReindexConcurrencyBenchmark \
 *     [-k|--k-list 1,2,4,8,16,32] [-r|--repetitions 3] \
 *     [-n|--total-artifacts 60000] [-o|--out &lt;path&gt;]
 * </pre>
 *
 * <p>The embedded-PostgreSQL bootstrap (datasource class, connection params,
 * {@code synchronous_commit} correction) is duplicated from {@code
 * TestSQLArtifactIndexDbManager} rather than factored out of it, per instructions,
 * to avoid touching that test class's structure.
 */
public class ReindexConcurrencyBenchmark extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private static final String NAMESPACE = "reindexbench";

  private static final int[] DEFAULT_K_LIST = {1, 2, 4, 8, 16, 32};
  private static final int DEFAULT_REPETITIONS = 3;
  private static final int DEFAULT_TOTAL_ARTIFACTS = 60_000;

  private EmbeddedPostgres embeddedPg;
  private MockLockssDaemon theDaemon;
  private SQLArtifactIndexDbManager idxDbManager;
  private String dbName;

  /**
   * No-op test so IDEs that scan {@link LockssTestCase4} subclasses for JUnit
   * tests don't complain about finding none in a class that is otherwise a
   * standalone-main tool, not a test. Not run by {@code mvn test} because this
   * class's name does not match Surefire's default {@code Test*}/{@code *Test}
   * discovery pattern.
   */
  @Test
  public void testNothing() throws Exception {
    // Intentionally left blank.
  }

  // ***********************************************************************
  // * CLI ENTRY POINT
  // ***********************************************************************

  public static void main(String[] argv) throws Exception {
    int[] kList = DEFAULT_K_LIST;
    int repetitions = DEFAULT_REPETITIONS;
    int totalArtifacts = DEFAULT_TOTAL_ARTIFACTS;
    String outPath = null;

    try {
      for (int ix = 0; ix < argv.length; ix++) {
        String arg = argv[ix];
        switch (arg) {
          case "-k":
          case "--k-list":
            kList = parseIntList(argv[++ix]);
            break;
          case "-r":
          case "--repetitions":
            repetitions = Integer.parseInt(argv[++ix]);
            break;
          case "-n":
          case "--total-artifacts":
            totalArtifacts = Integer.parseInt(argv[++ix]);
            break;
          case "-o":
          case "--out":
            outPath = argv[++ix];
            break;
          default:
            usage("Unrecognized argument: " + arg);
        }
      }
    } catch (ArrayIndexOutOfBoundsException e) {
      usage("Missing value for last argument");
    }

    if (outPath == null) {
      usage("--out <path> is required");
    }

    // System.exit() below never runs a finally block, so tearDownBenchmark()
    // is called explicitly on every path BEFORE exiting -- not from a
    // finally -- so the embedded PostgreSQL instance and MockLockssDaemon are
    // always cleanly stopped rather than relying on zonky's JVM shutdown hook.
    ReindexConcurrencyBenchmark bench = new ReindexConcurrencyBenchmark();
    int exitCode;
    try {
      bench.setUpBenchmark();
      Report report = bench.runSweep(kList, repetitions, totalArtifacts);
      bench.writeReport(report, outPath);
      log.info("Wrote report to " + outPath);
      exitCode = 0;
    } catch (Exception e) {
      log.error("Benchmark failed", e);
      exitCode = 1;
    }
    bench.tearDownBenchmark();
    System.exit(exitCode);
  }

  private static void usage(String msg) {
    System.err.println(msg);
    System.err.println("Usage: java " + ReindexConcurrencyBenchmark.class.getName()
        + " [-k|--k-list 1,2,4,8,16,32] [-r|--repetitions 3]"
        + " [-n|--total-artifacts 60000] -o|--out <path>");
    System.exit(2);
  }

  private static int[] parseIntList(String csv) {
    String[] parts = csv.split(",");
    int[] result = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      result[i] = Integer.parseInt(parts[i].trim());
    }
    return result;
  }

  // ***********************************************************************
  // * EMBEDDED POSTGRES / DAEMON BOOTSTRAP
  // *
  // * Duplicated (not factored out of, and not modifying) the setUp() shape
  // * in TestSQLArtifactIndexDbManager, per instructions. This benchmark owns
  // * its own single EmbeddedPostgres instance/database for the whole sweep
  // * (all K values and repetitions run against the SAME database instance,
  // * matching the "concurrent writers against the same shared DB" model).
  // ***********************************************************************

  private void setUpBenchmark() throws Exception {
    super.setUp();
    setUpDiskSpace();
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();
    String extemp = System.getProperty("org.lockss.executableTempDir");
    if (!StringUtil.isNullString(extemp)) {
      builder.setOverrideWorkingDirectory(new File(extemp));
    }
    embeddedPg = builder.start();

    dbName = "bench_" + UUID.randomUUID().toString().replace("-", "");

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

    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(5);
    idxDbManager.startService();
    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private void tearDownBenchmark() {
    try {
      if (idxDbManager != null) {
        idxDbManager.stopService();
      }
    } catch (Exception e) {
      log.warning("Error stopping idxDbManager", e);
    }
    try {
      if (theDaemon != null) {
        theDaemon.stopDaemon();
      }
    } catch (Exception e) {
      log.warning("Error stopping daemon", e);
    }
    try {
      if (embeddedPg != null) {
        embeddedPg.close();
      }
    } catch (Exception e) {
      log.warning("Error closing embedded PostgreSQL", e);
    }
  }

  /**
   * Corrects zonky {@code EmbeddedPostgres}'s {@code synchronous_commit = off}
   * builder default to {@code on}, exactly as {@code
   * TestSQLArtifactIndexDbManager#trySetSynchronousCommitOn()} does -- without
   * this, the measured commit cost would be understated relative to production,
   * biasing the whole sweep toward looking better than it would in reality.
   * Run once, before the sweep starts (all runs share one database).
   */
  private boolean trySetSynchronousCommitOn() {
    String url = "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName;
    try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
         Statement st = conn.createStatement()) {
      st.execute("ALTER DATABASE \"" + dbName + "\" SET synchronous_commit = on");
      return true;
    } catch (SQLException e) {
      log.warning("Could not set synchronous_commit = on for the benchmark; " +
          "measured throughput will be optimistic relative to production", e);
      return false;
    }
  }

  /**
   * Truncates the artifact-index tables between repetitions so every
   * repetition (at every K) starts from the same empty-table state. Without
   * this, since the sweep runs many repetitions against one shared database
   * and K ascends over the sweep, later (higher-K) runs would accumulate
   * more rows -- and therefore bigger indexes -- than earlier (lower-K) runs,
   * systematically depressing measured speedup at high K for a reason having
   * nothing to do with concurrency. Safe here because {@link
   * SQLArtifactIndexManagerSql}'s namespace/AUID/URL sequence-number caches
   * are per-instance (not static/shared), and every repetition constructs a
   * fresh {@code SQLArtifactIndexManagerSql} per thread, so no thread can
   * hold a cached seq number that survives a truncate.
   */
  private void truncateArtifactTables() throws SQLException {
    String url = "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName;
    try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres")) {
      // long_urls does not exist at every schema version this benchmark might
      // run against (it wasn't present at v5 in this environment), so only
      // truncate the tables that actually exist rather than hardcoding the
      // full list and failing on a missing one.
      String[] candidateTables = {
          org.lockss.config.db.SqlConstants.ARTIFACT_TABLE,
          org.lockss.config.db.SqlConstants.URL_TABLE,
          org.lockss.config.db.SqlConstants.LONG_URL_TABLE,
          org.lockss.config.db.SqlConstants.AUID_TABLE,
          org.lockss.config.db.SqlConstants.NAMESPACE_TABLE,
      };

      List<String> existingTables = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(
          "SELECT 1 FROM information_schema.tables WHERE table_name = ?")) {
        for (String table : candidateTables) {
          ps.setString(1, table);
          try (java.sql.ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
              existingTables.add(table);
            }
          }
        }
      }

      try (Statement st = conn.createStatement()) {
        st.execute("TRUNCATE TABLE " + String.join(", ", existingTables)
            + " RESTART IDENTITY CASCADE");
      }
    }
  }

  private String readPostgresVersion() {
    String url = "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName;
    try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
         Statement st = conn.createStatement();
         java.sql.ResultSet rs = st.executeQuery("SELECT version()")) {
      if (rs.next()) {
        return rs.getString(1);
      }
    } catch (SQLException e) {
      log.warning("Could not read PostgreSQL version", e);
    }
    return "unknown";
  }

  // ***********************************************************************
  // * SYNTHETIC ARTIFACT GENERATION
  // ***********************************************************************

  /**
   * Generates {@code count} synthetic artifacts for one thread's slice, all
   * under the given (unique to this thread/run) {@code auid}, each at a
   * distinct URL and version 1 -- so no artifact generated by this call can
   * ever collide, on {@code (namespace, auid, url, version)}, with an
   * artifact generated for any other thread, repetition, or K value in the
   * same sweep (every caller passes a globally-unique {@code auid}).
   */
  private static List<Artifact> makeArtifacts(String auid, int count) {
    List<Artifact> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(NAMESPACE)
          .setAuid(auid)
          .setUrl("http://example.com/reindexbench/" + auid + "/" + i)
          .setVersion(1)
          .setStorageUrl(URI.create("storage_url_" + auid + "_" + i))
          .setContentLength(1)
          .setContentDigest("digest_" + auid + "_" + i)
          .setCommitted(true)
          .setCollectionDate(1000L + i);
      result.add(spec.getArtifact());
    }
    return result;
  }

  // ***********************************************************************
  // * SWEEP
  // ***********************************************************************

  private static final class ThreadOutcome {
    final long elapsedNanos;
    final int attempted;
    final int failed;

    ThreadOutcome(long elapsedNanos, int attempted, int failed) {
      this.elapsedNanos = elapsedNanos;
      this.attempted = attempted;
      this.failed = failed;
    }
  }

  /**
   * Runs one (K, repetition) combination: K threads, each holding its own
   * pre-generated disjoint artifact list, synchronized on a {@link
   * CyclicBarrier} so they all call {@code upsertArtifactsForReindex}
   * as close to simultaneously as the JVM's scheduler allows, then waits for
   * all of them to finish and reports wall-clock elapsed time plus the sum of
   * each thread's own time spent inside the call.
   *
   * @param k        Number of concurrent worker threads.
   * @param runLabel A label unique across the whole sweep (encodes K and
   *                 repetition index), used to build globally-unique auids.
   * @param totalArtifacts Total artifacts across all K threads this run.
   */
  private RepResult runOneRepetition(int k, String runLabel, int totalArtifacts)
      throws Exception {
    int perThread = totalArtifacts / k;
    int remainder = totalArtifacts - (perThread * k);

    // Pre-generate every thread's artifact list BEFORE starting the clock, so
    // synthetic-data generation cost is never counted as DB-commit time.
    List<List<Artifact>> perThreadArtifacts = new ArrayList<>(k);
    for (int t = 0; t < k; t++) {
      int count = perThread + (t == k - 1 ? remainder : 0);
      String auid = "conc-" + runLabel + "-t" + t;
      perThreadArtifacts.add(makeArtifacts(auid, count));
    }

    ExecutorService pool = Executors.newFixedThreadPool(k);
    AtomicLong wallStartNanos = new AtomicLong();
    CyclicBarrier barrier = new CyclicBarrier(k, () -> wallStartNanos.set(System.nanoTime()));

    List<Callable<ThreadOutcome>> tasks = new ArrayList<>(k);
    for (int t = 0; t < k; t++) {
      final List<Artifact> artifacts = perThreadArtifacts.get(t);
      tasks.add(() -> {
        SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);
        barrier.await();
        long start = System.nanoTime();
        SQLArtifactIndexManagerSql.ReindexUpsertOutcome outcome =
            idxdb.upsertArtifactsForReindex(artifacts,
                SQLArtifactIndex.VersionConflictResolution.PreferEarliest);
        long elapsed = System.nanoTime() - start;
        return new ThreadOutcome(elapsed, outcome.getAttempted(), outcome.getFailed());
      });
    }

    List<Future<ThreadOutcome>> futures = new ArrayList<>(k);
    for (Callable<ThreadOutcome> task : tasks) {
      futures.add(pool.submit(task));
    }

    long attempted = 0;
    long failed = 0;
    long sumThreadNanos = 0;
    try {
      for (Future<ThreadOutcome> f : futures) {
        ThreadOutcome outcome = f.get();
        attempted += outcome.attempted;
        failed += outcome.failed;
        sumThreadNanos += outcome.elapsedNanos;
      }
    } finally {
      pool.shutdown();
      pool.awaitTermination(5, TimeUnit.MINUTES);
    }

    long wallNanos = System.nanoTime() - wallStartNanos.get();

    RepResult rr = new RepResult();
    rr.wallClockMs = wallNanos / 1_000_000L;
    rr.aggregateThreadTimeMs = sumThreadNanos / 1_000_000L;
    rr.attempted = attempted;
    rr.failed = failed;
    return rr;
  }

  private Report runSweep(int[] kList, int repetitions, int totalArtifacts) {
    boolean correctedSyncCommit = trySetSynchronousCommitOn();
    String postgresVersion = readPostgresVersion();

    log.info("ReindexConcurrencyBenchmark starting: kList=" + Arrays.toString(kList)
        + ", repetitions=" + repetitions + ", totalArtifacts=" + totalArtifacts
        + ", synchronous_commit corrected to ON=" + correctedSyncCommit
        + ", postgresVersion=" + postgresVersion);

    Report report = new Report();
    report.timestamp = Instant.now().toString();
    report.jvm = System.getProperty("java.vendor") + " " + System.getProperty("java.version");
    report.os = System.getProperty("os.name") + " " + System.getProperty("os.version")
        + " " + System.getProperty("os.arch");
    report.postgresVersion = postgresVersion;
    report.synchronousCommitCorrectedToOn = correctedSyncCommit;
    report.params = new Params();
    report.params.kList = kList;
    report.params.repetitions = repetitions;
    report.params.totalArtifacts = totalArtifacts;

    Double baselineMedianMs = null;

    for (int k : kList) {
      List<RepResult> reps = new ArrayList<>(repetitions);
      for (int rep = 0; rep < repetitions; rep++) {
        String runLabel = "k" + k + "-rep" + rep;
        try {
          truncateArtifactTables();
          RepResult rr = runOneRepetition(k, runLabel, totalArtifacts);
          reps.add(rr);
          log.info("ReindexConcurrencyBenchmark [K=" + k + ", rep=" + rep + "]: "
              + "wallClock=" + rr.wallClockMs + "ms, aggregateThreadTime="
              + rr.aggregateThreadTimeMs + "ms, attempted=" + rr.attempted
              + ", failed=" + rr.failed);
        } catch (Exception e) {
          log.error("ReindexConcurrencyBenchmark [K=" + k + ", rep=" + rep
              + "] failed", e);
          throw new RuntimeException(e);
        }
      }

      long[] wallMs = reps.stream().mapToLong(r -> r.wallClockMs).toArray();
      long[] aggThreadMs = reps.stream().mapToLong(r -> r.aggregateThreadTimeMs).toArray();
      double medianWallMs = median(wallMs);
      double minWallMs = Arrays.stream(wallMs).min().orElse(0);
      double maxWallMs = Arrays.stream(wallMs).max().orElse(0);
      double medianAggThreadMs = median(aggThreadMs);

      double artifactsPerSecMedian = medianWallMs <= 0 ? Double.POSITIVE_INFINITY
          : totalArtifacts / (medianWallMs / 1000.0);

      if (baselineMedianMs == null) {
        baselineMedianMs = medianWallMs;
      }

      double speedupVsK1 = medianWallMs <= 0 ? Double.POSITIVE_INFINITY
          : baselineMedianMs / medianWallMs;
      double parallelEfficiency = speedupVsK1 / k;

      KResult kr = new KResult();
      kr.k = k;
      kr.wallClockMsMedian = medianWallMs;
      kr.wallClockMsMin = minWallMs;
      kr.wallClockMsMax = maxWallMs;
      kr.aggregateThreadTimeMsMedian = medianAggThreadMs;
      kr.artifactsPerSecMedian = artifactsPerSecMedian;
      kr.speedupVsK1 = speedupVsK1;
      kr.parallelEfficiency = parallelEfficiency;
      kr.repetitions = reps;
      report.results.add(kr);

      log.info("ReindexConcurrencyBenchmark SUMMARY [K=" + k + "]: "
          + "wallClockMsMedian=" + medianWallMs
          + " (min=" + minWallMs + ", max=" + maxWallMs + ")"
          + ", artifactsPerSecMedian=" + String.format("%.1f", artifactsPerSecMedian)
          + ", speedupVsK1=" + String.format("%.2f", speedupVsK1) + "x"
          + ", parallelEfficiency=" + String.format("%.2f", parallelEfficiency)
          + ", aggregateThreadTimeMsMedian=" + medianAggThreadMs
          + " (aggregate/wallClock ratio=" + String.format("%.2f",
              medianWallMs <= 0 ? 0 : medianAggThreadMs / medianWallMs) + ", ideal="
          + k + " if no contention)");
    }

    // Drift check: re-run the K=1 baseline once more, last, after every other
    // K has run (and after each repetition's table truncation). If this
    // disagrees materially with the original K=1 median, that is evidence of
    // machine-load drift (JIT warm-up, background activity, thermal
    // throttling) over the course of the sweep -- a confound the per-run
    // truncation above does not address, since it is about time, not table
    // state.
    try {
      truncateArtifactTables();
      RepResult driftRep = runOneRepetition(kList[0], "drift-recheck-k" + kList[0], totalArtifacts);
      report.k1DriftRecheckWallClockMs = driftRep.wallClockMs;
      double originalK1Median = report.results.get(0).wallClockMsMedian;
      log.info("ReindexConcurrencyBenchmark DRIFT CHECK: K=" + kList[0]
          + " re-run at end of sweep = " + driftRep.wallClockMs
          + "ms vs. original median = " + originalK1Median
          + "ms (ratio=" + String.format("%.2f",
              originalK1Median <= 0 ? 0 : driftRep.wallClockMs / originalK1Median) + ")");
    } catch (Exception e) {
      log.warning("Drift-check re-run of K=" + kList[0] + " failed; sweep results " +
          "above are unaffected, but no drift signal is available", e);
    }

    return report;
  }

  private static double median(long[] values) {
    long[] sorted = values.clone();
    Arrays.sort(sorted);
    int n = sorted.length;
    if (n == 0) {
      return 0;
    }
    if (n % 2 == 1) {
      return sorted[n / 2];
    }
    return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
  }

  private void writeReport(Report report, String outPath) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    mapper.writeValue(new File(outPath), report);
  }

  // ***********************************************************************
  // * REPORT SCHEMA (Jackson POJOs)
  // ***********************************************************************

  public static final class Params {
    public int[] kList;
    public int repetitions;
    public int totalArtifacts;
  }

  public static final class RepResult {
    public long wallClockMs;
    public long aggregateThreadTimeMs;
    public long attempted;
    public long failed;
  }

  public static final class KResult {
    public int k;
    public double wallClockMsMedian;
    public double wallClockMsMin;
    public double wallClockMsMax;
    public double artifactsPerSecMedian;
    public double speedupVsK1;
    public double parallelEfficiency;
    public double aggregateThreadTimeMsMedian;
    public List<RepResult> repetitions;
  }

  public static final class Report {
    public String timestamp;
    public String jvm;
    public String os;
    public String postgresVersion;
    public boolean synchronousCommitCorrectedToOn;
    public Params params;
    public List<KResult> results = new ArrayList<>();
    /** K=1 re-run once at the very end of the sweep; see the drift-check log
     *  line for how it compares to the original K=1 median. Null if the
     *  drift-check run itself failed. */
    public Long k1DriftRecheckWallClockMs;
  }
}
