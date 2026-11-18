/*
 * Copyright (c) 2026, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.rs.io.storage.warc;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.db.SQLArtifactIndex;
import org.lockss.rs.io.index.db.SQLArtifactIndexDbManager;
import org.lockss.rs.io.storage.ReindexResult;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssCoreTestCase5;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Investigates the operator claim, recorded in {@code au-reindex-recovery-plan.md}
 * E.3, that "the vast majority of the time [in a production reindex] appears to be
 * time spent reading WARC files to locate WARC records and construct artifact
 * storage URLs from them" -- i.e. that WARC parsing, not the batched DB commit that
 * E.3 optimized, dominates real reindex wall time.
 *
 * <p>This benchmark writes a synthetic WARC with realistic-scale record sizes,
 * indexes it via {@link WarcArtifactDataStore#indexArtifactsFromWarc(ArtifactIndex,
 * Path, ReindexResult)} against a real {@link SQLArtifactIndex} backed by embedded
 * PostgreSQL (not {@code VolatileArtifactIndex}, which would trivially make the DB
 * phase look negligible), and separates the two phases -- WARC read/parse vs.
 * {@code index.reindexArtifacts(batch)} -- entirely from OUTSIDE
 * {@code WarcArtifactDataStore}, via a Mockito spy on the real index that times its
 * own {@code reindexArtifacts(Iterable)} calls. No production code is modified.
 */
public class TestWarcReindexBenchmark extends LockssCoreTestCase5 {
  private static final L4JLogger log = L4JLogger.getLogger();

  private static final String NS = "lockss";
  private static final String AUID = "bench-auid";

  private static EmbeddedPostgres embeddedPg;

  private SQLArtifactIndexDbManager idxDbManager;
  private MockLockssDaemon theDaemon;
  private String dbName;

  private File basePath;
  private File stateDir;
  private LocalWarcArtifactDataStore store;

  @BeforeAll
  public static void setUpClass() throws Exception {
    embeddedPg = startEmbeddedPostgres();
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    stopEmbeddedPostgre();
  }

  @BeforeEach
  public void setUp() throws Exception {
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);
    setUpDiskSpace();

    dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

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

    basePath = getTempDir();
    stateDir = getTempDir();

    store = new LocalWarcArtifactDataStore(new File[]{basePath});
    BaseLockssRepository repo = Mockito.mock(BaseLockssRepository.class);
    Mockito.when(repo.getRepositoryStateDirPath()).thenReturn(stateDir.toPath());
    store.setLockssRepository(repo);
    store.init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.stop();
    if (idxDbManager != null) {
      idxDbManager.stopService();
    }
    theDaemon.stopDaemon();
  }

  /**
   * Best-effort correction for zonky EmbeddedPostgres's {@code synchronous_commit
   * = off} builder default (set for test-suite speed, not representativeness).
   * Without this the DB-commit phase measured below understates its real cost,
   * which would bias this benchmark TOWARD confirming the operator's claim
   * regardless of the truth. See the analogous helper and comment in
   * {@code TestSQLArtifactIndexDbManager#trySetSynchronousCommitOn()}.
   */
  private boolean trySetSynchronousCommitOn() {
    String url = "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName;
    try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
         Statement st = conn.createStatement()) {
      st.execute("ALTER DATABASE \"" + dbName + "\" SET synchronous_commit = on");
      return true;
    } catch (SQLException e) {
      log.warn("Could not set synchronous_commit = on for the benchmark; the " +
          "measured DB-phase share is a floor, not an estimate", e);
      return false;
    }
  }

  // *******************************************************************************
  // * SYNTHETIC WARC GENERATION
  // *******************************************************************************

  /** Result of writing a synthetic WARC: how many records and how many payload bytes. */
  private static final class WriteResult {
    final int numRecords;
    final long totalPayloadBytes;
    WriteResult(int numRecords, long totalPayloadBytes) {
      this.numRecords = numRecords;
      this.totalPayloadBytes = totalPayloadBytes;
    }
  }

  /**
   * Writes {@code numRecords} WARC response records of varying size directly to
   * {@code warcPath}, one {@link ArtifactSpec} at a time so at most one record's
   * content is held in memory at once (rather than building the whole WARC file
   * as one in-memory {@code byte[]}, which does not scale to hundreds of MB).
   *
   * <p>Record payload sizes are drawn uniformly from {@code [minBytes, maxBytes)}
   * -- not identical across records -- to roughly span the average-artifact-size
   * range actually observed in this deployment's V2 migration report (~30KB to
   * ~2.2MB per the addenda), rather than benchmark a single fixed size.
   *
   * <p>For the compressed case, each record gets its own {@link GZIPOutputStream}
   * instance, matching production's one-gzip-member-per-record layout (and the
   * existing {@code createWarcFileFromSpecs} test helper's pattern) -- but backed
   * here by a real file, so the {@link GZIPOutputStream} is wrapped in a
   * {@link CloseShieldOutputStream} to keep it from closing the underlying file
   * stream when each record's gzip member is finished.
   */
  private WriteResult writeSyntheticWarc(Path warcPath, int numRecords,
                                         boolean compressed, int minBytes, int maxBytes,
                                         long seed) throws IOException {
    Random rnd = new Random(seed);
    long totalBytes = 0;

    try (OutputStream fileOut =
             new BufferedOutputStream(Files.newOutputStream(warcPath), 1 << 20)) {
      for (int i = 0; i < numRecords; i++) {
        int len = minBytes + rnd.nextInt(maxBytes - minBytes);

        ArtifactSpec spec = new ArtifactSpec()
            .setArtifactUuid(UUID.randomUUID().toString())
            .setNamespace(NS)
            .setAuid(AUID)
            .setUrl("https://example.com/bench/" + i)
            .setVersion(1)
            .setContentLength(len);
        spec.generateContent();

        ArtifactData ad = spec.getArtifactData();

        // WarcArtifactDataStore.writeArtifactData() always closes the OutputStream
        // it is given (it wraps it in a CountingOutputStream and writes inside a
        // try-with-resources), so every call here -- compressed or not -- must be
        // shielded from closing the shared file stream underneath it.
        if (compressed) {
          OutputStream shielded = CloseShieldOutputStream.wrap(fileOut);
          try (GZIPOutputStream gzOut = new GZIPOutputStream(shielded)) {
            WarcArtifactDataStore.writeArtifactData(ad, gzOut);
          }
        } else {
          WarcArtifactDataStore.writeArtifactData(ad, CloseShieldOutputStream.wrap(fileOut));
        }

        totalBytes += len;
      }
    }

    return new WriteResult(numRecords, totalBytes);
  }

  // *******************************************************************************
  // * TIMED DB-PHASE WRAPPER
  // *******************************************************************************

  /**
   * Wraps {@code delegate} with a Mockito spy whose {@code reindexArtifacts
   * (Iterable)} calls the real implementation but accumulates the wall-clock
   * nanoseconds spent inside it into {@code dbNanosOut}. This isolates the
   * DB-commit phase's cost from OUTSIDE {@code WarcArtifactDataStore} -- no
   * change to production code is needed to get this separation.
   */
  private ArtifactIndex timedIndex(ArtifactIndex delegate, AtomicLong dbNanosOut) throws Exception {
    ArtifactIndex spy = Mockito.spy(delegate);
    Mockito.doAnswer((InvocationOnMock invocation) -> {
      long start = System.nanoTime();
      try {
        return invocation.callRealMethod();
      } finally {
        dbNanosOut.addAndGet(System.nanoTime() - start);
      }
    }).when(spy).reindexArtifacts(Mockito.any());
    return spy;
  }

  // *******************************************************************************
  // * BENCHMARK
  // *******************************************************************************

  /**
   * Runs the reindex benchmark for one compression variant and logs the
   * WARC-read/parse vs. DB-commit split. Asserts only that both phases take
   * non-zero time and that every written record was indexed -- the breakdown
   * itself, not a specific ratio, is the point of this test.
   */
  private void runBenchmark(String label, boolean compressed, int numRecords,
                            int minBytes, int maxBytes, long seed) throws Exception {
    boolean correctedSyncCommit = trySetSynchronousCommitOn();

    Path auPath = store.generateAUPath(basePath.toPath(), NS, AUID + "-" + label);
    Files.createDirectories(auPath);
    String ext = compressed ?
        org.archive.format.warc.WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION :
        org.archive.format.warc.WARCConstants.DOT_WARC_FILE_EXTENSION;
    Path warcPath = auPath.resolve("artifacts_bench" + ext);

    WriteResult written = writeSyntheticWarc(warcPath, numRecords, compressed,
        minBytes, maxBytes, seed);

    SQLArtifactIndex realIndex = new SQLArtifactIndex();
    realIndex.init();
    realIndex.start();

    try {
      AtomicLong dbNanos = new AtomicLong();
      ArtifactIndex spiedIndex = timedIndex(realIndex, dbNanos);

      long totalStartNanos = System.nanoTime();
      long numIndexed = store.indexArtifactsFromWarc(spiedIndex, warcPath, new ReindexResult());
      long totalNanos = System.nanoTime() - totalStartNanos;

      long dbMs = dbNanos.get() / 1_000_000L;
      long totalMs = totalNanos / 1_000_000L;
      long parseMs = totalMs - dbMs;

      double dbPct = 100.0 * dbMs / totalMs;
      double parsePct = 100.0 * parseMs / totalMs;

      log.info("Reindex WARC-read-vs-DB-commit benchmark [" + label + "]: "
          + "records=" + written.numRecords
          + ", corpusBytes=" + written.totalPayloadBytes
          + ", compressed=" + compressed
          + ", synchronous_commit corrected to ON=" + correctedSyncCommit
          + " -- total=" + totalMs + "ms"
          + ", WARC-read/parse=" + parseMs + "ms (" + String.format("%.1f", parsePct) + "%)"
          + ", DB-commit=" + dbMs + "ms (" + String.format("%.1f", dbPct) + "%)");

      assertEquals(numRecords, numIndexed,
          "every written record should have been indexed");
      assertTrue(dbMs > 0, "DB-commit phase should take measurable time");
      assertTrue(parseMs > 0, "WARC-read/parse phase should take measurable time");
    } finally {
      realIndex.stop();
    }
  }

  /**
   * Main pass: GZIP-compressed WARC, matching production's default
   * ({@code ArtifactDataStoreConfig.DEFAULT_REPO_USE_WARC_COMPRESSION = true}).
   * 3,000 records, payload sizes uniform in [30KB, 400KB) (mean ~215KB), which
   * sits in the middle of the ~30KB-2.2MB average-artifact-size range actually
   * observed in this deployment's V2 migration report.
   */
  @Test
  public void testReindexBenchmarkCompressed() throws Exception {
    runBenchmark("compressed", true, 3000, 30_000, 400_000, 42L);
  }

  /**
   * Smaller-scale pass: UNCOMPRESSED WARC. Compression is the single biggest
   * lever on whether "reading the WARC" is CPU-decompression-bound or
   * I/O-bound (see the JWAT finding in the report), so this variant is run at
   * reduced scale (600 records) to check whether the split changes materially
   * without doubling the benchmark's total runtime.
   */
  @Test
  public void testReindexBenchmarkUncompressed() throws Exception {
    runBenchmark("uncompressed", false, 600, 30_000, 400_000, 43L);
  }
}
