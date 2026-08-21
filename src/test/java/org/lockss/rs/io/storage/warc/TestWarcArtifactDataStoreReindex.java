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

import org.apache.commons.collections4.IterableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.ReindexResult;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.util.time.TimeBase;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the reindex paths of {@link WarcArtifactDataStore}: the targeted
 * per-AU reindex (Component C), the same-run-stamp rotation of the reindex
 * ledger and failure report (E.5) and the reporting of per-WARC failures (E.7).
 */
public class TestWarcArtifactDataStoreReindex extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final String NS = "lockss";
  private static final String AUID_A = "auidA";
  private static final String AUID_B = "auidB";
  private static final String AUID_MISSING = "auidMissing";

  private File basePath1;
  private File basePath2;
  private File stateDir;

  private LocalWarcArtifactDataStore store;
  private VolatileArtifactIndex index;

  @BeforeEach
  public void setUp() throws Exception {
    TimeBase.setSimulated(1700000000000L);

    basePath1 = getTempDir();
    basePath2 = getTempDir();
    stateDir = getTempDir();

    index = new VolatileArtifactIndex();
    index.init();
    index.start();

    store = new LocalWarcArtifactDataStore(new File[]{basePath1, basePath2});

    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    when(repo.getRepositoryStateDirPath()).thenReturn(stateDir.toPath());
    store.setLockssRepository(repo);

    store.init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.stop();
    index.stop();
  }

  // *******************************************************************************
  // * COMPONENT C -- PER-AU REINDEX
  // *******************************************************************************

  /**
   * An AU may have a directory under any or all base paths; all of them are walked.
   */
  @Test
  public void testReindexArtifactsInAuSpansTwoBasePaths() throws Exception {
    List<String> uuidsOn1 = writeAuWarc(basePath1, NS, AUID_A, "warc1", 2);
    List<String> uuidsOn2 = writeAuWarc(basePath2, NS, AUID_A, "warc2", 3);

    ReindexResult result = store.reindexArtifactsInAu(index, NS, AUID_A);

    assertFalse(result.hasFailures(), "unexpected failures: " + result.getWarcFailures());
    assertEquals(2, result.getWarcsAttempted());
    assertEquals(2, result.getWarcsSucceeded());
    assertEquals(5, result.getArtifactsIndexed());
    assertEquals(0, result.getArtifactsSkipped());

    for (String uuid : uuidsOn1) {
      assertTrue(index.artifactExists(uuid), "missing artifact from base path 1: " + uuid);
    }
    for (String uuid : uuidsOn2) {
      assertTrue(index.artifactExists(uuid), "missing artifact from base path 2: " + uuid);
    }

    assertEquals(5, IterableUtils.size(index.getArtifactsAllVersions(NS, AUID_A)));
  }

  /**
   * An AUID with no directory under any base path is unrecoverable: it must be
   * reported, never silently skipped.
   */
  @Test
  public void testReindexArtifactsInAuReportsMissingAu() throws Exception {
    // Another AU exists, so the base paths themselves are populated
    writeAuWarc(basePath1, NS, AUID_A, "warc1", 1);

    ReindexResult result = store.reindexArtifactsInAu(index, NS, AUID_MISSING);

    assertTrue(result.hasFailures());
    assertEquals(List.of(AUID_MISSING), result.getMissingAus());
    assertEquals(0, result.getWarcsAttempted());
    assertEquals(0, result.getArtifactsIndexed());

    // ...and it is named in the failure report written at the end of the run
    store.finishReindexRun(result);

    String report = readOnlyRotatedFile(WarcArtifactDataStore.REINDEX_FAILURES_FILE);
    assertTrue(report.contains(AUID_MISSING), "failure report did not name the AU: " + report);
  }

  /**
   * Journal and metadata WARCs in an AU directory are not artifact WARCs and must
   * not be reindexed.
   */
  @Test
  public void testReindexArtifactsInAuExcludesJournals() throws Exception {
    writeAuWarc(basePath1, NS, AUID_A, "warc1", 2);

    Path auPath = store.generateAUPath(basePath1.toPath(), NS, AUID_A);

    // Both of these end in ".warc" and so are found by findWarcs(), and both must
    // be filtered out by isWarcJournalPath()
    Files.write(auPath.resolve(WarcArtifactDataStore.V0_STATE_FILE),
                "not a WARC".getBytes(StandardCharsets.UTF_8));
    Files.write(auPath.resolve("lockss-repo.metadata.warc"),
                "not a WARC".getBytes(StandardCharsets.UTF_8));

    ReindexResult result = store.reindexArtifactsInAu(index, NS, AUID_A);

    assertFalse(result.hasFailures(), "unexpected failures: " + result.getWarcFailures());
    assertEquals(1, result.getWarcsAttempted());
    assertEquals(2, result.getArtifactsIndexed());
  }

  /**
   * The artifact UUID comes from the WARC record, so re-running must upsert the
   * same rows rather than duplicating them.
   */
  @Test
  public void testReindexArtifactsInAuIsIdempotent() throws Exception {
    List<String> uuids = writeAuWarc(basePath1, NS, AUID_A, "warc1", 3);
    uuids.addAll(writeAuWarc(basePath2, NS, AUID_A, "warc2", 2));

    ReindexResult first = store.reindexArtifactsInAu(index, NS, AUID_A);
    assertFalse(first.hasFailures());
    assertEquals(5, first.getArtifactsIndexed());
    assertEquals(5, IterableUtils.size(index.getArtifactsAllVersions(NS, AUID_A)));

    ReindexResult second = store.reindexArtifactsInAu(index, NS, AUID_A);
    assertFalse(second.hasFailures());
    assertEquals(2, second.getWarcsAttempted(),
                 "the ledger must not cause the AU's WARCs to be skipped on a rerun");

    // Same artifacts, not twice as many
    assertEquals(5, IterableUtils.size(index.getArtifactsAllVersions(NS, AUID_A)));
    for (String uuid : uuids) {
      assertTrue(index.artifactExists(uuid));
    }
  }

  /**
   * E.6: a record whose own index row already matches it exactly must still be
   * presented to the index, not skipped.
   *
   * <p>The skipped-if-equal guard this asserts against was written when a reindex
   * meant rebuilding from an empty index. Against a populated index it fired
   * routinely -- and because its lookup was BY UUID, it could only tell whether
   * THIS artifact's row was current, never whether a different uuid occupied the
   * same (namespace, auid, url, version). Version-conflict resolution runs only for
   * artifacts that reach the batch, so anything the guard skipped was never
   * arbitrated, and a duplicated tuple survived every pass.
   *
   * <p>The second pass here re-presents five artifacts whose rows are byte-identical
   * to what the WARCs say. With the guard they were all skipped and the pass indexed
   * 0; without it all five are presented and the upsert decides.
   */
  @Test
  public void testReindexPresentsArtifactsAlreadyMatchingTheIndex() throws Exception {
    writeAuWarc(basePath1, NS, AUID_A, "warc1", 3);
    writeAuWarc(basePath2, NS, AUID_A, "warc2", 2);

    ReindexResult first = store.reindexArtifactsInAu(index, NS, AUID_A);
    assertEquals(5, first.getArtifactsIndexed());

    // Every row now matches its WARC record exactly.
    ReindexResult second = store.reindexArtifactsInAu(index, NS, AUID_A);
    assertFalse(second.hasFailures());
    assertEquals(5, second.getArtifactsIndexed(),
                 "an artifact matching its index row must still be presented, " +
                 "so that version-conflict resolution can see it");

    // ...and re-presenting them does not duplicate them.
    assertEquals(5, IterableUtils.size(index.getArtifactsAllVersions(NS, AUID_A)));
  }

  /**
   * Reindexing one AU must not pull in another AU's WARCs.
   */
  @Test
  public void testReindexArtifactsInAuIsScopedToTheAu() throws Exception {
    writeAuWarc(basePath1, NS, AUID_A, "warc1", 2);
    List<String> otherUuids = writeAuWarc(basePath1, NS, AUID_B, "warc1", 2);

    ReindexResult result = store.reindexArtifactsInAu(index, NS, AUID_A);

    assertEquals(1, result.getWarcsAttempted());
    assertEquals(2, result.getArtifactsIndexed());

    for (String uuid : otherUuids) {
      assertFalse(index.artifactExists(uuid), "reindexed an artifact of another AU");
    }
  }

  // *******************************************************************************
  // * E.5 -- SAME-DAY ROTATION
  // *******************************************************************************

  /**
   * Two reindex passes on the same UTC day (here: at the same simulated
   * millisecond) must both retain their files, under distinct names, and neither
   * may throw. The two files of a run share one stamp.
   */
  @Test
  public void testTwoReindexPassesOnTheSameDay() throws Exception {
    writeAuWarc(basePath1, NS, AUID_A, "warc1", 2);

    // TimeBase is simulated and not advanced, so both runs compute the same base
    // stamp: exactly the case that used to throw with BASIC_ISO_DATE.
    store.reindexArtifacts(index);
    store.reindexArtifacts(index);

    List<String> ledgers = rotatedNames(WarcArtifactDataStore.REINDEXED_WARCS_FILE);
    List<String> reports = rotatedNames(WarcArtifactDataStore.REINDEX_FAILURES_FILE);

    assertEquals(2, ledgers.size(), "both runs' ledgers must be retained: " + ledgers);
    assertEquals(2, reports.size(), "both runs' failure reports must be retained: " + reports);

    // A run's two files share one stamp
    assertIterableEquals(ledgers, reports,
                         "the ledger and failure report of a run must share a stamp");

    // ...and the two runs' stamps differ
    assertNotEquals(ledgers.get(0), ledgers.get(1));
  }

  // *******************************************************************************
  // * E.7 -- FAILURES ARE REPORTED, NOT RETRIED
  // *******************************************************************************

  /**
   * One unreadable WARC among several: the good ones are indexed and recorded in
   * the ledger, and the bad one is named with its cause in the failure report.
   */
  @Test
  public void testReindexArtifactsReportsUnreadableWarc() throws Exception {
    List<String> good = writeAuWarc(basePath1, NS, AUID_A, "warc1", 2);
    good.addAll(writeAuWarc(basePath2, NS, AUID_B, "warc1", 1));

    // A real file that findWarcs() picks up but that cannot be read. Note that
    // simply filling it with garbage is NOT enough: JWAT's iterator swallows the
    // IOException in hasNext(), so a corrupt WARC yields zero records rather than
    // throwing. The failure is injected at the point where the store opens it.
    Path auPath = store.generateAUPath(basePath1.toPath(), NS, AUID_A);
    Path badWarc = auPath.resolve("artifacts_broken.warc");
    Files.write(badWarc, "definitely not a WARC".getBytes(StandardCharsets.UTF_8));

    LocalWarcArtifactDataStore spyStore = Mockito.spy(store);
    Mockito.doThrow(new IOException("simulated unreadable WARC"))
        .when(spyStore).getInputStreamAndSeek(badWarc, 0);

    ReindexResult result = spyStore.reindexArtifacts(index);

    assertTrue(result.hasFailures());
    assertEquals(1, result.getWarcsFailedCount());
    assertEquals(2, result.getWarcsSucceeded());
    assertEquals(3, result.getWarcsAttempted());
    assertEquals(3, result.getArtifactsIndexed());

    // The good artifacts are in the index
    for (String uuid : good) {
      assertTrue(index.artifactExists(uuid), "missing artifact " + uuid);
    }

    // The good WARCs, and only those, are in the ledger
    String ledger = readOnlyRotatedFile(WarcArtifactDataStore.REINDEXED_WARCS_FILE);
    assertFalse(ledger.contains(badWarc.toString()),
                "the unreadable WARC must not be recorded as reindexed");

    // The bad WARC is named with its cause in the failure report
    String report = readOnlyRotatedFile(WarcArtifactDataStore.REINDEX_FAILURES_FILE);
    assertTrue(report.startsWith("warc_file,error"), "unexpected report header: " + report);
    assertTrue(report.contains(badWarc.toString()),
               "failure report did not name the bad WARC: " + report);
    assertEquals(2, report.lines().count(), "expected one failure row: " + report);
  }

  /**
   * A clean run leaves an empty failure report -- header only -- rather than no
   * report at all or a stale one.
   */
  @Test
  public void testCleanRunWritesEmptyFailureReport() throws Exception {
    writeAuWarc(basePath1, NS, AUID_A, "warc1", 2);

    ReindexResult result = store.reindexArtifacts(index);

    assertFalse(result.hasFailures());

    String report = readOnlyRotatedFile(WarcArtifactDataStore.REINDEX_FAILURES_FILE);
    assertEquals(1, report.lines().count(), "expected a header-only report: " + report);
    assertTrue(report.startsWith("warc_file,error"));
  }

  // *******************************************************************************
  // * UTILITIES
  // *******************************************************************************

  /**
   * Writes a WARC of {@code numArtifacts} artifacts into the AU's directory under
   * the given base path, creating the directory if needed.
   *
   * @return the UUIDs of the artifacts written.
   */
  private List<String> writeAuWarc(File basePath, String namespace, String auid,
                                   String warcName, int numArtifacts)
      throws IOException {

    Path auPath = store.generateAUPath(basePath.toPath(), namespace, auid);
    Files.createDirectories(auPath);

    List<ArtifactSpec> specs = new ArrayList<>();
    List<String> uuids = new ArrayList<>();

    for (int i = 0; i < numArtifacts; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(namespace)
          .setAuid(auid)
          .setUrl("https://example.com/" + warcName + "/" + i)
          .setVersion(1)
          .setContentLength(128);

      spec.generateContent();

      specs.add(spec);
      uuids.add(spec.getArtifactUuid());
    }

    byte[] warcBytes = AbstractWarcArtifactDataStoreTest
        .createWarcFileFromSpecs(false, specs.toArray(new ArtifactSpec[0]));

    Path warcPath = auPath.resolve("artifacts_" + warcName + ".warc");
    Files.write(warcPath, warcBytes);

    log.debug("Wrote {} artifacts to {}", numArtifacts, warcPath);

    return uuids;
  }

  /** The stamp suffixes of the rotated copies of a state file, in name order. */
  private List<String> rotatedNames(String stateFile) throws IOException {
    Path path = stateDir.toPath().resolve(stateFile);
    Path dir = path.getParent();
    String prefix = path.getFileName() + ".";

    assertFalse(path.toFile().exists(),
                "the un-rotated file should not be left behind: " + path);

    if (!dir.toFile().isDirectory()) {
      return List.of();
    }

    try (var paths = Files.list(dir)) {
      return paths
          .map(p -> p.getFileName().toString())
          .filter(name -> name.startsWith(prefix))
          .map(name -> name.substring(prefix.length()))
          .sorted()
          .toList();
    }
  }

  /** Contents of the single rotated copy of a state file. */
  private String readOnlyRotatedFile(String stateFile) throws IOException {
    Path path = stateDir.toPath().resolve(stateFile);
    List<String> stamps = rotatedNames(stateFile);
    assertEquals(1, stamps.size(), "expected exactly one rotated copy: " + stamps);
    return Files.readString(path.resolveSibling(path.getFileName() + "." + stamps.get(0)));
  }
}
