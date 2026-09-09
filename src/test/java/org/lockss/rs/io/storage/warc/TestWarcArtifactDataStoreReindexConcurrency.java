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

package org.lockss.rs.io.storage.warc;

import org.apache.commons.collections4.IterableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.ReindexResult;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssCoreTestCase5;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the concurrency of {@link WarcArtifactDataStore}'s reindex WARC walk
 * (#737): that reindexing WARCs in parallel produces the same result as reindexing
 * them one at a time, that the permanent-before-temporary phase ordering survives
 * concurrency, and that the per-record and per-WARC isolation the sequential path
 * has is not lost when WARCs are parsed at the same time.
 *
 * @see WarcArtifactDataStore#PARAM_REINDEX_PARSE_THREADS
 */
public class TestWarcArtifactDataStoreReindexConcurrency extends LockssCoreTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final String NS = "lockss";

  private File basePath1;
  private File basePath2;
  private File stateDir;

  private InstrumentedStore store;
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

    store = new InstrumentedStore(new File[]{basePath1, basePath2});

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
  // * THE CONFIGURATION PARAMETER
  // *******************************************************************************

  /**
   * The thread count comes from configuration, and a value that would make no
   * sense as a thread count falls back to sequential rather than throwing.
   */
  @Test
  public void testReindexParseThreadsParam() throws Exception {
    assertEquals(WarcArtifactDataStore.DEFAULT_REINDEX_PARSE_THREADS,
                 store.getReindexParseThreads(),
                 "unset parameter should yield the default");

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "7");
    assertEquals(7, store.getReindexParseThreads());

    // Not a hardcoded ceiling: a value above the measured peak is honoured
    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "64");
    assertEquals(64, store.getReindexParseThreads());

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "1");
    assertEquals(1, store.getReindexParseThreads());

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "0");
    assertEquals(1, store.getReindexParseThreads(), "0 threads must degrade to sequential");

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "-4");
    assertEquals(1, store.getReindexParseThreads(), "negative threads must degrade to sequential");
  }

  // *******************************************************************************
  // * CONCURRENT == SEQUENTIAL
  // *******************************************************************************

  /**
   * The same corpus reindexed concurrently and sequentially must produce the same
   * index contents and the same {@link ReindexResult} counts.
   * <p>
   * Both passes run against the same on-disk corpus but into their own index. The
   * ledger of reindexed WARCs is rotated away at the end of a pass, so the second
   * pass re-reads every WARC rather than skipping them.
   */
  @Test
  public void testConcurrentReindexMatchesSequential() throws Exception {
    // A corpus with more WARCs than threads, spread over both base paths, plus
    // temporary WARCs so both phases are exercised.
    List<String> uuids = new ArrayList<>();
    uuids.addAll(writeAuWarc(basePath1, "auA", "w1", 3));
    uuids.addAll(writeAuWarc(basePath1, "auA", "w2", 5));
    uuids.addAll(writeAuWarc(basePath1, "auB", "w3", 2));
    uuids.addAll(writeAuWarc(basePath2, "auB", "w4", 4));
    uuids.addAll(writeAuWarc(basePath2, "auC", "w5", 1));
    uuids.addAll(writeAuWarc(basePath2, "auC", "w6", 6));
    uuids.addAll(writeTmpWarc(basePath1, "auD", "t1", 2));
    uuids.addAll(writeTmpWarc(basePath2, "auD", "t2", 3));

    VolatileArtifactIndex sequentialIndex = newIndex();
    VolatileArtifactIndex concurrentIndex = newIndex();

    try {
      ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "1");
      ReindexResult sequential = store.reindexArtifacts(sequentialIndex);
      int sequentialThreads = store.parseThreads.size();

      store.reset();

      ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "4");
      ReindexResult concurrent = store.reindexArtifacts(concurrentIndex);

      // Guard against the comparison passing because the "concurrent" pass quietly
      // ran on one thread: then it would prove nothing.
      assertEquals(1, sequentialThreads, "the sequential pass must not use a pool");
      assertTrue(store.parseThreads.size() > 1,
                 "the concurrent pass parsed on one thread only: " + store.parseThreads);

      assertFalse(sequential.hasFailures(), "sequential failures: " + sequential.getWarcFailures());
      assertFalse(concurrent.hasFailures(), "concurrent failures: " + concurrent.getWarcFailures());

      assertEquals(describe(sequential), describe(concurrent),
                   "concurrent reindex produced a different result than sequential");

      assertEquals(snapshot(sequentialIndex, uuids), snapshot(concurrentIndex, uuids),
                   "concurrent reindex produced different index contents than sequential");

      // Every AU holds the same number of artifacts in both indexes, so the
      // comparison above cannot be satisfied by a pass that indexed extra rows.
      for (String auid : List.of("auA", "auB", "auC", "auD")) {
        assertEquals(countArtifacts(sequentialIndex, auid), countArtifacts(concurrentIndex, auid),
                     "artifact count differs for " + auid);
      }
    } finally {
      sequentialIndex.stop();
      concurrentIndex.stop();
    }
  }

  // *******************************************************************************
  // * PERMANENT WARCS BEFORE TEMPORARY ONES
  // *******************************************************************************

  /**
   * Every permanent WARC must be finished before the first temporary one is
   * started, however many parse threads are in play.
   * <p>
   * One permanent WARC is made much slower than the rest and there are more
   * permanent WARCs than threads, so a walk that merely submitted the concatenation
   * of the two groups to one pool would start a temporary WARC while that slow
   * permanent WARC was still being parsed. The assertion is on a single global
   * sequence of parse start/end events, so it does not depend on timing.
   */
  @Test
  public void testPermanentWarcsFinishBeforeTemporaryOnesStart() throws Exception {
    Path slowWarc = writeAuWarcAt(basePath1, "auA", "slow", 2);
    writeAuWarc(basePath1, "auA", "fast1", 1);
    writeAuWarc(basePath1, "auB", "fast2", 1);
    writeAuWarc(basePath2, "auB", "fast3", 1);
    writeAuWarc(basePath2, "auC", "fast4", 1);
    writeAuWarc(basePath2, "auC", "fast5", 1);

    writeTmpWarc(basePath1, "auD", "t1", 1);
    writeTmpWarc(basePath2, "auD", "t2", 1);

    // Long enough that every other WARC is parsed while this one is still going
    store.delays.put(slowWarc, 300L);

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "4");

    VolatileArtifactIndex target = newIndex();

    try {
      ReindexResult result = store.reindexArtifacts(target);
      assertFalse(result.hasFailures(), "unexpected failures: " + result.getWarcFailures());
    } finally {
      target.stop();
    }

    List<ParseEvent> events = store.events();

    assertEquals(6, events.stream().filter(event -> !event.temporary).count(),
                 "expected six permanent WARCs: " + events);
    assertEquals(2, events.stream().filter(event -> event.temporary).count(),
                 "expected two temporary WARCs: " + events);
    assertTrue(store.parseThreads.size() > 1,
               "the walk parsed on one thread only, so it cannot demonstrate the ordering");

    int lastPermanentEnd = events.stream()
        .filter(event -> !event.temporary)
        .mapToInt(event -> event.end)
        .max()
        .orElseThrow();

    int firstTemporaryStart = events.stream()
        .filter(event -> event.temporary)
        .mapToInt(event -> event.start)
        .min()
        .orElseThrow();

    assertTrue(lastPermanentEnd < firstTemporaryStart,
               "a temporary WARC was parsed before every permanent WARC had finished: " + events);
  }

  // *******************************************************************************
  // * ISOLATION UNDER CONCURRENCY
  // *******************************************************************************

  /**
   * A malformed record in one WARC costs that one artifact, and nothing in the
   * WARCs being parsed at the same time.
   */
  @Test
  public void testMalformedRecordDoesNotAffectConcurrentWarcs() throws Exception {
    List<String> good = new ArrayList<>();
    good.addAll(writeAuWarc(basePath1, "auB", "g1", 4));
    good.addAll(writeAuWarc(basePath2, "auB", "g2", 4));
    good.addAll(writeAuWarc(basePath2, "auC", "g3", 4));

    // Three records, the middle one corrupted so that it, and only it, fails
    CorruptedWarc corrupted = writeAuWarcWithBadRecord(basePath1, "auA", "bad", 3, 1);

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "4");

    VolatileArtifactIndex target = newIndex();

    try {
      ReindexResult result = store.reindexArtifacts(target);

      assertTrue(store.parseThreads.size() > 1,
                 "the walk parsed on one thread only, so it cannot demonstrate isolation");

      // A bad record is not a bad WARC: every WARC still succeeds
      assertFalse(result.hasFailures(), "unexpected WARC failures: " + result.getWarcFailures());
      assertEquals(4, result.getWarcsAttempted());
      assertEquals(4, result.getWarcsSucceeded());

      // 12 good artifacts in the other WARCs, plus the 2 good ones in the bad WARC
      assertEquals(14, result.getArtifactsIndexed());

      for (String uuid : good) {
        assertNotNull(target.getArtifact(uuid),
                      "an artifact of a WARC parsed alongside the bad one is missing: " + uuid);
      }

      for (String uuid : corrupted.goodUuids) {
        assertNotNull(target.getArtifact(uuid),
                      "a good record of the bad WARC is missing: " + uuid);
      }

      assertNull(target.getArtifact(corrupted.badUuid),
                 "the malformed record should not have been indexed");
    } finally {
      target.stop();
    }
  }

  /**
   * A {@link Throwable} that the per-WARC handler does not catch is reported
   * against the WARC it came from, and the WARCs being parsed at the same time
   * still complete. Without this the whole pass would abort and every other WARC's
   * work would be discarded along with it.
   */
  @Test
  public void testUnexpectedThrowableIsReportedAndOtherWarcsComplete() throws Exception {
    List<String> good = new ArrayList<>();
    good.addAll(writeAuWarc(basePath1, "auB", "g1", 2));
    good.addAll(writeAuWarc(basePath2, "auB", "g2", 2));
    good.addAll(writeAuWarc(basePath2, "auC", "g3", 2));

    Path doomed = writeAuWarcAt(basePath1, "auA", "doomed", 2);

    // An Error, not an Exception: reindexOneWarc() catches Exception itself, so
    // this is the case that reaches the walk's own handler.
    store.errors.put(doomed, "simulated unexpected failure");

    ConfigurationUtil.addFromArgs(WarcArtifactDataStore.PARAM_REINDEX_PARSE_THREADS, "4");

    VolatileArtifactIndex target = newIndex();

    try {
      ReindexResult result = store.reindexArtifacts(target);

      assertTrue(store.parseThreads.size() > 1,
                 "the walk parsed on one thread only, so it cannot demonstrate isolation");

      assertTrue(result.hasFailures(), "the doomed WARC should have been reported");
      assertEquals(1, result.getWarcsFailedCount());
      assertEquals(doomed, result.getWarcFailures().get(0).getWarcFile());
      assertTrue(result.getWarcFailures().get(0).getError().contains("simulated unexpected failure"),
                 "unexpected cause: " + result.getWarcFailures().get(0).getError());

      assertEquals(4, result.getWarcsAttempted());
      assertEquals(3, result.getWarcsSucceeded());
      assertEquals(6, result.getArtifactsIndexed());

      for (String uuid : good) {
        assertNotNull(target.getArtifact(uuid),
                      "an artifact of a WARC parsed alongside the doomed one is missing: " + uuid);
      }
    } finally {
      target.stop();
    }
  }

  // *******************************************************************************
  // * INSTRUMENTED STORE
  // *******************************************************************************

  /** One WARC's parse, stamped from a sequence shared by every parse. */
  private static final class ParseEvent {
    final Path warcFile;
    final boolean temporary;
    final int start;
    final int end;

    ParseEvent(Path warcFile, boolean temporary, int start, int end) {
      this.warcFile = warcFile;
      this.temporary = temporary;
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return (temporary ? "tmp " : "perm ") + warcFile.getFileName()
          + " [" + start + ".." + end + "]";
    }
  }

  /**
   * A data store that records the order in which WARCs are parsed and on which
   * threads, and can make a chosen WARC slow or make its parse fail with an
   * {@link Error}.
   */
  private static class InstrumentedStore extends LocalWarcArtifactDataStore {
    final Map<Path, Long> delays = new ConcurrentHashMap<>();
    final Map<Path, String> errors = new ConcurrentHashMap<>();
    /** Ids, not names, so the test does not depend on how the pool names its threads. */
    final Set<Long> parseThreads = Collections.synchronizedSet(new HashSet<>());

    private final AtomicInteger sequence = new AtomicInteger();
    private final List<ParseEvent> events = Collections.synchronizedList(new ArrayList<>());

    InstrumentedStore(File[] basePaths) throws IOException {
      super(basePaths);
    }

    @Override
    public long indexArtifactsFromWarc(ArtifactIndex index, Path warcFile, ReindexResult result)
        throws IOException {
      parseThreads.add(Thread.currentThread().getId());

      boolean temporary = isTmpStorage(warcFile);
      int start = sequence.getAndIncrement();

      try {
        String error = errors.get(warcFile);
        if (error != null) {
          throw new AssertionError(error);
        }

        Long delay = delays.get(warcFile);
        if (delay != null) {
          try {
            Thread.sleep(delay);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
          }
        }

        return super.indexArtifactsFromWarc(index, warcFile, result);
      } finally {
        events.add(new ParseEvent(warcFile, temporary, start, sequence.getAndIncrement()));
      }
    }

    List<ParseEvent> events() {
      synchronized (events) {
        return new ArrayList<>(events);
      }
    }

    void reset() {
      parseThreads.clear();
      synchronized (events) {
        events.clear();
      }
    }
  }

  // *******************************************************************************
  // * UTILITIES
  // *******************************************************************************

  private VolatileArtifactIndex newIndex() throws IOException {
    VolatileArtifactIndex idx = new VolatileArtifactIndex();
    idx.init();
    idx.start();
    return idx;
  }

  /** The result counts of a pass, in a form that makes a mismatch readable. */
  private static String describe(ReindexResult result) {
    return "warcsAttempted=" + result.getWarcsAttempted()
        + ", warcsSucceeded=" + result.getWarcsSucceeded()
        + ", warcsFailed=" + result.getWarcsFailedCount()
        + ", artifactsIndexed=" + result.getArtifactsIndexed()
        + ", artifactsSkipped=" + result.getArtifactsSkipped();
  }

  /** What the index holds for each of the given artifacts. */
  private static Map<String, String> snapshot(ArtifactIndex idx, List<String> uuids)
      throws IOException {
    Map<String, String> snapshot = new LinkedHashMap<>();

    for (String uuid : uuids) {
      Artifact artifact = idx.getArtifact(uuid);
      snapshot.put(uuid, artifact == null ? "(absent)"
          : artifact.getNamespace() + "|" + artifact.getAuid() + "|" + artifact.getUri()
              + "|v" + artifact.getVersion() + "|committed=" + artifact.getCommitted()
              + "|" + artifact.getStorageUrl());
    }

    return snapshot;
  }

  private static int countArtifacts(ArtifactIndex idx, String auid) throws IOException {
    return IterableUtils.size(idx.getArtifacts(NS, auid, true));
  }

  /** The UUIDs of a WARC written with one record deliberately corrupted. */
  private static final class CorruptedWarc {
    final List<String> goodUuids = new ArrayList<>();
    String badUuid;
  }

  private List<String> writeAuWarc(File basePath, String auid, String warcName, int numArtifacts)
      throws IOException {
    List<ArtifactSpec> specs = makeSpecs(auid, warcName, numArtifacts);
    writeWarc(auWarcPath(basePath, auid, warcName), specs);
    return uuidsOf(specs);
  }

  /** As {@link #writeAuWarc}, but returns the WARC's path rather than its UUIDs. */
  private Path writeAuWarcAt(File basePath, String auid, String warcName, int numArtifacts)
      throws IOException {
    List<ArtifactSpec> specs = makeSpecs(auid, warcName, numArtifacts);
    Path warcPath = auWarcPath(basePath, auid, warcName);
    writeWarc(warcPath, specs);
    return warcPath;
  }

  /**
   * Writes a WARC into the base path's temporary WARC directory. No journal is
   * written alongside it, which is the state a WARC left by a crash is in.
   */
  private List<String> writeTmpWarc(File basePath, String auid, String warcName, int numArtifacts)
      throws IOException {
    List<ArtifactSpec> specs = makeSpecs(auid, warcName, numArtifacts);

    Path tmpWarcDir = basePath.toPath().resolve(WarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH);
    Files.createDirectories(tmpWarcDir);

    writeWarc(tmpWarcDir.resolve(warcName + ".warc"), specs);

    return uuidsOf(specs);
  }

  /**
   * Writes a WARC whose record at {@code badRecordIndex} is corrupted such that
   * only that record fails to parse: its WARC {@code Content-Type} is rewritten to
   * a value {@code WarcArtifactDataUtil.fromWarcRecord()} rejects, in place, so no
   * record offset or length in the file changes.
   */
  private CorruptedWarc writeAuWarcWithBadRecord(File basePath, String auid, String warcName,
                                                 int numArtifacts, int badRecordIndex)
      throws IOException {

    List<ArtifactSpec> specs = makeSpecs(auid, warcName, numArtifacts);

    byte[] warcBytes = AbstractWarcArtifactDataStoreTest
        .createWarcFileFromSpecs(false, specs.toArray(new ArtifactSpec[0]));

    String warc = new String(warcBytes, StandardCharsets.ISO_8859_1);

    // The WARC record header of each record carries exactly one of these
    String marker = "application/http";
    List<Integer> offsets = new ArrayList<>();
    for (int at = warc.indexOf(marker); at >= 0; at = warc.indexOf(marker, at + 1)) {
      offsets.add(at);
    }

    assertEquals(numArtifacts, offsets.size(),
                 "expected one '" + marker + "' per record; the corruption below assumes it");

    // Same length, so every record offset and length in the file is untouched
    int at = offsets.get(badRecordIndex);
    String corrupted = warc.substring(0, at) + "application/junk"
        + warc.substring(at + marker.length());

    Path warcPath = auWarcPath(basePath, auid, warcName);
    Files.createDirectories(warcPath.getParent());
    Files.write(warcPath, corrupted.getBytes(StandardCharsets.ISO_8859_1));

    CorruptedWarc result = new CorruptedWarc();
    for (int i = 0; i < specs.size(); i++) {
      if (i == badRecordIndex) {
        result.badUuid = specs.get(i).getArtifactUuid();
      } else {
        result.goodUuids.add(specs.get(i).getArtifactUuid());
      }
    }

    log.debug("Wrote {} artifacts with record {} corrupted to {}",
              numArtifacts, badRecordIndex, warcPath);

    return result;
  }

  private Path auWarcPath(File basePath, String auid, String warcName) {
    return store.generateAUPath(basePath.toPath(), NS, auid)
        .resolve("artifacts_" + warcName + ".warc");
  }

  private void writeWarc(Path warcPath, List<ArtifactSpec> specs) throws IOException {
    Files.createDirectories(warcPath.getParent());

    byte[] warcBytes = AbstractWarcArtifactDataStoreTest
        .createWarcFileFromSpecs(false, specs.toArray(new ArtifactSpec[0]));

    Files.write(warcPath, warcBytes);

    log.debug("Wrote {} artifacts to {}", specs.size(), warcPath);
  }

  private List<ArtifactSpec> makeSpecs(String auid, String warcName, int numArtifacts) {
    List<ArtifactSpec> specs = new ArrayList<>(numArtifacts);

    for (int i = 0; i < numArtifacts; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(NS)
          .setAuid(auid)
          .setUrl("https://example.com/" + auid + "/" + warcName + "/" + i)
          .setVersion(1)
          .setContentLength(128);

      spec.generateContent();
      specs.add(spec);
    }

    return specs;
  }

  private static List<String> uuidsOf(List<ArtifactSpec> specs) {
    List<String> uuids = new ArrayList<>(specs.size());
    for (ArtifactSpec spec : specs) {
      uuids.add(spec.getArtifactUuid());
    }
    return uuids;
  }
}
