/*
 * Copyright (c) 2025, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.rs;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.ArtifactIndexVersion;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.ReindexResult;
import org.lockss.rs.io.storage.warc.AbstractWarcArtifactDataStoreTest;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
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

import static org.mockito.ArgumentMatchers.anyString;

/**
 * Tests for the targeted per-AU reindex trigger in {@link BaseLockssRepository}
 * (Component B): the {@code index/auids-to-reindex} list, its {@code .done}
 * resume file, and its sequencing behind the full-rebuild path.
 */
public class TestBaseLockssRepositoryTargetedReindex extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final String NS = BaseLockssRepository.TARGETED_REINDEX_NAMESPACE;
  private static final String AUID_A = "auidA";
  private static final String AUID_B = "auidB";
  private static final String AUID_C = "auidC";

  private File basePath;
  private File stateDir;

  private LocalWarcArtifactDataStore store;
  private VolatileArtifactIndex index;
  private TestableRepository repo;

  /**
   * A repository that can be driven without a {@link org.lockss.app.LockssDaemon}:
   * the recorded index version matches the target, so {@code updateIndexIfNeeded()}
   * only takes the full-rebuild path when the reindex token says to.
   */
  static class TestableRepository extends BaseLockssRepository {
    boolean fullReindexRan = false;

    TestableRepository(File stateDir, ArtifactIndex index, ArtifactDataStore store) {
      super(stateDir, index, store);
    }

    @Override
    public ReindexResult reindexArtifacts() throws IOException {
      fullReindexRan = true;
      return super.reindexArtifacts();
    }

    @Override
    protected ArtifactIndexVersion getLastRecordedArtifactIndexVersion() {
      return index.getArtifactIndexTargetVersion();
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    TimeBase.setSimulated(1700000000000L);

    basePath = getTempDir();
    stateDir = getTempDir();

    index = new VolatileArtifactIndex();
    index.init();
    index.start();

    store = Mockito.spy(new LocalWarcArtifactDataStore(new File[]{basePath}));

    repo = new TestableRepository(stateDir, index, store);
    store.setLockssRepository(repo);
    store.init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    store.stop();
    index.stop();
  }

  // *******************************************************************************
  // * LIST PARSING
  // *******************************************************************************

  @Test
  public void testIgnoresCommentsAndBlankLines() throws Exception {
    List<String> uuidsA = writeAuWarc(AUID_A, 2);

    writeList("# a comment",
              "",
              "   ",
              "  " + AUID_A + "  ",
              "# " + AUID_B);

    ReindexResult result = repo.reindexArtifactsInListedAus();

    assertNotNull(result);
    assertEquals(2, result.getArtifactsIndexed());
    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_A);
    Mockito.verify(store, Mockito.never()).reindexArtifactsInAu(index, NS, AUID_B);

    for (String uuid : uuidsA) {
      assertTrue(index.artifactExists(uuid));
    }
  }

  @Test
  public void testNoListIsANoOp() throws Exception {
    assertNull(repo.reindexArtifactsInListedAus());
    Mockito.verify(store, Mockito.never())
        .reindexArtifactsInAu(org.mockito.ArgumentMatchers.any(), anyString(), anyString());
  }

  /** A duplicated line must not cause the AU to be reindexed twice in one run. */
  @Test
  public void testDuplicateEntryIsReindexedOnce() throws Exception {
    writeAuWarc(AUID_A, 2);
    writeList(AUID_A, AUID_A);

    repo.reindexArtifactsInListedAus();

    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_A);
  }

  // *******************************************************************************
  // * RESUME
  // *******************************************************************************

  /** AUIDs already in the .done file are not reindexed again. */
  @Test
  public void testSkipsAuidsAlreadyDone() throws Exception {
    List<String> uuidsA = writeAuWarc(AUID_A, 2);
    List<String> uuidsB = writeAuWarc(AUID_B, 1);
    List<String> uuidsC = writeAuWarc(AUID_C, 1);

    writeList(AUID_A, AUID_B, AUID_C);
    writeDone(AUID_A);

    ReindexResult result = repo.reindexArtifactsInListedAus();

    Mockito.verify(store, Mockito.never()).reindexArtifactsInAu(index, NS, AUID_A);
    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_B);
    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_C);

    assertEquals(2, result.getArtifactsIndexed());

    for (String uuid : uuidsA) {
      assertFalse(index.artifactExists(uuid), "AU already in .done was reindexed");
    }
    for (String uuid : uuidsB) {
      assertTrue(index.artifactExists(uuid));
    }
    for (String uuid : uuidsC) {
      assertTrue(index.artifactExists(uuid));
    }
  }

  /**
   * A crash part way through the list resumes rather than restarting: no AU is
   * reindexed twice, and none is skipped.
   */
  @Test
  public void testResumesAfterACrashMidList() throws Exception {
    List<String> uuidsA = writeAuWarc(AUID_A, 2);
    List<String> uuidsB = writeAuWarc(AUID_B, 1);
    List<String> uuidsC = writeAuWarc(AUID_C, 3);

    writeList(AUID_A, AUID_B, AUID_C);

    // Crash (an Error, so it escapes the per-AU catch, as a real JVM death would)
    // on the second AU of the list.
    boolean[] crash = {true};
    Mockito.doAnswer(invocation -> {
      if (crash[0] && AUID_B.equals(invocation.getArgument(2))) {
        throw new Error("simulated crash");
      }
      return invocation.callRealMethod();
    }).when(store).reindexArtifactsInAu(org.mockito.ArgumentMatchers.any(), anyString(), anyString());

    assertThrows(Error.class, () -> repo.reindexArtifactsInListedAus());

    // A alone is recorded as done, and neither file has been rotated away
    assertEquals(List.of(AUID_A), readDone());
    assertTrue(listPath().toFile().exists(), "the list must survive a crash");
    for (String uuid : uuidsA) {
      assertTrue(index.artifactExists(uuid));
    }
    for (String uuid : uuidsB) {
      assertFalse(index.artifactExists(uuid));
    }

    // Restart
    crash[0] = false;
    ReindexResult result = repo.reindexArtifactsInListedAus();

    // A is not reindexed a second time; B and C are picked up
    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_A);
    Mockito.verify(store, Mockito.times(2)).reindexArtifactsInAu(index, NS, AUID_B);
    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_C);

    assertEquals(4, result.getArtifactsIndexed());

    for (String uuid : uuidsB) {
      assertTrue(index.artifactExists(uuid));
    }
    for (String uuid : uuidsC) {
      assertTrue(index.artifactExists(uuid));
    }

    // The whole list has now been attempted, so both files are rotated, not deleted
    assertFalse(listPath().toFile().exists());
    assertFalse(donePath().toFile().exists());
    assertEquals(1, rotatedCount(BaseLockssRepository.AUIDS_TO_REINDEX_FILE));
    assertEquals(1, rotatedCount(BaseLockssRepository.AUIDS_TO_REINDEX_DONE_FILE));
  }

  /**
   * An AUID with no AU directory anywhere is reported, stays out of .done, and does
   * not stop the rest of the list.
   */
  @Test
  public void testUnrecoverableAuidIsReportedAndDoesNotBlockTheList() throws Exception {
    List<String> uuidsC = writeAuWarc(AUID_C, 2);

    writeList(AUID_A, AUID_C);

    ReindexResult result = repo.reindexArtifactsInListedAus();

    assertTrue(result.hasFailures());
    assertEquals(List.of(AUID_A), result.getMissingAus());
    assertEquals(2, result.getArtifactsIndexed());

    for (String uuid : uuidsC) {
      assertTrue(index.artifactExists(uuid));
    }

    // Rotated with the run stamp anyway: failures are reported, not retried.
    assertFalse(listPath().toFile().exists());
    assertEquals(1, rotatedCount(BaseLockssRepository.AUIDS_TO_REINDEX_FILE));

    // The failed AUID is not in the rotated .done file
    assertFalse(readOnlyRotated(BaseLockssRepository.AUIDS_TO_REINDEX_DONE_FILE)
                    .contains(AUID_A));
  }

  // *******************************************************************************
  // * SEQUENCING AGAINST THE FULL REBUILD
  // *******************************************************************************

  /**
   * A full rebuild that finishes CLEANLY supersedes the targeted list. It walks
   * findWarcs() over every base path, a superset of the AU directories the targeted
   * pass would walk, so the list is rotated -- preserved as history, but not left
   * behind to arm a redundant pass on the next startup.
   */
  @Test
  public void testCleanFullRebuildSupersedesAndRotatesTheList() throws Exception {
    writeAuWarc(AUID_A, 2);
    writeList(AUID_A, AUID_B);

    // The reindex token routes startup to the full rebuild
    FileUtils.touch(stateDir.toPath().resolve(BaseLockssRepository.REINDEXING_STATE_FILE).toFile());

    repo.updateIndexIfNeeded();

    assertTrue(repo.fullReindexRan);
    Mockito.verify(store, Mockito.never())
        .reindexArtifactsInAu(org.mockito.ArgumentMatchers.any(), anyString(), anyString());

    assertFalse(listPath().toFile().exists(),
                "a clean full rebuild must consume the list");
    assertEquals(1, rotatedCount(BaseLockssRepository.AUIDS_TO_REINDEX_FILE));

    // ...and the full rebuild deleted its token
    assertFalse(stateDir.toPath().resolve(BaseLockssRepository.REINDEXING_STATE_FILE)
                    .toFile().exists());
  }

  /**
   * A full rebuild that finishes WITH FAILURES may not have covered the listed AUs:
   * E.7 reports failures but still returns normally and deletes the token, so the
   * result is the only signal. The list is kept for a later startup.
   */
  @Test
  public void testFullRebuildWithFailuresRetainsTheList() throws Exception {
    writeAuWarc(AUID_A, 2);
    writeList(AUID_A, AUID_B);

    ReindexResult failing = new ReindexResult();
    failing.addWarcFailure(basePath.toPath().resolve("unreadable.warc"), "boom");
    Mockito.doReturn(failing).when(store).reindexArtifacts(index);

    FileUtils.touch(stateDir.toPath().resolve(BaseLockssRepository.REINDEXING_STATE_FILE).toFile());

    repo.updateIndexIfNeeded();

    assertTrue(repo.fullReindexRan);
    Mockito.verify(store, Mockito.never())
        .reindexArtifactsInAu(org.mockito.ArgumentMatchers.any(), anyString(), anyString());

    assertTrue(listPath().toFile().exists(),
               "a full rebuild that did not finish cleanly must retain the list");
    assertEquals(0, rotatedCount(BaseLockssRepository.AUIDS_TO_REINDEX_FILE));
  }

  /**
   * With no full rebuild pending, startup runs the targeted pass.
   */
  @Test
  public void testStartupRunsTheTargetedPass() throws Exception {
    List<String> uuidsA = writeAuWarc(AUID_A, 2);

    writeList(AUID_A);

    repo.updateIndexIfNeeded();

    assertFalse(repo.fullReindexRan);
    Mockito.verify(store, Mockito.times(1)).reindexArtifactsInAu(index, NS, AUID_A);

    for (String uuid : uuidsA) {
      assertTrue(index.artifactExists(uuid));
    }

    assertFalse(listPath().toFile().exists());
    assertEquals(1, rotatedCount(BaseLockssRepository.AUIDS_TO_REINDEX_FILE));
  }

  /**
   * E.7: the {@code index/reindexing} token is deleted even when the pass finished
   * with failures, and a restart therefore does not reindex again.
   */
  @Test
  public void testReindexTokenIsDeletedDespiteFailures() throws Exception {
    writeAuWarc(AUID_A, 2);

    Path badWarc = store.generateAUPath(basePath.toPath(), NS, AUID_A)
        .resolve("artifacts_broken.warc");
    Files.write(badWarc, "definitely not a WARC".getBytes(StandardCharsets.UTF_8));
    Mockito.doThrow(new IOException("simulated unreadable WARC"))
        .when(store).getInputStreamAndSeek(badWarc, 0);

    Path token = stateDir.toPath().resolve(BaseLockssRepository.REINDEXING_STATE_FILE);
    FileUtils.touch(token.toFile());

    repo.updateIndexIfNeeded();

    assertTrue(repo.fullReindexRan);
    assertFalse(token.toFile().exists(),
                "the reindex token must be deleted even when WARCs failed");

    String report = readOnlyRotated(WarcArtifactDataStore.REINDEX_FAILURES_FILE);
    assertTrue(report.contains(badWarc.toString()), "unexpected report: " + report);

    // Restart: nothing left to trigger a reindex
    repo.fullReindexRan = false;
    repo.updateIndexIfNeeded();
    assertFalse(repo.fullReindexRan, "a restart must not reindex again");
  }

  // *******************************************************************************
  // * UTILITIES
  // *******************************************************************************

  private Path listPath() {
    return stateDir.toPath().resolve(BaseLockssRepository.AUIDS_TO_REINDEX_FILE);
  }

  private Path donePath() {
    return stateDir.toPath().resolve(BaseLockssRepository.AUIDS_TO_REINDEX_DONE_FILE);
  }

  private void writeList(String... lines) throws IOException {
    Files.createDirectories(listPath().getParent());
    Files.write(listPath(), List.of(lines));
  }

  private void writeDone(String... auids) throws IOException {
    Files.createDirectories(donePath().getParent());
    Files.write(donePath(), List.of(auids));
  }

  private List<String> readDone() throws IOException {
    return Files.readAllLines(donePath());
  }

  /** Writes a WARC of {@code numArtifacts} artifacts into the AU's directory. */
  private List<String> writeAuWarc(String auid, int numArtifacts) throws IOException {
    Path auPath = store.generateAUPath(basePath.toPath(), NS, auid);
    Files.createDirectories(auPath);

    List<ArtifactSpec> specs = new ArrayList<>();
    List<String> uuids = new ArrayList<>();

    for (int i = 0; i < numArtifacts; i++) {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setNamespace(NS)
          .setAuid(auid)
          .setUrl("https://example.com/" + auid + "/" + i)
          .setVersion(1)
          .setContentLength(128);

      spec.generateContent();

      specs.add(spec);
      uuids.add(spec.getArtifactUuid());
    }

    byte[] warcBytes = AbstractWarcArtifactDataStoreTest
        .createWarcFileFromSpecs(false, specs.toArray(new ArtifactSpec[0]));

    Files.write(auPath.resolve("artifacts_" + auid + ".warc"), warcBytes);

    return uuids;
  }

  private static final String DONE_NAME =
      Path.of(BaseLockssRepository.AUIDS_TO_REINDEX_DONE_FILE).getFileName().toString();

  private List<String> rotatedNames(String stateFile) throws IOException {
    Path path = stateDir.toPath().resolve(stateFile);
    Path dir = path.getParent();
    String prefix = path.getFileName() + ".";

    if (!dir.toFile().isDirectory()) {
      return List.of();
    }

    try (var paths = Files.list(dir)) {
      return paths
          .map(p -> p.getFileName().toString())
          .filter(name -> name.startsWith(prefix))
          // "auids-to-reindex.done" and its rotated copies are themselves
          // "<list>.<suffix>" names; they are not rotations of the list.
          .filter(name -> prefix.startsWith(DONE_NAME) || !name.startsWith(DONE_NAME))
          .sorted()
          .toList();
    }
  }

  private int rotatedCount(String stateFile) throws IOException {
    return rotatedNames(stateFile).size();
  }

  private String readOnlyRotated(String stateFile) throws IOException {
    Path path = stateDir.toPath().resolve(stateFile);
    List<String> names = rotatedNames(stateFile);
    assertEquals(1, names.size(), "expected exactly one rotated copy: " + names);
    return Files.readString(path.resolveSibling(names.get(0)));
  }
}
