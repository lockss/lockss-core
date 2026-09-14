/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

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

package org.lockss.rs;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.lockss.app.LockssDaemon;
import org.lockss.log.L4JLogger;
import org.lockss.rs.ErrorHarness.ErrorInjectionRule;
import org.lockss.rs.ErrorHarness.TestingErrorOp;
import org.lockss.rs.io.index.AbstractArtifactIndex;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.ArtifactIndexVersion;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.ArtifactDataStoreVersion;
import org.lockss.rs.io.storage.ReindexResult;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataUtil;
import org.lockss.util.BuildInfo;
import org.lockss.util.ByteArray;
import org.lockss.util.StreamUtil;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.io.FileUtil;
import org.lockss.util.jms.JmsFactory;
import org.lockss.util.rest.repo.LockssArtifactAlreadyExistsException;
import org.lockss.util.rest.repo.LockssNoSuchArtifactIdException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ImportStatusIterable;
import org.lockss.util.rest.repo.util.JmsFactorySource;
import org.lockss.util.rest.repo.util.LockssRepositoryUtil;
import org.lockss.util.storage.StorageInfo;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeUtil;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Base implementation of the LOCKSS Repository service.
 */
public class BaseLockssRepository implements LockssRepository, JmsFactorySource {

  private final static L4JLogger log = L4JLogger.getLogger();
  private static final byte[] GZIP_MEMBER_ID = ByteArray.fromHexString("1F8B");

  private File repoStateDir;

  private long timeSpentReiterating = 0;

  protected ArtifactDataStore store;
  protected ArtifactIndex index;
  protected JmsFactory jmsFact;

  protected ScheduledExecutorService scheduledExecutor =
      Executors.newSingleThreadScheduledExecutor();

  private static final BuildInfo BUILD_INFO = BuildInfo.getBuildInfoFor("lockss-core")
      .orElseThrow(() -> new IllegalStateException("Could not determine LOCKSS repository version"));

  public static String REPOSITORY_VERSION = BUILD_INFO.getBuildPropertyInst("build.version");

  /**
   * Create a LOCKSS repository with the provided artifact index and storage layers.
   *
   * @param index An instance of {@code ArtifactIndex}.
   * @param store An instance of {@code ArtifactDataStore}.
   */
  protected BaseLockssRepository(ArtifactIndex index, ArtifactDataStore store) {
    if (index == null || store == null) {
      throw new IllegalArgumentException("Cannot start repository with a null artifact index or store");
    }

    setArtifactIndex(index);
    setArtifactDataStore(store);
  }

  /** No-arg constructor for subclasses */
  protected BaseLockssRepository() throws IOException {
  }

  /**
   * Constructor.
   *
   * @param repoStateDir A {@link Path} containing the path to the state of this repository.
   * @param index An instance of {@link ArtifactIndex}.
   * @param store An instance of {@link ArtifactDataStore}.
   * @param store
   */
  public BaseLockssRepository(File repoStateDir, ArtifactIndex index, ArtifactDataStore store) {
    this(index, store);
    setRepositoryStateDir(repoStateDir);
  }

  /**
   * Validates a namespace.
   *
   * @param namespace A {@link String} containing the namespace to validate.
   * @throws IllegalArgumentException Thrown if the namespace did not pass validation.
   */
  private static void validateNamespace(String namespace) throws IllegalArgumentException {
    if (!LockssRepositoryUtil.validateNamespace(namespace)) {
      throw new IllegalArgumentException("Invalid namespace: " + namespace);
    }
  }

  /**
   * Getter for the repository state directory.
   *
   * @return A {@link Path} containing the path to the repository state directory.
   */
  public Path getRepositoryStateDirPath() {
    return repoStateDir.toPath();
  }

  /**
   * Setter for the repository state directory.
   *
   * @param dir A {@link File} containing the path to the repository state directory.
   */
  protected void setRepositoryStateDir(File dir) {
    repoStateDir = dir;
  }

  public final static String REINDEXING_STATE_FILE = "index/reindex";

  /**
   * List of AUIDs to reindex individually, one bare AUID per line. Lines starting
   * with '#' are comments; blank lines are ignored. Written by the installer's
   * {@code extract-reindex-auids} script and consumed at startup.
   */
  public final static String AUIDS_TO_REINDEX_FILE = "index/auids-to-reindex";

  /**
   * AUIDs from {@link #AUIDS_TO_REINDEX_FILE} that have been reindexed. Appended to
   * and flushed after each AU, so a crash part way through the list resumes rather
   * than restarting.
   */
  public final static String AUIDS_TO_REINDEX_DONE_FILE = "index/auids-to-reindex.done";

  /**
   * Namespace the targeted reindex list is interpreted in. The list carries bare
   * AUIDs with no namespace column, matching the "lockss, &lt;auid&gt;" in the
   * migration errors it is generated from.
   */
  public final static String TARGETED_REINDEX_NAMESPACE = "lockss";

  /**
   * Reindexes every WARC under every configured base path.
   *
   * @return the {@link ReindexResult} of the pass, so callers can tell a clean
   *         run from one that finished with failures. {@code reindexArtifacts()}
   *         deletes the reindexing token either way (E.7 reports failures rather
   *         than retrying them), so the result is the only signal available.
   */
  public ReindexResult reindexArtifacts() throws IOException {
    log.info("Reindexing artifacts");

    // (Re)enter reindexing state
    Path reindexingStateFilePath = getRepositoryStateDirPath()
        .resolve(REINDEXING_STATE_FILE);

    File reindexingStateFile = reindexingStateFilePath.toFile();
    FileUtils.touch(reindexingStateFile);

    // Reindex artifacts in the data store to index
    long reindexStart = TimeBase.nowMs();
    ReindexResult result = store.reindexArtifacts(index);
    log.info("Finished reindex in {}",
        TimeUtil.timeIntervalToString(TimeBase.msSince(reindexStart)));

    if (result != null && result.hasFailures()) {
      log.error("Reindex completed with failures: {} of {} WARCs could not be read; " +
                "see the reindex-failures report in {}",
                result.getWarcsFailedCount(), result.getWarcsAttempted(),
                getRepositoryStateDirPath()
                    .resolve(WarcArtifactDataStore.DATASTORE_STATE_DIR));
    }

    // Exit reindexing state. Deliberately on the normal return path only: if the
    // reindex threw, the token must survive so the pass resumes at next startup.
    FileUtil.safeDeleteFile(reindexingStateFile);

    return result;
  }

  /**
   * Reindexes the AUs named in {@link #AUIDS_TO_REINDEX_FILE}, if that file exists.
   * <p>
   * This is the recovery path for AUs whose index rows were lost by the
   * pre-{@code 516d8358} {@code finishBulkStore()} bug. It is a startup check only,
   * like every other repository state signal; there is no polling.
   * <p>
   * Each AU that succeeds is appended to {@link #AUIDS_TO_REINDEX_DONE_FILE} and
   * flushed immediately, so a crash part way through the list resumes rather than
   * restarting. An AUID that fails stays out of the .done file and is logged at
   * ERROR. Once every entry has been <em>attempted</em> -- not necessarily
   * succeeded -- both files are rotated with the run stamp rather than deleted, so
   * that failures are not retried forever; regenerating the list is an explicit
   * operator action.
   *
   * @return A {@link ReindexResult} aggregating every AU of the list, or
   *         {@code null} if there was no list to process.
   */
  protected ReindexResult reindexArtifactsInListedAus() throws IOException {
    Path listPath = getRepositoryStateDirPath().resolve(AUIDS_TO_REINDEX_FILE);

    if (!listPath.toFile().exists()) {
      return null;
    }

    if (!(store instanceof WarcArtifactDataStore wads)) {
      log.error("Targeted reindex list found but the data store does not support " +
                "per-AU reindex; ignoring [file: {}, store: {}]",
                listPath, store.getClass().getName());
      return null;
    }

    List<String> auids = readAuidsToReindex(listPath);

    if (auids.isEmpty()) {
      log.info("Targeted reindex list is empty; nothing to do [file: {}]", listPath);
    }

    Path donePath = getRepositoryStateDirPath().resolve(AUIDS_TO_REINDEX_DONE_FILE);
    Set<String> done = new LinkedHashSet<>(readAuidsToReindex(donePath));

    ReindexResult aggregate = new ReindexResult();
    int ausProcessed = 0;
    int ausSkipped = 0;
    int ausFailed = 0;

    long start = TimeBase.nowMs();

    FileUtils.forceMkdirParent(donePath.toFile());

    try (BufferedWriter doneWriter =
             Files.newBufferedWriter(donePath, StandardOpenOption.CREATE,
                                     StandardOpenOption.APPEND)) {

      for (String auid : auids) {
        // Covers both a previous run's .done entries and a duplicated line in
        // this run's input.
        if (done.contains(auid)) {
          log.debug("Already reindexed, skipping [auid: {}]", auid);
          ausSkipped++;
          continue;
        }

        try {
          ReindexResult auResult =
              wads.reindexArtifactsInAu(index, TARGETED_REINDEX_NAMESPACE, auid);

          aggregate.add(auResult);

          if (auResult.isSuccessful()) {
            done.add(auid);
            doneWriter.write(auid);
            doneWriter.newLine();
            doneWriter.flush();
            ausProcessed++;
          } else {
            ausFailed++;
            log.error("Could not fully reindex AU; leaving it out of {} [auid: {}, result: {}]",
                      AUIDS_TO_REINDEX_DONE_FILE, auid, auResult);
          }
        } catch (Exception e) {
          ausFailed++;
          aggregate.addAuFailure(auid, e);
          log.error("Could not reindex AU [auid: {}]", auid, e);
        }
      }
    }

    // Every entry has been attempted: rotate the ledger and failure report under
    // one run stamp, then the list and its .done file under another, rather than
    // deleting any of them.
    wads.finishReindexRun(aggregate);
    WarcArtifactDataStore.rotateWithRunStamp(listPath, donePath);

    log.info("Targeted reindex finished in {}: {} AUs reindexed, {} already done, " +
             "{} failed, {} artifacts indexed",
             TimeUtil.timeIntervalToString(TimeBase.msSince(start)),
             ausProcessed, ausSkipped, ausFailed, aggregate.getArtifactsIndexed());

    if (ausFailed > 0) {
      log.error("Targeted reindex could not reindex {} of {} AUs; see the errors above",
                ausFailed, auids.size());
    }

    return aggregate;
  }

  /**
   * Reads a file of one bare AUID per line. Lines starting with '#' are comments,
   * blank lines are ignored and whitespace is trimmed. A missing file reads as an
   * empty list.
   */
  private static List<String> readAuidsToReindex(Path path) throws IOException {
    List<String> auids = new ArrayList<>();

    if (!path.toFile().exists()) {
      return auids;
    }

    try (BufferedReader reader = Files.newBufferedReader(path)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String auid = line.trim();
        if (auid.isEmpty() || auid.startsWith("#")) {
          continue;
        }
        auids.add(auid);
      }
    }

    return auids;
  }

  public ScheduledExecutorService getScheduledExecutorService() {
    return scheduledExecutor;
  }

  @Override
  public void initRepository() throws IOException {
    try {
      log.debug("Waiting for LockssApp to become ready");
      LockssDaemon.getLockssDaemon().waitUntilAppRunning();

      log.info("Initializing LOCKSS repository");

      // Initialize the components
      index.init();
      store.init();

      updateDatastoreIfNeeded();
      updateIndexIfNeeded();

      // Start the components
      index.start();
      store.start();
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while waiting for LOCKSS daemon", e);
    }
  }

  private void updateDatastoreIfNeeded() throws IOException {
    ArtifactDataStoreVersion onDiskVersion = getLastRecordedArtifactDataStoreVersion();
    ArtifactDataStoreVersion targetVersion = store.getDataStoreTargetVersion();

    if (!onDiskVersion.equals(targetVersion)) {
      if (onDiskVersion == ArtifactDataStoreVersion.UNKNOWN) {
        log.debug("Initializing data store for the first time");
        ((WarcArtifactDataStore)store).updateDatastoreToVersion(0, targetVersion.getDatastoreVersion());
      } else if (!onDiskVersion.getDatastoreType().equals(targetVersion.getDatastoreType())) {
        throw new UnsupportedOperationException("Switching data stores is not supported");
      } else if (onDiskVersion.getDatastoreVersion() < targetVersion.getDatastoreVersion()) {
        ((WarcArtifactDataStore) store).updateDatastoreToVersion(
            onDiskVersion.getDatastoreVersion(),
            targetVersion.getDatastoreVersion());
      }
    }
  }

  protected ArtifactDataStoreVersion getLastRecordedArtifactDataStoreVersion() {
    return ArtifactDataStoreVersion.UNKNOWN;
  }

  // Protected rather than private so that the startup sequencing between a full
  // rebuild and the targeted per-AU pass can be tested without standing up a
  // LockssDaemon.
  protected void updateIndexIfNeeded() throws IOException {
    ArtifactIndexVersion onDiskVersion = getLastRecordedArtifactIndexVersion();
    ArtifactIndexVersion targetVersion = index.getArtifactIndexTargetVersion();

    if (log.isDebug2Enabled()) {
      log.debug2("index = {}", index);
      log.debug2("index.onDiskVersion = {}", onDiskVersion);
      log.debug2("index.targetVersion = {}", targetVersion);
    }

    boolean indexChanged = false;

    if (!onDiskVersion.equals(targetVersion)) {
      // Either type changed, version changed, or both changed:
      // (type changed, version changed)
      // t t --- sync index
      // t f --- sync index
      // f t --- sync index if previousVersion < currentVersion
      // f f --- nothing to do

      if (onDiskVersion == ArtifactIndexVersion.UNKNOWN) {
        log.debug("Initializing index for the first time");
        ((AbstractArtifactIndex)index).updateIndexToVersion(0, targetVersion.getIndexVersion());
        indexChanged = true;
      } else if (!onDiskVersion.getIndexType().equals(targetVersion.getIndexType())) {
        log.debug("Switching index: {} -> {}",
            onDiskVersion.getIndexType(), targetVersion.getIndexType());
        // Q: Is this right?
        ((AbstractArtifactIndex)index).updateIndexToVersion(0, targetVersion.getIndexVersion());
        indexChanged = true;
      } else if (onDiskVersion.getIndexVersion() < targetVersion.getIndexVersion()) {
        ((AbstractArtifactIndex) index).updateIndexToVersion(
            onDiskVersion.getIndexVersion(),
            targetVersion.getIndexVersion());
        indexChanged = true;
      }
    }

    // 1. Touch reindex token — crash here: token exists, shouldStartOrResumeReindex triggers reindex
    // 2. Clear index — crash here: token exists + stale base paths file, so next startup re-detects the change, re-clears, and reindexes
    // 3. Record new base paths — crash here: token exists, reindex resumes via shouldStartOrResumeReindex
    // 4. Reindex — deletes token on completion
    boolean contentPathListChanged = false;
    if (store instanceof WarcArtifactDataStore wads) {
      if (wads.didConfiguredBasePathsChange()) {
        log.info("Content base paths changed; clearing index in preparation for a reindex");
        File reindexTokenFile = getRepositoryStateDirPath()
            .resolve(REINDEXING_STATE_FILE).toFile();
        FileUtils.touch(reindexTokenFile);
        index.clearIndex();
        wads.clearReindexState();
        wads.recordConfiguredBasePaths();
        contentPathListChanged = true;
      }
    }

    if (indexChanged || shouldStartOrResumeReindex() || isReindexWanted() || contentPathListChanged) {
      ReindexResult fullResult = reindexArtifacts();

      // A full rebuild walks findWarcs() over every base path, which is a superset
      // of the targeted AU directories (<basePath>/ns/<ns>/au-<md5>/), so a CLEAN
      // full pass satisfies the targeted list outright. Rotate the list under a run
      // stamp -- the same ending the targeted pass gives it -- rather than leaving
      // it behind to arm a redundant pass on the next startup.
      //
      // A pass that finished with failures is different: E.7 reports failures but
      // still returns normally and deletes the token, so the listed AUs may not have
      // been covered. There the list is kept for a later startup.
      Path listPath = getRepositoryStateDirPath().resolve(AUIDS_TO_REINDEX_FILE);

      if (listPath.toFile().exists()) {
        if (fullResult == null || fullResult.hasFailures()) {
          log.warn("A full reindex ran this startup but did not finish cleanly; " +
                   "leaving {} for a later startup [result: {}]",
                   AUIDS_TO_REINDEX_FILE, fullResult);
        } else {
          WarcArtifactDataStore.rotateWithRunStamp(listPath,
              getRepositoryStateDirPath().resolve(AUIDS_TO_REINDEX_DONE_FILE));
          log.info("A full reindex superseded the targeted list; rotated {}",
                   AUIDS_TO_REINDEX_FILE);
        }
      }
    } else {
      // Sequenced after the full-rebuild branches so a whole-store reindex and a
      // targeted one can never interleave.
      reindexArtifactsInListedAus();
    }
  }

  protected boolean isReindexWanted() {
    return false;
  }

  public boolean shouldStartOrResumeReindex() {
    if (getRepositoryStateDirPath() == null) {
      throw new IllegalStateException("Missing repository state directory");
    }

    // Path to reindex state file
    Path reindexingStateFilePath = getRepositoryStateDirPath()
        .resolve(REINDEXING_STATE_FILE);

    File reindexingStateFile = reindexingStateFilePath.toFile();

    return reindexingStateFile.exists();
  }

  protected ArtifactIndexVersion getLastRecordedArtifactIndexVersion() {
    return ArtifactIndexVersion.UNKNOWN;
  }

  /**
   * Returns a boolean indicating whether this repository is ready.
   * <p>
   * Delegates to readiness of internal artifact index and data store components.
   *
   * @return
   */
  @Override
  public boolean isReady() {
    return store.isReady() && index.isReady();
  }

  @Override
  public void shutdownRepository() throws InterruptedException {
    log.info("Shutting down repository");

    index.stop();
    store.stop();

    scheduledExecutor.shutdown();
    scheduledExecutor.awaitTermination(1, TimeUnit.MINUTES);
  }

  /** JmsFactorySource method to store a JmsFactory for use by (a user of)
   * this index.
   * @param fact a JmsFactory for creating JmsProducer and JmsConsumer
   * instances.
   */
  @Override
  public void setJmsFactory(JmsFactory fact) {
    this.jmsFact = fact;
  }

  /** JmsFactorySource method to provide a JmsFactory.
   * @return a JmsFactory for creating JmsProducer and JmsConsumer
   * instances.
   */
  public JmsFactory getJmsFactory() {
    return jmsFact;
  }

  /**
   * Returns information about the repository's storage areas
   *
   * @return A {@code RepositoryInfo}
   * @throws IOException if there are problems getting the repository data.
   */
  @Override
  public RepositoryInfo getRepositoryInfo() throws IOException {
    StorageInfo ind = null;
    StorageInfo sto = null;
    try {
      ind = index.getStorageInfo();
    } catch (Exception e) {
      log.warn("Couldn't get index space", e);
    }
    try {
      sto = store.getStorageInfo();
    } catch (Exception e) {
      log.warn("Couldn't get store space", e);
    }

    RepositoryStatistics repoStats = new RepositoryStatistics();
    repoStats.setTimeSpentReiteratingIterators(timeSpentReiterating);

    return new RepositoryInfo(sto, ind)
        .repositoryStatistics(repoStats);
  }

  @Override
  public StorageInfo getStorageInfo() throws IOException {
    try {
      return store.getStorageInfo();
    } catch (Exception e) {
      log.error("Couldn't get artifact data store space", e);
      throw new IOException("Could not get artifact data store space", e);
    }
  }

  /**
   * Adds an artifact to this LOCKSS repository.
   *
   * @param artifactData {@code ArtifactData} instance to add to this LOCKSS repository.
   * @return The artifact ID of the newly added artifact.
   * @throws IOException
   */
  @Override
  public Artifact addArtifact(ArtifactData artifactData) throws IOException {
    if (artifactData == null) {
      throw new IllegalArgumentException("Null ArtifactData");
    }

    ArtifactIdentifier artifactId = artifactData.getIdentifier();

    index.acquireVersionLock(artifactId.getArtifactStem());

    try {
      int nextVersion = 1;

      boolean hasVersion =
          artifactId.getVersion() != null && artifactId.getVersion() > 0;

      if (hasVersion) {
        // Check whether the repository already has this artifact
        Artifact result = index.getArtifactVersion(
            artifactId.getNamespace(),
            artifactId.getAuid(),
            artifactId.getUri(),
            artifactId.getVersion(),
            true);

        if (result != null) {
          if (result.isCommitted()) {
            throw new LockssArtifactAlreadyExistsException(artifactId);
          }

          // Delete the existing artifact, allowing it to effectively perform
          // a replacement with the given artifact
          index.deleteArtifact(result.getUuid());
        }

        nextVersion = artifactId.getVersion();
      } else {
        // Retrieve latest version in this URL lineage
        Artifact result = index.getArtifact(
            artifactId.getNamespace(),
            artifactId.getAuid(),
            artifactId.getUri(),
            true);

        if (result != null) {
          nextVersion = result.getVersion() + 1;
        }
      }

      // Create a new artifact identifier for this artifact
      ArtifactIdentifier newId = new ArtifactIdentifier(
          UUID.randomUUID().toString(), // FIXME: Artifact ID collision unlikely but possible
          artifactId.getNamespace(),
          artifactId.getAuid(),
          artifactId.getUri(),
          nextVersion);

      injectTestingAction(newId, TestingErrorOp.AddArtifact);
      // Set the new artifact identifier
      artifactData.setIdentifier(newId);

      // Set collection date if it is not set
      long collectionDate = artifactData.getCollectionDate();
      if (collectionDate < 0) {
        artifactData.setCollectionDate(TimeBase.nowMs());
      }

      // Add the artifact the data store and index
      return store.addArtifactData(artifactData);
    } finally {
      index.releaseVersionLock(artifactId.getArtifactStem());
    }
  }

  /**
   * Imports artifacts from an archive into this LOCKSS repository.
   *
   * @param namespace A {@link String} containing the namespace of the artifacts.
   * @param auId         A {@link String} containing the AUID of the artifacts.
   * @param inputStream  The {@link InputStream} of the archive.
   * @param type         A {@link ArchiveType} indicating the type of archive.
   * @param storeDuplicate A {@code boolean} indicating whether new versions of artifacets whose content would be identical to the previous version should be stored
   * @param excludeStatusPattern    A {@link String} containing a regexp.  WARC records whose HTTP response status code matches will not be added to the repository
   * @return
   */
  @Override
  public ImportStatusIterable addArtifacts(String namespace, String auId, InputStream inputStream,
                                           ArchiveType type, boolean storeDuplicate, String excludeStatusPattern) throws IOException {

    validateNamespace(namespace);

    if (type != ArchiveType.WARC) {
      throw new NotImplementedException("Archive type not supported: " + type);
    }

    try {
      // This doesn't work because it appears to consume the first byte?
      // PeekableInputStream input = new PeekableInputStream(inputStream, GZIP_MEMBER_ID.length);
      // boolean isCompressed = input.peek(GZIP_MEMBER_ID);

      BufferedInputStream input = new BufferedInputStream(inputStream);

      // Read two bytes and compare to GZIP Member ID to determine isCompressed
      input.mark(GZIP_MEMBER_ID.length);
      byte[] buf = new byte[GZIP_MEMBER_ID.length];
      if (StreamUtil.readBytes(input, buf, GZIP_MEMBER_ID.length) != GZIP_MEMBER_ID.length)
        throw new IOException("Could not read magic number");
      boolean isCompressed = Arrays.equals(GZIP_MEMBER_ID, buf);
      input.reset();

      ArchiveReader archiveReader = isCompressed ?
          new WarcArtifactDataStore.CompressedWARCReader("archive.warc.gz", input) :
          new WarcArtifactDataStore.UncompressedWARCReader("archive.warc", input);

      archiveReader.setDigest(false);
      archiveReader.setStrict(true);

      try (DeferredTempFileOutputStream out =
               new DeferredTempFileOutputStream((int) (16 * FileUtils.ONE_MB), (String) null)) {

        ObjectMapper objMapper = new ObjectMapper();
        objMapper.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        ObjectWriter objWriter = objMapper.writerFor(ImportStatus.class);

        Pattern excludePat =
          StringUtils.isEmpty(excludeStatusPattern) ? null : Pattern.compile(excludeStatusPattern);

        // ArchiveReader is an iterable over ArchiveRecord objects
        for (ArchiveRecord record : archiveReader) {
          ImportStatus status = new ImportStatus();

          try {
            ArchiveRecordHeader header = record.getHeader();
            String realUri = realRecordUri(header.getUrl());

            status.setWarcId((String) header.getHeaderValue(WARCConstants.HEADER_KEY_ID));
            status.setOffset(header.getOffset());
            status.url(realUri);

            // Read WARC record type from record headers
            WARCConstants.WARCRecordType recordType =
                WARCConstants.WARCRecordType.valueOf((String) header.getHeaderValue(WARCConstants.HEADER_KEY_TYPE));

            if (!(recordType == WARCConstants.WARCRecordType.response ||
                recordType == WARCConstants.WARCRecordType.resource)) {
              continue;
            }

            // Transform WARC record to ArtifactData
            ArtifactData ad = WarcArtifactDataUtil.fromArchiveRecord(record);
            assert ad != null;

            if (excludePat != null && ad.getHttpStatus() != null)  {
              String statusCode = Integer.toString(ad.getHttpStatus().getStatusCode());
              if (excludePat.matcher(statusCode).matches()) {
                status.setStatus(ImportStatus.StatusEnum.EXCLUDED);
                objWriter.writeValue(out, status);
                continue;
              }
            }

            ArtifactIdentifier aid = ad.getIdentifier();
            aid.setNamespace(namespace);
            aid.setAuid(auId);
            aid.setUri(realUri);

            // TODO: Write to permanent storage directly
            // (But that conflicts with dup detection)
            Artifact artifact = addArtifact(ad);
            Artifact dup = null;
            if (!storeDuplicate) {
              dup = LockssRepositoryUtil.getIdenticalPreviousVersion(this, artifact);
            }
            if (dup != null) {
              try {
                deleteArtifact(artifact);
                status.setArtifactUuid(dup.getUuid());
                status.setDigest(dup.getContentDigest());
                status.setVersion(dup.getVersion());
                status.setStatus(ImportStatus.StatusEnum.DUPLICATE);
              } catch (Exception e) {
                log.error("Error deleting duplicate artifact: {}", artifact, e);
              }
            } else {
              commitArtifact(artifact);

              status.setArtifactUuid(artifact.getUuid());
              status.setDigest(artifact.getContentDigest());
              status.setVersion(artifact.getVersion());
              status.setStatus(ImportStatus.StatusEnum.OK);
            }
          } catch (Exception e) {
            log.error("Could not import artifact from archive", e);
            status.setStatus(ImportStatus.StatusEnum.ERROR);
          }
          objWriter.writeValue(out, status);
        }

        out.flush();

        return new ImportStatusIterable(out.getDeleteOnCloseInputStream());
      } catch (IOException e) {
        log.error("Could not open temporary CSV file", e);
        throw e;
      }
    } catch (IOException e) {
      // Error while opening an ArchiveReader for the archive
      log.error("Error importing archive", e);
      throw e;
    }
  }

  /** WARC 1.0 spec has URI enclosed in "< ... >".  Remove them if
   * present. */
  String realRecordUri(String recordUri) {
    if (recordUri == null) {
      return null;
    }
    if (recordUri.startsWith("<") && recordUri.endsWith(">")) {
      return recordUri.substring(1, recordUri.length() - 1);
    }
    return recordUri;
  }

  /**
   * Returns the artifact with the specified UUID
   *
   * @param artifactUuid
   * @return The {@code Artifact} with the UUID, or null if none
   * @throws IOException
   */
  public Artifact getArtifactFromUuid(String artifactUuid) throws IOException {
    return index.getArtifact(artifactUuid);
  }

  @Override
  public ArtifactData getArtifactData(Artifact artifact, IncludeContentEnum includeContent) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    return getArtifactData(artifact.getNamespace(), artifact.getUuid());
  }

  /**
   * Retrieves an artifact from this LOCKSS repository.
   *
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to retrieve from this repository.
   * @return The {@code ArtifactData} referenced by this artifact ID.
   * @throws IOException
   */
  @Deprecated
  public ArtifactData getArtifactData(String namespace, String artifactUuid) throws IOException {
    validateNamespace(namespace);

    if (StringUtils.isEmpty(artifactUuid)) {
      throw new IllegalArgumentException("Null artifact ID");
    }

    Artifact artifactRef = index.getArtifact(artifactUuid);

    if (artifactRef == null) {
      throw new LockssNoSuchArtifactIdException("Non-existent artifact [uuid: " + artifactUuid + "]");
    }

    // Fetch and return artifact from data store
    return store.getArtifactData(artifactRef);
  }

  /**
   * Commits an artifact to this LOCKSS repository for permanent storage and inclusion in LOCKSS repository queries.
   *
   * @param namespace A {code String} containing the namespace.
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to commit to the repository.
   * @return An {@code Artifact} containing the updated artifact state information.
   * @throws IOException
   */
  @Override
  public Artifact commitArtifact(String namespace, String artifactUuid) throws IOException {
    validateNamespace(namespace);

    if (StringUtils.isEmpty(artifactUuid)) {
      throw new IllegalArgumentException("Null artifact UUID");
    }

    // Get artifact as it is currently
    Artifact artifact = index.getArtifact(artifactUuid);

    if (artifact == null) {
      throw new LockssNoSuchArtifactIdException("Non-existent artifact [uuid: " + artifactUuid + "]");
    }

    if (!artifact.getCommitted()) {
      injectTestingAction(artifact.getIdentifier(), TestingErrorOp.CommitArtifact);
      // Commit artifact in data store and index
      store.commitArtifactData(artifact);
      index.commitArtifact(artifactUuid);
      artifact.setCommitted(true);
    }

    return artifact;
  }

  /**
   * Permanently removes an artifact from this LOCKSS repository.
   *
   * @param artifactUuid A {@code String} with the artifact ID of the artifact to remove from this LOCKSS repository.
   * @throws IOException
   */
  @Override
  public void deleteArtifact(String namespace, String artifactUuid) throws IOException {
    validateNamespace(namespace);

    if (StringUtils.isEmpty(artifactUuid)) {
      throw new IllegalArgumentException("Null artifact UUID");
    }

    Artifact artifact = index.getArtifact(artifactUuid);

    if (artifact == null) {
      throw new LockssNoSuchArtifactIdException("Non-existent artifact [uuid: " + artifactUuid + "]");
    }

    // Remove from index and data store
    store.deleteArtifactData(artifact);
  }

  /**
   * Checks whether an artifact is committed to this LOCKSS repository.
   *
   * @param artifactUuid A {@code String} containing the artifact ID to check.
   * @return A boolean indicating whether the artifact is committed.
   */
//  @Override
//  public Boolean isArtifactCommitted(String namespace, String artifactUuid) throws IOException {
//    validateNamespace(namespace);
//
//    if (StringUtils.isEmpty(artifactUuid)) {
//      throw new IllegalArgumentException("Null artifact UUID");
//    }
//
//    Artifact artifact = index.getArtifact(artifactUuid);
//
//    if (artifact == null) {
//      throw new LockssNoSuchArtifactIdException("Non-existent artifact [uuid: " + artifactUuid + "]");
//    }
//
//    return artifact.getCommitted();
//  }

  /**
   * Provides the namespace of the committed artifacts in the index.
   *
   * @return An {@code Iterator<String>} with namespaces in this repository.
   */
  @Override
  public Iterable<String> getNamespaces() throws IOException {
    return index.getNamespaces();
  }

  /**
   * Returns a list of Archival Unit IDs (AUIDs) in a namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @return A {@code Iterator<String>} iterating over the AUIDs in a namespace.
   * @throws IOException
   */
  @Override
  public Iterable<String> getAuIds(String namespace) throws IOException {
    validateNamespace(namespace);
    return index.getAuIds(namespace);
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
   * @throws IOException
   */
  @Override
  public Iterable<Artifact> getArtifacts(String namespace, String auid) throws IOException {
    validateNamespace(namespace);

    if (auid == null) {
      throw new IllegalArgumentException("Null AUID");
    }

    return index.getArtifacts(namespace, auid);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs, from a specified Archival Unit and namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid) throws IOException {
    validateNamespace(namespace);

    if (auid == null) {
      throw new IllegalArgumentException("Null AUID");
    }

    return index.getArtifactsAllVersions(namespace, auid);
  }

  /**
   * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
   * Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param prefix     A {@code String} containing a URL prefix.
   * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
   * @throws IOException
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String namespace, String auid, String prefix) throws IOException {
    validateNamespace(namespace);

    if (auid == null || prefix == null) {
      throw new IllegalArgumentException("Null AUID or URL prefix");
    }

    return index.getArtifactsWithPrefix(namespace, auid, prefix);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a specified Archival Unit and
   * namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @param prefix     A String with the URL prefix.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix from an AU.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace, String auid, String prefix) throws IOException {
    validateNamespace(namespace);
    if (auid == null || prefix == null) {
      throw new IllegalArgumentException("Null AUID or URL prefix");
    }

    return index.getArtifactsWithPrefixAllVersions(namespace, auid, prefix);
  }

  /**
   * Returns the committed artifacts of all versions of all URLs matching a prefix, from a namespace.
   *
   * @param namespace A String with the namespace.
   * @param prefix     A String with the URL prefix.
   * @param versions   A {@link VersionsEnum} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String prefix,
                                                                VersionsEnum versions) throws IOException {

    validateNamespace(namespace);

    if (prefix == null) {
      throw new IllegalArgumentException("Null URL prefix");
    }

    return index.getArtifactsWithUrlPrefixFromAllAus(namespace, prefix, versions);
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} with the namespace.
   * @param auid       A {@code String} with the Archival Unit identifier.
   * @param url        A {@code String} with the URL to be matched.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
   * Archival Unit.
   */
  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, String url) throws IOException {
    validateNamespace(namespace);

    if (auid == null || url == null) {
      throw new IllegalArgumentException("Null AUID or URL");
    }

    return index.getArtifactsAllVersions(namespace, auid, url);
  }

  /**
   * Returns the committed artifacts of all versions of a given URL, from a specified namespace.
   *
   * @param namespace A {@code String} with the namespace.
   * @param url        A {@code String} with the URL to be matched.
   * @param versions   A {@link VersionsEnum} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace, String url, VersionsEnum versions)
      throws IOException {

    validateNamespace(namespace);

    if (url == null) {
      throw new IllegalArgumentException("Null URL");
    }

    return index.getArtifactsWithUrlFromAllAus(namespace, url, versions);
  }

  /**
   * Returns the artifact of the latest version of given URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @param url        A {@code String} containing a URL.
   * @return The {@code Artifact} representing the latest version of the URL in the AU.
   * @throws IOException
   */
  @Override
  public Artifact getArtifact(String namespace, String auid, String url) throws IOException {
    validateNamespace(namespace);

    if (auid == null || url == null) {
      throw new IllegalArgumentException("Null AUID or URL");
    }
    // Versionless error injection rule might trigger before add
    boolean errorActionApplied =
      injectTestingAction(new ArtifactIdentifier(namespace, auid, url, 0),
                          TestingErrorOp.GetArtifact);
    Artifact res = index.getArtifact(namespace, auid, url);
    // if no error action triggered, need to check version-full pattern.
    if (!errorActionApplied && res != null) {
      injectTestingAction(new ArtifactIdentifier(namespace, auid, url,
                                                 res.getVersion()),
                          TestingErrorOp.GetArtifact);
    }
    return res;
  }

  /**
   * Returns the artifact of a given version of a URL, from a specified Archival Unit and namespace.
   *
   * @param namespace A String with the namespace.
   * @param auid       A String with the Archival Unit identifier.
   * @param url        A String with the URL to be matched.
   * @param version    A String with the version.
   * @param includeUncommitted
   *          A boolean with the indication of whether an uncommitted artifact
   *          may be returned.
   * @return The {@code Artifact} of a given version of a URL, from a specified AU and namespace.
   */
  @Override
  public Artifact getArtifactVersion(String namespace, String auid, String url, Integer version, boolean includeUncommitted) throws IOException {
    validateNamespace(namespace);

    if (auid == null || url == null || version == null) {
      throw new IllegalArgumentException("Null AUID, URL or version");
    }
    injectTestingAction(new ArtifactIdentifier(namespace, auid, url, version),
                        TestingErrorOp.GetArtifact);

    return index.getArtifactVersion(namespace, auid, url, version,
        includeUncommitted);
  }

  /**
   * Returns the size, in bytes, of AU in a namespace.
   *
   * @param namespace A {@code String} containing the namespace.
   * @param auid       A {@code String} containing the Archival Unit ID.
   * @return A {@link AuSize} with byte size statistics of the specified AU.
   */
  @Override
  public AuSize auSize(String namespace, String auid) throws IOException {
    validateNamespace(namespace);

    if (auid == null) {
      throw new IllegalArgumentException("Null AUID");
    }

    // Get AU size from index query
    AuSize auSize = index.auSize(namespace, auid);
    return auSize;
  }

  public void setArtifactIndex(ArtifactIndex index) {
    this.index = index;
    index.setLockssRepository(this);
  }

  public ArtifactIndex getArtifactIndex() {
    return index;
  }

  public void setArtifactDataStore(ArtifactDataStore store) {
    this.store = store;
    store.setLockssRepository(this);
  }

  public ArtifactDataStore getArtifactDataStore() {
    return store;
  }

  public synchronized void incTimeSpentReiterating(long msAmount) {
    timeSpentReiterating += msAmount;
  }

  // Testing harness
  private List<ErrorInjectionRule> errorRules;

  /** Set or clear error injection rules */
  public void setErrorInjectionRules(List<ErrorInjectionRule> rules) {
    errorRules = rules;
  }

  /** Set error injection rules from text specification.  See  */
  public void setErrorInjectionRulesFromSpecs(String specs) {
    List<ErrorInjectionRule> oldRules = errorRules;
    try {
      errorRules = ErrorHarness.fromSpecs(specs);
      if (oldRules != null && !oldRules.isEmpty() && errorRules.isEmpty()) {
        log.debug("Error injection rules cleared");
      } else if (!errorRules.isEmpty()) {
        log.debug("Error injection rules: {}", errorRules);
      }
    } catch (IllegalArgumentException e) {
      log.error("Error parsing error injection rules: {}, exising rules (if any) unchanged.", specs);
      throw e;
    }
  }

  /** Trigger any applicable error action.
   * @return true if an action was triggered (in case it's a
   * non-throwing action & the colling code needs to know
   */
  private boolean injectTestingAction(ArtifactIdentifier artifactId,
                                      TestingErrorOp op) throws IOException {
    if (errorRules == null) return false;
    for (ErrorInjectionRule eir : errorRules) {
      if (eir.apply(artifactId, op)) {
        return true;
      }
    }
    return false;
  }

}
