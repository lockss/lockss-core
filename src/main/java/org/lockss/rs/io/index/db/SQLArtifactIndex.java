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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.lockss.app.LockssApp;
import org.lockss.db.DbException;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.AbstractArtifactIndex;
import org.lockss.rs.io.index.ArtifactIndexVersion;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.SemaphoreMap;
import org.lockss.util.storage.StorageInfo;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SQLArtifactIndex extends AbstractArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  public SQLArtifactIndex() {
    // Intentionally left blank
  }

  public SQLArtifactIndex(SQLArtifactIndexManagerSql idxdb) {
    this.idxdb = idxdb;
  }

  @Override
  public ArtifactIndexVersion getArtifactIndexTargetVersion() {
    return new ArtifactIndexVersion()
        .setIndexType(SQLArtifactIndex.class.getSimpleName())
        .setIndexVersion(1);
  }

  public static String ARTIFACT_INDEX_TYPE = "SQL";

  /**
   * How a reindex resolves two artifacts that claim the same
   * (namespace, AUID, URL, version) under different UUIDs.
   *
   * <p>The artifacts table has a unique index on {@code uuid} only, so nothing
   * in the schema prevents such a pair from existing; see
   * {@link SQLArtifactIndexManagerSql#resolveVersionConflict}.
   *
   * <p>Lives here, in repocore, rather than in the Spring configuration class
   * that reads the parameter, because repocore cannot see
   * {@code org.lockss.config}. Mirrors
   * {@code RepositoryManager.CheckUnnormalizedMode}.
   */
  public enum VersionConflictResolution {
    /** Keep the artifact with the earliest {@code crawl_time}. */
    PreferEarliest,
    /** Keep the artifact with the latest {@code crawl_time}. */
    PreferLatest
  }

  /**
   * Default version-conflict resolution: keep the earliest collection date,
   * which preserves the original crawl's provenance.
   */
  public static final VersionConflictResolution DEFAULT_VERSION_CONFLICT_RESOLUTION =
      VersionConflictResolution.PreferEarliest;

  /**
   * Volatile so that a configuration callback on another thread is seen by an
   * in-progress reindex; initialized to the default so the index behaves
   * correctly when no callback ever runs (e.g. in tests).
   */
  private volatile VersionConflictResolution versionConflictResolution =
      DEFAULT_VERSION_CONFLICT_RESOLUTION;

  private SQLArtifactIndexManagerSql idxdb = null;

  private final SemaphoreMap<ArtifactIdentifier.ArtifactStem> versionLock = new SemaphoreMap<>();

  private final Map<String, CompletableFuture<AuSize>> auSizeFutures =
      new ConcurrentHashMap<>();


  @Override
  public void init() {
    log.info("Initializing SQLArtifactIndex");
    idxdb = new SQLArtifactIndexManagerSql(
        LockssApp.getManagerByTypeStatic(SQLArtifactIndexDbManager.class));
  }

  @Override
  public boolean isReady() {
    return idxdb != null;
  }

  /**
   * Sets the policy used to resolve two artifacts claiming the same
   * (namespace, AUID, URL, version) during a reindex. Pushed in from the
   * service's configuration callback, which repocore cannot reach itself.
   *
   * @param res The {@link VersionConflictResolution} to use; null selects the
   *            default.
   * @return This index, for chaining.
   */
  public SQLArtifactIndex setVersionConflictResolution(VersionConflictResolution res) {
    versionConflictResolution =
        (res == null) ? DEFAULT_VERSION_CONFLICT_RESOLUTION : res;
    log.debug("Version conflict resolution set to {}", versionConflictResolution);
    return this;
  }

  /** @return The version-conflict resolution policy in effect. */
  public VersionConflictResolution getVersionConflictResolution() {
    return versionConflictResolution;
  }

  @Override
  public void start() {
    // Intentionally left blank
  }

  @Override
  public void stop() {
    if (idxdb != null) {
      // Stops the background statistics-refresh thread.
      idxdb.shutdown();
    }
  }

  @Override
  public StorageInfo getStorageInfo() {
    // FIXME: Use correct Derby / PostgreSQL data path
    // In most environments it will be the the same as our data dir
    return StorageInfo.fromDF(ARTIFACT_INDEX_TYPE, PlatformUtil.getInstance().getDF(repository.getRepositoryStateDirPath().toString()));
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException {
    // Acquire the lock for this artifact stem
    try {
      versionLock.getLock(stem);
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted while waiting to acquire artifact version lock");
    }
  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    // Release the lock for the artifact stem
    versionLock.releaseLock(stem);
  }

  @Override
  public void indexArtifact(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    try {
      idxdb.addArtifact(artifact);
    } catch (DbException e) {
      throw new IOException("Could not add artifact to database", e);
    }
  }

  @Override
  public void indexArtifacts(Iterable<Artifact> artifacts) throws IOException {
    try {
      Set<Pair<String,String>> nsAuids = idxdb.addArtifacts(artifacts);
      for (Pair<String,String> nsAuid : nsAuids) {
        try {
          invalidateAuSize(nsAuid.getLeft(), nsAuid.getRight());
        } catch (DbException e) {
          log.warn("Could not invalidate AU size for: {}",
                   nsAuid.getRight(), e);
        }
      }
    } catch (DbException e) {
      throw new IOException("Could not add artifacts to database", e);
    }
  }

  @Override
  public void reindexArtifact(Artifact artifact) throws IOException {
    if (artifact == null) {
      throw new IllegalArgumentException("Null artifact");
    }

    try {
      idxdb.upsertArtifactForReindex(artifact, versionConflictResolution);
    } catch (DbException e) {
      throw new IOException("Could not add/update artifact to database", e);
    }
  }

  @Override
  public int reindexArtifacts(Iterable<Artifact> artifacts) throws IOException {
    // TODO (E.3): replace this per-artifact loop with a batched
    //  idxdb.upsertArtifactsForReindex(artifacts, versionConflictResolution).
    //  As written, each iteration costs a connection checkout and a COMMIT --
    //  a WAL flush per artifact under synchronous_commit=on -- plus, since
    //  Component D, the version-conflict SELECT. The batched shape is spelled
    //  out in the javadoc of SQLArtifactIndexManagerSql.upsertArtifactsForReindex().

    Artifact firstArtifact = null;
    int attempted = 0;
    int failed = 0;

    for (Artifact artifact : artifacts) {
      if (firstArtifact == null) {
        firstArtifact = artifact;
      }

      attempted++;

      // Per-item isolation: each upsert is its own connection and its own
      // transaction, so a failure here has already been rolled back and leaves
      // nothing for the next artifact to trip over. Before this, the first bad
      // artifact -- a null version NPEs on setInt(), for instance -- abandoned
      // every artifact after it in the batch.
      try {
        idxdb.upsertArtifactForReindex(artifact, versionConflictResolution);
      } catch (DbException | RuntimeException e) {
        failed++;
        log.error("Could not reindex artifact, skipping it [uuid: {}, url: {}]",
                  artifact.getUuid(), artifact.getUri(), e);
      }
    }

    if (failed > 0) {
      // This log names the individual artifacts; the count returned below is
      // what the caller uses to report the batch as partially failed.
      log.error("Reindex skipped {} of {} artifacts; see the errors above",
                failed, attempted);
    }

    // FIXME (E.4): The assumption that all the artifacts are in the same namespace
    //  and AUID (as determined by the first artifact) is only true in "bulk-mode".
    //  A temporary WARC interleaves AUs, so every AU but the first keeps a stale
    //  cached size. The general fix is to collect the distinct (namespace, auid)
    //  set as we go and invalidate each; the per-AU reindex path
    //  (WarcArtifactDataStore.reindexArtifactsInAu()) is correct by construction.
    if (firstArtifact != null) {
      try {
        invalidateAuSize(firstArtifact.getNamespace(), firstArtifact.getAuid());
      } catch (DbException e) {
        // Warn and carry on, as indexArtifacts() does. A stale AU-size cache
        // entry is cosmetic and self-correcting; it must never fail a reindex
        // whose artifacts are already committed.
        log.warn("Could not invalidate AU size", e);
      }
    }

    return failed;
  }

  @Override
  public Artifact getArtifact(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    try {
      return idxdb.getArtifact(uuid);
    } catch (DbException e) {
      throw new IOException("Could not retrieve artifact from database", e);
    }
  }

  @Override
  // Q: Get rid of method from API?
  public Artifact getArtifact(UUID uuid) throws IOException {
    if (uuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return getArtifact(uuid.toString());
  }

  @Override
  public Artifact getArtifact(String namespace, String auid, String url, boolean includeUncommitted)
      throws IOException {
    try {
      return idxdb.getLatestArtifact(namespace, auid, url, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Could not query database for artifact", e);
    }
  }

  @Override
  public Artifact getArtifactVersion(String namespace, String auid, String url, Integer version, boolean includeUncommitted)
      throws IOException {

    try {
      // Q: Of what use is includeUncommitted here? The tuple (ns, auid, url, ver)
      //  uniquely identifies an artifact and we either have it or we don't
      return idxdb.getArtifact(namespace, auid, url, version, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Could not query database for artifact", e);
    }
  }

  @Override
  public Artifact commitArtifact(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    try {
      idxdb.commitArtifact(uuid);

      Artifact result = getArtifact(uuid);

      try {
        if (result != null) invalidateAuSize(result.getNamespace(), result.getAuid());
      } catch (DbException e) {
        throw new IOException("Could not invalidate AU size", e);
      }

      // Q: Remove Artifact return from method signature?
      return result;
    } catch (DbException e) {
      throw new IOException("Could not mark artifact as committed in database", e);
    }
  }

  @Override
  // Q: Get rid of method from API?
  public Artifact commitArtifact(UUID uuid) throws IOException {
    if (uuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return commitArtifact(uuid.toString());
  }

  @Override
  public void clearIndex() throws IOException {
    if (indexState == ArtifactIndexState.RUNNING) {
      throw new IllegalStateException("Cannot clear the artifact index while in running state");
    }
    try {
      idxdb.clearAllArtifacts();
    } catch (DbException e) {
      throw new IOException("Could not clear artifact index database", e);
    }
  }

  @Override
  public boolean deleteArtifact(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty UUID");
    }

    try {
      try {
        Artifact result = getArtifact(uuid);
        if (result != null) invalidateAuSize(result.getNamespace(), result.getAuid());
      } catch (DbException e) {
        throw new IOException("Could not invalidate AU size", e);
      }

      // Q: Remove boolean return from method signature?
      return idxdb.deleteArtifact(uuid) != 0;
    } catch (DbException e) {
      throw new IOException("Could not remove artifact from database", e);
    }
  }

  @Override
  // Q: Get rid of method from API?
  public boolean deleteArtifact(UUID uuid) throws IOException {
    if (uuid == null) {
      throw new IllegalArgumentException("Null UUID");
    }

    return deleteArtifact(uuid.toString());
  }

  @Override
  public boolean artifactExists(String uuid) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Null or empty artifact UUID");
    }

    return getArtifact(uuid) != null;
  }

  @Override
  public Artifact updateStorageUrl(String uuid, String storageUrl) throws IOException {
    if (StringUtils.isEmpty(uuid)) {
      throw new IllegalArgumentException("Invalid artifact UUID");
    }

    try {
      if (idxdb.updateStorageUrl(uuid, storageUrl) == 0) {
        throw new IOException("Artifact not found");
      }

      // Q: Remove Artifact return from method signature?
      return getArtifact(uuid);
    } catch (DbException e) {
      throw new IOException("Database error trying to update storage URL", e);
    }
  }

  @Override
  public Iterable<String> getNamespaces() throws IOException {
    try {
      return idxdb.getNamespaces();
    } catch (DbException e) {
      throw new IOException("Database error fetching namespaces", e);
    }
  }

  @Override
  public Iterable<String> getAuIds(String namespace) throws IOException {
    try {
      return idxdb.findAuids(namespace);
    } catch (DbException e) {
      throw new IOException("Database error fetching AUIDs", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifacts(String namespace, String auid, boolean includeUncommitted)
      throws IOException {
    try {
      return idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(namespace, auid, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, boolean includeUncommitted)
      throws IOException {
    try {
      return idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(namespace, auid, includeUncommitted);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, String url)
      throws IOException {
    try {
      return idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(namespace, auid, url);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace, String url, VersionsEnum versions)
      throws IOException {

    if (!(versions == VersionsEnum.ALL ||
        versions == VersionsEnum.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (namespace == null || url == null) {
      throw new IllegalArgumentException("Namespace or URL is null");
    }

    try {
      return idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(namespace, url, versions);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String namespace, String auid, String prefix)
      throws IOException {
    try {
      return idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(namespace, auid, prefix);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace, String auid, String prefix)
      throws IOException {
    try {
      return idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(namespace, auid, prefix);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String prefix, VersionsEnum versions)
      throws IOException {

    if (!(versions == VersionsEnum.ALL ||
        versions == VersionsEnum.LATEST)) {
      throw new IllegalArgumentException("Versions must be ALL or LATEST");
    }

    if (namespace == null) {
      throw new IllegalArgumentException("Namespace is null");
    }

    try {
      return idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(namespace, prefix, versions);
    } catch (DbException e) {
      throw new IOException("Database error fetching artifacts", e);
    }
  }

  private AuSize computeAuSize(String namespace, String auid) throws DbException, IOException {
    AuSize result = new AuSize();

    long totalWarcSize = repository.getArtifactDataStore()
        .auWarcSize(namespace, auid);

    result.setTotalWarcSize(totalWarcSize);
    result.setTotalAllVersions(idxdb.getSizeOfArtifacts(namespace, auid, VersionsEnum.ALL));
    result.setTotalLatestVersions(idxdb.getSizeOfArtifacts(namespace, auid, VersionsEnum.LATEST));

    return result;
  }

  public AuSize findAuSize(String namespace, String auid) throws DbException {
    return idxdb.findAuSize(NamespacedAuid.key(namespace, auid));
  }

  @Override
  public AuSize auSize(String namespace, String auid) throws IOException {
    try {
      AuSize result = findAuSize(namespace, auid);

      if (result == null) {
        Future<AuSize> ausFuture = getAuSizeFuture(namespace, auid);
        result = ausFuture.get();
      }

      return result;
    } catch (DbException e) {
      throw new IOException("Could not query AU size from database", e);
    } catch (ExecutionException e) {
      // Unconditionally casting the cause to IOException would throw
      // ClassCastException and discard the real error whenever the cause is
      // anything else -- which it now can be, since getAuSizeFuture() completes
      // the future with whatever it caught.
      Throwable cause = e.getCause();
      log.error("Could not recompute AU size", cause);
      if (cause instanceof IOException ioe) {
        throw ioe;
      }
      throw new IOException("Could not recompute AU size", cause);
    } catch (InterruptedException e) {
      log.error("Interrupted while waiting for AU size recalculation", e);
      throw new IOException("AU size recalculation was interrupted", e);
    }
  }

  private Future<AuSize> getAuSizeFuture(String namespace, String auid) {
    String nsAuid = NamespacedAuid.key(namespace, auid);
    CompletableFuture<AuSize> ausFuture;

    synchronized (auSizeFutures) {
      ausFuture = auSizeFutures.get(nsAuid);
      if (ausFuture != null) {
        return ausFuture;
      } else {
        ausFuture = new CompletableFuture<>();
        auSizeFutures.put(nsAuid, ausFuture);
      }
    }

    try {
      AuSize result = computeAuSize(namespace, auid);
      updateAuSize(namespace, auid, result);
      ausFuture.complete(result);
    } catch (DbException e) {
      log.error("Couldn't update AU size in database", e);
      ausFuture.completeExceptionally(new IOException("Couldn't update AU size in database", e));
    } catch (IOException e) {
      log.error("Couldn't compute AU size", e);
      ausFuture.completeExceptionally(e);
    } catch (Throwable t) {
      // Nothing else can ever complete this future: the finally below removes it
      // from the map, so a concurrent caller that already read it out and is
      // blocked in auSize() on get() would wait forever -- an unbounded,
      // uninterruptible hang rather than a failure. Catching Throwable rather
      // than RuntimeException so an Error can't leave a thread hung either.
      log.error("Couldn't compute AU size", t);
      ausFuture.completeExceptionally(t);
      throw t;
    } finally {
      synchronized (auSizeFutures) {
        auSizeFutures.remove(nsAuid);
      }
    }

    return ausFuture;
  }

  public Long updateAuSize(String namespace, String auid, AuSize auSize) throws DbException {
    String nsAuid = NamespacedAuid.key(namespace, auid);
    return idxdb.updateAuSize(nsAuid, auSize);
  }

  public void invalidateAuSize(String namespace, String auid) throws DbException {
    String nsAuid = NamespacedAuid.key(namespace, auid);
    log.debug2("Invalidating AU size [ns: {}, auid: {}]", namespace, auid);
    idxdb.deleteAuSize(nsAuid);
  }
}
