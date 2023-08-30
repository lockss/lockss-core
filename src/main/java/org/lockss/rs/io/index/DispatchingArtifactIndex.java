/*
 * Copyright (c) 2017-2022, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.rs.io.index;

import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.util.concurrent.CopyOnWriteMap;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.storage.StorageInfo;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.UUID;

/**
 * ArtifactIndex that dispatches all operations to either a permanent
 * SolrArtifactIndex, or a transient VolatileArtifactIndex.  Allows
 * substantially faster bulk artifact storage by firat storing the
 * Artifacts in a VolatileArtifactIndex, then transferring them into
 * the SolrArtifactIndex in a batch.
 */
public class DispatchingArtifactIndex implements ArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  private ArtifactIndex masterIndex;
  private Map<String,ArtifactIndex> tempIndexMap = new CopyOnWriteMap<>();
  private BaseLockssRepository repository;

  public DispatchingArtifactIndex(ArtifactIndex master) {
    this.masterIndex = master;
  }

  /** Return true if this {namespace,auid} is currently in the temp index */
  private ArtifactIndex findIndexHolding(String namespace, String auid) {
    ArtifactIndex res = tempIndexMap.get(key(namespace, auid));
    return res != null ? res : masterIndex;
  }

  /** Return true if the artifact's {namespace,auid} is declared to
   * be in the temp index */
  private ArtifactIndex findIndexHolding(ArtifactIdentifier artifactId) {
    return findIndexHolding(artifactId.getNamespace(), artifactId.getAuid());
  }

  /** Return true if the stem's {namespace,auid} is declared to be in
   * the temp index */
  private ArtifactIndex findIndexHolding(ArtifactIdentifier.ArtifactStem stem) {
    return findIndexHolding(stem.getNamespace(), stem.getAuid());
  }

  /** Return true if the ArtifactData's {namespace,auid} is declared
   * to be in the temp index */
  private ArtifactIndex findIndexHolding(ArtifactData ad) {
    ArtifactIdentifier id = ad.getIdentifier();
    return findIndexHolding(id.getNamespace(), id.getAuid());
  }

  /** Return the temp index in which the artifact UUID is found, or the
   * masterIndex */
  private ArtifactIndex findIndexHolding(String artifactUuid) throws IOException{
    for (ArtifactIndex ind : tempIndexMap.values()) {
      if (ind.getArtifact(artifactUuid) != null) {
        return ind;
      }
    }
    return masterIndex;
  }

  /** Return true if the artifact's UUID is found in the temp index.  (This
   * is a heuristic - checks whether the artifact UUID is known to the
   * temp index*/
  private ArtifactIndex findIndexHolding(UUID artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid.toString());
  }

  @Override
  public void init() {
    masterIndex.init();
  }

  @Override
  public void setLockssRepository(BaseLockssRepository repository) {
    this.repository = repository;
    masterIndex.setLockssRepository(repository);
    for (ArtifactIndex ind : tempIndexMap.values()) {
      ind.setLockssRepository(repository);
    }
  }

  @Override
  public void start() {
    masterIndex.start();
  }

  @Override
  public void stop() {
    masterIndex.stop();
  }

  @Override
  public boolean isReady() {
    return masterIndex.isReady();
  }

  @Override
  public void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem)
      throws IOException {
    findIndexHolding(stem).acquireVersionLock(stem);
  }

  @Override
  public void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem) {
    findIndexHolding(stem).releaseVersionLock(stem);
  }

  @Override
  public void indexArtifact(Artifact artifact) throws IOException {
    findIndexHolding(artifact.getIdentifier()).indexArtifact(artifact);
  }

  @Override
  public void indexArtifacts(Iterable<Artifact> artifacts) throws IOException {
    // FIXME: This is safe for reindex but once the Repository has started,
    //  it is not going to direct index operations to the correct index.
    masterIndex.indexArtifacts(artifacts);
  }

  @Override
  public Artifact getArtifact(String artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).getArtifact(artifactUuid);
  }

  @Override
  public Artifact getArtifact(UUID artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).getArtifact(artifactUuid);
  }

  @Override
  public Artifact commitArtifact(String artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).commitArtifact(artifactUuid);
  }

  @Override
  public Artifact commitArtifact(UUID artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).commitArtifact(artifactUuid);
  }

  @Override
  public boolean deleteArtifact(String artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).deleteArtifact(artifactUuid);
  }

  @Override
  public boolean deleteArtifact(UUID artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).deleteArtifact(artifactUuid);
  }

  @Override
  public boolean artifactExists(String artifactUuid) throws IOException {
    return findIndexHolding(artifactUuid).artifactExists(artifactUuid);
  }

  @Override
  public Artifact updateStorageUrl(String artifactUuid, String storageUrl)
      throws IOException {
    return findIndexHolding(artifactUuid).updateStorageUrl(artifactUuid, storageUrl);
  }

  @Override
  public Iterable<String> getNamespaces() throws IOException {
    return masterIndex.getNamespaces();
  }

  @Override
  public Iterable<String> getAuIds(String namespace) throws IOException {
    return masterIndex.getAuIds(namespace);
  }

  @Override
  public Iterable<Artifact> getArtifacts(String namespace, String auid,
                                         boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifacts(namespace, auid, includeUncommitted);
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                                    String auid,
                                                    boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifactsAllVersions(namespace, auid,
                                                                      includeUncommitted);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefix(String namespace,
                                                   String auid, String prefix)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifactsWithPrefix(namespace, auid, prefix);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace,
                                                              String auid,
                                                              String prefix)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifactsWithPrefixAllVersions(namespace, auid,
                                                                                prefix);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace,
                                                                String prefix,
                                                                ArtifactVersions versions)
      throws IOException {
    return masterIndex.getArtifactsWithUrlPrefixFromAllAus(namespace,
                                                           prefix,
                                                           versions);
  }

  @Override
  public Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                                    String auid,
                                                    String url)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifactsAllVersions(namespace, auid, url);
  }

  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace,
                                                          String url,
                                                          ArtifactVersions versions)
      throws IOException {
    return masterIndex.getArtifactsWithUrlFromAllAus(namespace, url, versions);
  }

  @Override
  public Artifact getArtifact(String namespace, String auid,
                              String url, boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifact(namespace, auid, url, includeUncommitted);
  }

  @Override
  public Artifact getArtifactVersion(String namespace,
                                     String auid,
                                     String url,
                                     Integer version,
                                     boolean includeUncommitted)
      throws IOException {
    return findIndexHolding(namespace, auid).getArtifactVersion(namespace, auid, url,
                                                                 version, includeUncommitted);
  }

  @Override
  public AuSize auSize(String namespace, String auid) throws IOException {
    return findIndexHolding(namespace, auid).auSize(namespace, auid);
  }

  /**
   * Returns information about the storage size and free space
   * @return A {@code StorageInfo}
   */
  @Override
  public StorageInfo getStorageInfo() {
    return masterIndex.getStorageInfo();
  }

  @Override
  public void startBulkStore(String namespace, String auid) {
    VolatileArtifactIndex volInd = new VolatileArtifactIndex();
    volInd.init();
    volInd.start();
    volInd.setLockssRepository(repository);
    tempIndexMap.put(key(namespace, auid), volInd);
  }

  @Override
  public void finishBulkStore(String namespace, String auid,
                              int copyBatchSize) throws IOException {
    // Wait for all background commits for this AU to finish.  (They
    // call updateStorageUrl(), but index reads or writes are not
    // permitted and likely won't work correctly while the Artifacts
    // are being copied into Solr
    ArtifactDataStore store = repository.getArtifactDataStore();
    if (store instanceof WarcArtifactDataStore) {
      WarcArtifactDataStore warcStore = (WarcArtifactDataStore)store;
      if (!warcStore.waitForCommitTasks(namespace, auid)) {
        log.warn("waitForCommitTasks() was interrupted");
        throw new InterruptedIOException("finishBulk interrupted");
      }
    }
    // copy Artifacts to master index
    ArtifactIndex volInd;
    volInd = tempIndexMap.remove(key(namespace, auid));
    if (volInd == null) {
      throw new IllegalStateException("Attempt to finishBulkStore of AU not in bulk store mode: " + namespace + ", " + auid);
    }
    volInd.stop();
    try {
      Iterable<Artifact> artifacts = volInd.getArtifactsAllVersions(namespace, auid, true);
      masterIndex.indexArtifacts(artifacts);
    } catch (IOException e) {
      log.error("Failed to retrieve and bulk add artifacts", e);
      throw e;
    }
  }

  private String key(String namespace, String auid) {
    return namespace + auid;
  }

}
