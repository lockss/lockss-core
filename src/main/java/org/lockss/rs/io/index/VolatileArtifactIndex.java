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

package org.lockss.rs.io.index;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.rest.repo.util.ArtifactComparators;
import org.lockss.util.rest.repo.util.SemaphoreMap;
import org.lockss.util.storage.StorageInfo;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ArtifactData index implemented in memory, not persisted.
 */
public class VolatileArtifactIndex extends AbstractArtifactIndex {
    private final static L4JLogger log = L4JLogger.getLogger();

    /** Label to describe type of VolatileArtifactIndex */
    public static String ARTIFACT_INDEX_TYPE = "In-memory";

    // Internal map from artifact ID to Artifact
    protected Map<String, Artifact> index = new ConcurrentHashMap<>();

    /**
     * Map from artifact stem to semaphore. Used for artifact version locking.
     */
    private SemaphoreMap<ArtifactIdentifier.ArtifactStem> versionLock = new SemaphoreMap<>();

    @Override
    public void init() {
      log.debug("Initializing volatile artifact index");
      setState(ArtifactIndexState.INITIALIZED);
    }

  @Override
  public void start() {
    log.debug("Starting volatile artifact index");
    setState(ArtifactIndexState.RUNNING);
  }

  @Override
    public void stop() {
      setState(ArtifactIndexState.STOPPED);
    }

    /**
     * Returns information about the storage size and free space
     * @return A {@code StorageInfo}
     */
    @Override
    public StorageInfo getStorageInfo() {
      return StorageInfo.fromRuntime().setType(ARTIFACT_INDEX_TYPE);
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

  /**
     * Adds an artifact to the index.
     * 
     * @param artifact An {@link Artifact} to be added to the index.
     */
    @Override
    public void indexArtifact(Artifact artifact) {
      log.debug2("Adding artifact to index: {}", artifact);

        if (artifact == null) {
          throw new IllegalArgumentException("Null artifact");
        }

        ArtifactIdentifier artifactId = artifact.getIdentifier();

        if (artifactId == null) {
          throw new IllegalArgumentException("Artifact has null identifier");
        }

        String artifactUuid = artifactId.getUuid();

        if (StringUtils.isEmpty(artifactUuid)) {
          throw new IllegalArgumentException(
              "ArtifactIdentifier has null or empty artifact UUID");
        }

        // Add Artifact to the index
        addToIndex(artifactUuid, artifact);

        log.debug2("Added artifact to index: {}", artifact);
    }

  @Override
  public void indexArtifacts(Iterable<Artifact> artifacts) {
      artifacts.forEach(this::indexArtifact);
  }

    /**
     * Provides the index data of an artifact with a given text index
     * identifier.
     * 
     * @param artifactUuid
     *          A String with the artifact index identifier.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact getArtifact(String artifactUuid) {
      if (StringUtils.isEmpty(artifactUuid)) {
        throw new IllegalArgumentException("Null or empty artifact UUID");
      }

      return index.get(artifactUuid);
    }

    /**
     * Provides the index data of an artifact with a given index identifier
     * UUID.
     * 
     * @param artifactUuid
     *          An UUID with the artifact index identifier.
     * @return an Artifact with the artifact indexing data.
     */
    @Override
    public Artifact getArtifact(UUID artifactUuid) {
      if (artifactUuid == null) {
        throw new IllegalArgumentException("Null UUID");
      }

      return getArtifact(artifactUuid.toString());
    }

    /**
     * Commits to the index an artifact with a given text index identifier.
     * 
     * @param artifactUuid
     *          A String with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    @Override
    public Artifact commitArtifact(String artifactUuid) {
      if (StringUtils.isEmpty(artifactUuid)) {
        throw new IllegalArgumentException("Null or empty artifact UUID");
      }

      Artifact artifact = index.get(artifactUuid);

      if (artifact != null) {
        artifact.setCommitted(true);
      }

      return artifact;
    }

    /**
     * Commits to the index an artifact with a given index identifier UUID.
     * 
     * @param artifactUuid
     *          An UUID with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    @Override
    public Artifact commitArtifact(UUID artifactUuid) {
      if (artifactUuid == null) {
        throw new IllegalArgumentException("Null UUID");
      }

      return commitArtifact(artifactUuid.toString());
    }

    /**
     * Removes from the index an artifact with a given text index identifier.
     * 
     * @param artifactUuid
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean deleteArtifact(String artifactUuid) {
      if (StringUtils.isEmpty(artifactUuid)) {
        throw new IllegalArgumentException("Null or empty UUID");
      }

      return removeFromIndex(artifactUuid) != null;
    }

    /**
     * Removes from the index an artifact with a given index identifier UUID.
     * 
     * @param artifactUuid
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean deleteArtifact(UUID artifactUuid) {
      if (artifactUuid == null) {
        throw new IllegalArgumentException("Null UUID");
      }

      return deleteArtifact(artifactUuid.toString());
    }

    /**
     * Provides an indication of whether an artifact with a given text index
     * identifier exists in the index.
     * 
     * @param artifactUuid
     *          A String with the artifact identifier.
     * @return <code>true</code> if the artifact exists in the index,
     * <code>false</code> otherwise.
     */
    @Override
    public boolean artifactExists(String artifactUuid) {
      if (StringUtils.isEmpty(artifactUuid)) {
        throw new IllegalArgumentException("Null or empty artifact UUID");
      }

      return index.containsKey(artifactUuid);
    }
    
    @Override
    public Artifact updateStorageUrl(String artifactUuid, String storageUrl) throws IOException {
      if (StringUtils.isEmpty(artifactUuid)) {
        throw new IllegalArgumentException("Invalid artifact UUID");
      }

      if (StringUtils.isEmpty(storageUrl)) {
        throw new IllegalArgumentException("Cannot update storage URL: Null or empty storage URL");
      }

      // Retrieve the Artifact from the internal artifacts map
      Artifact artifact = index.get(artifactUuid);

      // Return null if the artifact could not be found
      if (artifact == null) {
        log.warn("Could not update storage URL: Artifact not found [uuid: " + artifactUuid + "]");
        throw new IOException("Artifact not found in volatile index");
      }

      // Update the storage URL of this Artifact in the internal artifacts map
      artifact.setStorageUrl(storageUrl);

      // Return the artifact
      return artifact;
    }

    /**
     * Provides the namespaces of the committed artifacts in the index.
     *
     * @return An {@code Iterator<String>} with the index committed artifacts namespaces.
     */
    @Override
    public Iterable<String> getNamespaces() {
      List<String> res = index.values().stream()
        .map(x -> x.getNamespace())
        .distinct()
        .sorted()
        .collect(Collectors.toList());
      return res;
    }

    /**
     * Returns a list of Archival Unit IDs (AUIDs) in a namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @return A {@code Iterator<String>} iterating over the AUIDs in this namespace.
     * @throws IOException
     */
    @Override
    public Iterable<String> getAuIds(String namespace) throws IOException {
      VolatileArtifactPredicateBuilder query = new VolatileArtifactPredicateBuilder();
      query.filterByNamespace(namespace);

      List<String> res = index.values().stream()
        .filter(query.build())
        .map(x -> x.getAuid()).distinct()
        .sorted()
        .collect(Collectors.toList());
      return res;
    }

    /**
     * Returns the committed artifacts of the latest version of all URLs, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return An {@code Iterator<Artifact>} containing the latest version of all URLs in an AU.
     * @throws IOException
     */
    @Override
    public Iterable<Artifact> getArtifacts(String namespace, String auid, boolean includeUncommitted) {
        VolatileArtifactPredicateBuilder q = new VolatileArtifactPredicateBuilder();

        // Filter by committed status equal to true?
        if (!includeUncommitted)
            q.filterByCommitStatus(true);

        q.filterByNamespace(namespace);
        q.filterByAuid(auid);

        // Filter, then group the Artifacts by URI, and pick the Artifacts with max version from each group
        Map<String, Optional<Artifact>> result = index.values().stream()
          .filter(q.build())
          .collect(Collectors.groupingBy(Artifact::getUri,
                                         Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))));

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream()
                                        .filter(Optional::isPresent).map(x -> x.get())
                                        .sorted(ArtifactComparators.BY_URI).iterator());
    }

    /**
     * Returns the artifacts of all versions of all URLs, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return all the versions among both committed and uncommitted
     *          artifacts.
     * @return An {@code Iterator<Artifact>} containing the artifacts of all version of all URLs in an AU.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, boolean includeUncommitted) {
        VolatileArtifactPredicateBuilder query = new VolatileArtifactPredicateBuilder();

        if (!includeUncommitted) {
            query.filterByCommitStatus(true);
        }

        query.filterByNamespace(namespace);
        query.filterByAuid(auid);

        // Apply the filter, sort by artifact URL then descending version, and return an iterator over the Artifacts
        return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the artifacts of the latest committed version of all URLs matching a prefix, from a specified Archival
     * Unit and namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param prefix
     *          A {@code String} containing a URL prefix.
     * @return An {@code Iterator<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
     * @throws IOException
     */
    @Override
    public Iterable<Artifact> getArtifactsWithPrefix(String namespace, String auid, String prefix) throws IOException {
        VolatileArtifactPredicateBuilder q = new VolatileArtifactPredicateBuilder();
        q.filterByCommitStatus(true);
        q.filterByNamespace(namespace);
        q.filterByAuid(auid);
        q.filterByURIPrefix(prefix);

        // Apply the filter, group the Artifacts by URL, then pick the Artifact with highest version from each group
        Map<String, Optional<Artifact>> result = index.values().stream()
          .filter(q.build())
          .collect(Collectors.groupingBy(Artifact::getUri,
                                         Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))));

        // Return an iterator over the artifact from each group (one per URI), after sorting them by artifact URI then
        // descending version.
        return IteratorUtils.asIterable(result.values().stream()
                                        .filter(Optional::isPresent).map(x -> x.get())
                                        .sorted(ArtifactComparators.BY_URI).iterator());
    }

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified Archival Unit and
     * namespace.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix from an AU.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace, String auid, String prefix) {
        VolatileArtifactPredicateBuilder query = new VolatileArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByNamespace(namespace);
        query.filterByAuid(auid);
        query.filterByURIPrefix(prefix);

	// Apply filter then sort the resulting Artifacts by URL and descending version
	return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_URI_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified namespace.
     *
     * @param namespace
     *          A String with the namespace.
     * @param urlPrefix
     *          A String with the URL prefix.
     * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
     *                   versions of an artifact.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String urlPrefix,
                                                                  ArtifactVersions versions) {

      if (!(versions == ArtifactVersions.ALL ||
            versions == ArtifactVersions.LATEST)) {
        throw new IllegalArgumentException("Versions must be ALL or LATEST");
      }

      if (namespace == null) {
        throw new IllegalArgumentException("Namespace is null");
      }

      VolatileArtifactPredicateBuilder query = new VolatileArtifactPredicateBuilder();
      query.filterByCommitStatus(true);
      query.filterByNamespace(namespace);

      if (urlPrefix != null) {
        query.filterByURIPrefix(urlPrefix);
      }

      // Apply predicates filter to Artifact stream
      Stream<Artifact> allVersions = index.values().stream().filter(query.build());

      if (versions == ArtifactVersions.LATEST) {
        Stream<Artifact> latestVersions = allVersions
          // Group the Artifacts by URL then pick the Artifact with highest version from each group
          .collect(Collectors.groupingBy(artifact -> artifact.getIdentifier().getArtifactStem(),
                                         Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
          .values()
          .stream()
          .filter(Optional::isPresent)
          .map(Optional::get);

        return IteratorUtils.asIterable(
                                        latestVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
      }

      return IteratorUtils.asIterable(
                                      allVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A {@code String} with the namespace.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL from an
     *         Archival Unit.
     */
    @Override
    public Iterable<Artifact> getArtifactsAllVersions(String namespace, String auid, String url) {
        VolatileArtifactPredicateBuilder query = new VolatileArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByNamespace(namespace);
        query.filterByAuid(auid);
        query.filterByURIMatch(url);

        // Apply filter then sort the resulting Artifacts by URL and descending version
        return IteratorUtils.asIterable(getIterableArtifacts().stream().filter(query.build())
            .sorted(ArtifactComparators.BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the committed artifacts of all versions of a given URL, from a specified namespace.
     *
     * @param namespace
     *          A {@code String} with the namespace.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
     *                   versions of an artifact.
     * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
     */
    @Override
    public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace, String url, ArtifactVersions versions) {
      if (!(versions == ArtifactVersions.ALL ||
          versions == ArtifactVersions.LATEST)) {
        throw new IllegalArgumentException("Versions must be ALL or LATEST");
      }

      if (namespace == null || url == null) {
        throw new IllegalArgumentException("Namespace or URL is null");
      }

      VolatileArtifactPredicateBuilder query = new VolatileArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByNamespace(namespace);
        query.filterByURIMatch(url);

        // Apply predicates filter to Artifact stream
        Stream<Artifact> allVersions = index.values().stream().filter(query.build());

        if (versions == ArtifactVersions.LATEST) {
          Stream<Artifact> latestVersions = allVersions
            // Group the Artifacts by URL then pick the Artifact with highest version from each group
            .collect(Collectors.groupingBy(artifact -> artifact.getIdentifier().getArtifactStem(),
                                           Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get);

          return IteratorUtils.asIterable(latestVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
        }

        return IteratorUtils.asIterable(allVersions.sorted(ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION).iterator());
    }

    /**
     * Returns the artifact of the latest version of given URL, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param url
     *          A {@code String} containing a URL.
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return the latest version among both committed and uncommitted
     *          artifacts of a URL.
     * @return An {@code Artifact} representing the latest version of the URL in the AU.
     * @throws IOException
     */
    @Override
    public Artifact getArtifact(String namespace, String auid, String url, boolean includeUncommitted) {
        VolatileArtifactPredicateBuilder q = new VolatileArtifactPredicateBuilder();

        if (!includeUncommitted) {
            q.filterByCommitStatus(true);
        }

        q.filterByNamespace(namespace);
        q.filterByAuid(auid);
        q.filterByURIMatch(url);

        // Apply the filter then get the artifact with max version
        Optional<Artifact> result = index.values().stream().filter(q.build()).max(Comparator.comparingInt(Artifact::getVersion));

        // Return the artifact, or null if one was not found
        return result.orElse(null);
    }

    /**
     * Returns the artifact of a given version of a URL, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @param version
     *          A String with the version.
     * @param includeUncommitted
     *          A boolean with the indication of whether an uncommitted artifact
     *          may be returned.
     * @return The {@code Artifact} of a given version of a URL, from a specified AU and namespace.
     */
    @Override
    public Artifact getArtifactVersion(String namespace, String auid, String url, Integer version, boolean includeUncommitted) {
      VolatileArtifactPredicateBuilder q = new VolatileArtifactPredicateBuilder();

      // Only filter by commit status when no uncommitted artifact is to be returned.
      if (!includeUncommitted) {
	q.filterByCommitStatus(true);
      }

      q.filterByNamespace(namespace);
      q.filterByAuid(auid);
      q.filterByURIMatch(url);
      q.filterByVersion(version);

      List<Artifact> artifacts = index.values().stream().filter(q.build()).collect(Collectors.toList());

      switch (artifacts.size()) {
      case 0:
        return null;
      case 1:
        return artifacts.get(0);
      default:
        String errMsg = "Found more than one artifact having the same version!";
        log.error(errMsg);
        throw new IllegalStateException(errMsg);
      }
    }

    /**
     * Returns the size, in bytes, of AU in a namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return A {@link AuSize} with byte size statistics of the specified AU.
     */
    @Override
    public AuSize auSize(String namespace, String auid) throws IOException {
      AuSize auSize = new AuSize();

      auSize.setTotalAllVersions(0L);
      auSize.setTotalLatestVersions(0L);
      // auSize.setTotalWarcSize(null);

      VolatileArtifactPredicateBuilder q = new VolatileArtifactPredicateBuilder();
      q.filterByCommitStatus(true);
      q.filterByNamespace(namespace);
      q.filterByAuid(auid);

      boolean isAuEmpty = !index.values()
          .stream()
          .anyMatch(q.build());

      if (isAuEmpty) {
        auSize.setTotalWarcSize(0L);
        return auSize;
      }

      // Disk size calculation
      long totalWarcSize = repository.getArtifactDataStore()
          .auWarcSize(namespace, auid);
      auSize.setTotalWarcSize(totalWarcSize);

      auSize.setTotalAllVersions(
          index.values()
              .stream()
              .filter(q.build())
              .mapToLong(Artifact::getContentLength)
              .sum());

      Map<String, Optional<Artifact>> latestArtifactVersions =
          index.values()
              .stream()
              .filter(q.build())
              .collect(Collectors.groupingBy(Artifact::getUri, Collectors.maxBy(Comparator.comparingInt(Artifact::getVersion))));

      auSize.setTotalLatestVersions(
          latestArtifactVersions.values().stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .mapToLong(Artifact::getContentLength)
              .sum());

      return auSize;
    }
    /**
     * Adds an artifact to the index.
     *
     * @param id
     *          A String with the identifier of the article to be added.
     * @param artifact
     *          An Artifact with the artifact to be added.
     */
    protected void addToIndex(String id, Artifact artifact) {
      // Add Artifact to the index.
      index.put(id, artifact);
    }

    /**
     * Removes an artifact from the index.
     *
     * @param id
     *          A String with the identifier of the article to be removed.
     * @return an Artifact with the artifact that has been removed from the
     *         index or null if not found
     */
    protected Artifact removeFromIndex(String id) {
      // Remove Artifact from the index.
      return index.remove(id);
    }

    private Collection<Artifact> getIterableArtifacts() {
      return index.values();
    }

    @Override
    public String toString() {
      return "[VolatileArtifactIndex index=" + index + "]";
    }

    /**
     * Returns a boolean indicating whether this artifact index is ready.
     *
     * Always returns true in the violate implementation.
     */
    @Override
    public boolean isReady() {
        return getState() == ArtifactIndexState.RUNNING;
    }
}
