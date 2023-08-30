/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.rs.io.index;

import org.lockss.rs.io.LockssRepositorySubsystem;
import org.lockss.rs.io.StorageInfoSource;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.PreOrderComparator;
import org.lockss.util.lang.Ready;
import org.lockss.util.time.Deadline;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Interface of the artifact index.
 */
public interface ArtifactIndex extends LockssRepositorySubsystem, StorageInfoSource, Ready {

    /**
     * Acquires the artifact version lock for an artifact stem. See
     * {@link ArtifactIdentifier.ArtifactStem}.
     *
     * @param stem The {@link ArtifactIdentifier.ArtifactStem} to acquire the lock of.
     * @throws IOException
     */
    void acquireVersionLock(ArtifactIdentifier.ArtifactStem stem) throws IOException;

    /**
     * Releases the artifact version lock for an artifact stem. See
     * {@link ArtifactIdentifier.ArtifactStem}.
     *
     * @param stem
     */
    void releaseVersionLock(ArtifactIdentifier.ArtifactStem stem);

    /**
     * Adds an artifact to the index.
     * 
     * @param artifact The {@link Artifact} to add to this index.
     * @throws IOException
     */
    void indexArtifact(Artifact artifact) throws IOException;

    /**
     * Bulk addition of artifacts into this index.
     *
     * @param artifacts An {@link Iterable<Artifact>} containing artifacts to add to this index.
     * @throws IOException
     */
    void indexArtifacts(Iterable<Artifact> artifacts) throws IOException;

    /**
     * Provides the index data of an artifact with a given text index
     * identifier.
     * 
     * @param artifactUuid
     *          A {@code String} with the artifact index identifier.
     * @return an Artifact with the artifact indexing data.
     */
    Artifact getArtifact(String artifactUuid) throws IOException;

    default Artifact getArtifact(ArtifactIdentifier aid) throws IOException {
        return getArtifact(aid.getUuid());
    }

    /**
     * Provides the index data of an artifact with a given index identifier
     * UUID.
     * 
     * @param artifactUuid
     *          An {@code UUID} with the artifact index identifier.
     * @return an Artifact with the artifact indexing data.
     */
    Artifact getArtifact(UUID artifactUuid) throws IOException;

    /**
     * Commits to the index an artifact with a given text index identifier.
     * 
     * @param artifactUuid
     *          A {@code String} with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    Artifact commitArtifact(String artifactUuid) throws IOException;

    /**
     * Commits to the index an artifact with a given index identifier UUID.
     * 
     * @param artifactUuid
     *          An {@code UUID} with the artifact index identifier.
     * @return an Artifact with the committed artifact indexing data.
     */
    Artifact commitArtifact(UUID artifactUuid) throws IOException;

    /**
     * Removes from the index an artifact with a given text index identifier.
     * 
     * @param artifactUuid
     *          A {@code String} with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    boolean deleteArtifact(String artifactUuid) throws IOException;

    /**
     * Removes from the index an artifact with a given index identifier UUID.
     * 
     * @param artifactUuid
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    boolean deleteArtifact(UUID artifactUuid) throws IOException;

    /**
     * Provides an indication of whether an artifact with a given text index
     * identifier exists in the index.
     * 
     * @param artifactUuid
     *          A String with the artifact identifier.
     * @return <code>true</code> if the artifact exists in the index,
     * <code>false</code> otherwise.
     */
    boolean artifactExists(String artifactUuid) throws IOException;

    /**
     * Updates the storage URL for an existing artifact.
     *
     * @param artifactUuid
     *          A {@code String) with the artifact ID to update.
     * @param storageUrl
     *          A {@code String} containing the new storage URL for this artifact.
     * @return {@code Artifact} with the new storage URL.
     * @throws IOException
     */
    Artifact updateStorageUrl(String artifactUuid, String storageUrl) throws IOException;

    /**
     * Provides the namespaces of the committed artifacts in the index.
     *
     * @return An {@code Iterable<String>} with the index committed artifacts namespaces.
     */
    Iterable<String> getNamespaces() throws IOException;

    /**
     * Returns a list of Archival Unit IDs (AUIDs) in a namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @return A {@code Iterable<String>} iterating over the AUIDs in the namespace.
     * @throws IOException
     */
    Iterable<String> getAuIds(String namespace) throws IOException;

    /**
     * Returns the artifacts of the latest committed version of all URLs, from a specified Archival Unit and namespace.
     * Returns artifacts with URLs ordered according to {@link PreOrderComparator}.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return An {@code Iterable<Artifact>} containing the latest version of all URLs in an AU.
     * @throws IOException
     */
    default Iterable<Artifact> getArtifacts(String namespace, String auid) throws IOException {
        return getArtifacts(namespace, auid, false);
    }

    Iterable<Artifact> getArtifacts(String namespace, String auid, boolean includeUncommitted) throws IOException;

    /**
     * Returns the artifacts of all committed versions of all URLs, from a specified Archival Unit and namespace.
     * Returns artifacts with URLs ordered according to {@link PreOrderComparator},
     * and for each URL, with version numbers in decreasing order.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all version of all URLs in an AU.
     * @throws IOException
     */
    default Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                                       String auid)
        throws IOException {
        return getArtifactsAllVersions(namespace, auid, false);
    }

    /**
     * Returns the artifacts of all versions of all URLs, from a specified Archival Unit and namespace.
     * Returns artifacts with URLs ordered according to {@link PreOrderComparator},
     * and for each URL, with version numbers in decreasing order.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param includeUncommitted
     *          A {@code boolean} indicating whether to return all the versions among both committed and uncommitted
     *          artifacts.
     * @return An {@code Iterable<Artifact>} containing the artifacts of all version of all URLs in an AU.
     * @throws IOException
     */
    Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                               String auid,
                                               boolean includeUncommitted)
        throws IOException;

    /**
     * Returns the committed artifacts of the latest version of all URLs matching a prefix, from a specified Archival
     * Unit and namespace.
     * Returns artifacts with URLs ordered according to {@link PreOrderComparator}.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param prefix
     *          A {@code String} containing a URL prefix.
     * @return An {@code Iterable<Artifact>} containing the latest version of all URLs matching a prefix in an AU.
     * @throws IOException
     */
    Iterable<Artifact> getArtifactsWithPrefix(String namespace,
                                              String auid,
                                              String prefix)
        throws IOException;

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified Archival Unit and
     * namespace.
     * Returns artifacts with URLs ordered according to {@link PreOrderComparator},
     * and for each URL, with version numbers in decreasing order.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix from an AU.
     */
    Iterable<Artifact> getArtifactsWithPrefixAllVersions(String namespace,
                                                         String auid,
                                                         String prefix)
        throws IOException;

    /**
     * Returns the artifacts of all committed versions of all URLs matching a prefix, from a specified namespace.
     * Returns artifacts with URLs ordered according to {@link PreOrderComparator},
     * and for each URL, with version numbers in decreasing order.
     *
     * @param namespace
     *          A String with the namespace.
     * @param prefix
     *          A String with the URL prefix.
     * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
     *                   versions of an artifact.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of all URLs matching a
     *         prefix.
     */
    Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace,
                                                           String prefix,
                                                           ArtifactVersions versions)
        throws IOException;

    /**
     * Returns the artifacts of all committed versions of a given URL, from a specified Archival Unit and namespace.
     * Returns artifacts ordered with version numbers in decreasing order.
     *
     * @param namespace
     *          A {@code String} with the namespace.
     * @param auid
     *          A {@code String} with the Archival Unit identifier.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL from an
     *         Archival Unit.
     */
    Iterable<Artifact> getArtifactsAllVersions(String namespace,
                                               String auid,
                                               String url)
        throws IOException;

    /**
     * Returns the artifacts of all committed versions of a given URL, from a specified namespace.
     * Returns artifacts ordered with version numbers in decreasing order.
     *
     * @param namespace
     *          A {@code String} with the namespace.
     * @param url
     *          A {@code String} with the URL to be matched.
     * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
     *                   versions of an artifact.
     * @return An {@code Iterable<Artifact>} containing the committed artifacts of all versions of a given URL.
     */
    Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace,
                                                     String url,
                                                     ArtifactVersions versions)
        throws IOException;

    /**
     * Returns the artifact of the latest committed version of given URL, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @param url
     *          A {@code String} containing a URL.
     * @return
     * @throws IOException
     */
    default Artifact getArtifact(String namespace,
                         String auid,
                         String url) throws IOException {
        return getArtifact(namespace, auid, url, false);
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
     * @return The {@code Artifact} representing the latest version of the URL in the AU.
     * @throws IOException
     */
    Artifact getArtifact(String namespace,
                         String auid,
                         String url,
                         boolean includeUncommitted)
        throws IOException;

    /**
     * Returns the committed artifact of a given version of a URL, from a specified Archival Unit and namespace.
     *
     * @param namespace
     *          A String with the namespace.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @param version
     *          A String with the version.
     * @return The {@code Artifact} of a given version of a URL, from a specified AU and namespace.
     */
    default Artifact getArtifactVersion(String namespace,
                                        String auid,
                                        String url,
                                        Integer version)
        throws IOException {
      return getArtifactVersion(namespace, auid, url, version, false);
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
    Artifact getArtifactVersion(String namespace,
                                String auid,
                                String url,
                                Integer version,
                                boolean includeUncommitted)
        throws IOException;

    /**
     * Returns the size, in bytes, of AU in a namespace.
     *
     * @param namespace
     *          A {@code String} containing the namespace.
     * @param auid
     *          A {@code String} containing the Archival Unit ID.
     * @return A {@link AuSize} with byte size statistics of the specified AU.
     */
    AuSize auSize(String namespace, String auid) throws IOException;

    long DEFAULT_WAITREADY = 5000;

    @Override
    default void waitReady(Deadline deadline) throws TimeoutException {
        final L4JLogger log = L4JLogger.getLogger();

        while (!isReady()) {
            if (deadline.expired()) {
                throw new TimeoutException("Deadline for artifact index to become ready expired");
            }

            long remainingTime = deadline.getRemainingTime();
            long sleepTime = Math.min(deadline.getSleepTime(), DEFAULT_WAITREADY);

            log.debug(
                "Waiting for artifact index to become ready (retrying in {} ms; deadline in {} ms)",
                sleepTime,
                remainingTime
            );

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while waiting for artifact index to become ready");
            }
        }
    }

    /**
     * Sets an AUID into bulk store mode
     *
     * @param auid
     *
     * @throws IOException if not a DispatchingArtifactIndex
     */
    void startBulkStore(String namespace, String auid) throws IOException;

    /**
     * Finish a bulk store operation (by copying the index entries
     * to the permanent index).
     *
     * @param auid
     *
     * @throws IOException if not a DispatchingArtifactIndex
     */
    void finishBulkStore(String namespace, String auid, int copyBatchSize)
        throws IOException;
}
