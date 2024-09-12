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
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataUtil;
import org.lockss.rs.io.storage.warc.WarcArtifactStateEntry;
import org.lockss.util.BuildInfo;
import org.lockss.util.ByteArray;
import org.lockss.util.StreamUtil;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.jms.JmsFactory;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
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

  protected ArtifactDataStore<ArtifactIdentifier, ArtifactData, WarcArtifactStateEntry> store;
  protected ArtifactIndex index;
  protected JmsFactory jmsFact;

  protected ScheduledExecutorService scheduledExecutor =
      Executors.newSingleThreadScheduledExecutor();

  private static BuildInfo BUILD_INFO = BuildInfo.getBuildInfoFor("lockss-core")
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

  /**
   * Triggers a re-index of all artifacts in the data store into the index if the
   * reindex state file is present.
   *
   * @throws IOException
   */
  public void reindexArtifactsIfNeeded() throws IOException {
    if (needsReindex()) {
      log.info("Reindexing artifacts");

      // Reindex artifacts in the data store to index
      long reindexStart = TimeBase.nowMs();
      store.reindexArtifacts(index);
      log.info("Finished reindex in {}",
               TimeUtil.timeIntervalToString(TimeBase.msSince(reindexStart)));
    }
  }

  public ScheduledExecutorService getScheduledExecutorService() {
    return scheduledExecutor;
  }

  @Override
  public void initRepository() throws IOException {
    try {
      log.info("Initializing repository");
      LockssDaemon.getLockssDaemon().waitUntilAppRunning();

      // Initialize and start the index
      index.init();
      index.start();

      // Initialize the data store
      store.init();

      // Re-index artifacts in the data store if needed
      reindexArtifactsIfNeeded();

      // Start the data store
      store.start();
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while waiting for LOCKSS daemon", e);
    }
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
    return new RepositoryInfo(sto, ind);
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
      // Retrieve latest version in this URL lineage
      Artifact latestVersion = index.getArtifact(
          artifactId.getNamespace(),
          artifactId.getAuid(),
          artifactId.getUri(),
          true
      );

      // Create a new artifact identifier for this artifact
      ArtifactIdentifier newId = new ArtifactIdentifier(
          // Assign a new artifact ID
          UUID.randomUUID().toString(), // FIXME: Artifact ID collision unlikely but possible
          artifactId.getNamespace(),
          artifactId.getAuid(),
          artifactId.getUri(),
          // Set the next version
          (latestVersion == null) ? 1 : latestVersion.getVersion() + 1
      );

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
  public ArtifactData getArtifactData(Artifact artifact, IncludeContent includeContent) throws IOException {
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

    // FIXME: We end up performing multiple index lookups here, which is slow.
    Artifact artifactRef = index.getArtifact(artifactUuid);

    if (artifactRef == null) {
      throw new LockssNoSuchArtifactIdException("Non-existent artifact [uuid: " + artifactUuid + "]");
    }

    // Fetch and return artifact from data store
    // Q: Should ArtifactData properties be populated from Artifact here instead
    //  of within the data store? That would make it more consistent with the
    //  RestLockssRepository implementation.
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
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of all URLs matching a
   * prefix.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlPrefixFromAllAus(String namespace, String prefix,
                                                                ArtifactVersions versions) throws IOException {

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
   * @param versions   A {@link ArtifactVersions} indicating whether to include all versions or only the latest
   *                   versions of an artifact.
   * @return An {@code Iterator<Artifact>} containing the committed artifacts of all versions of a given URL.
   */
  @Override
  public Iterable<Artifact> getArtifactsWithUrlFromAllAus(String namespace, String url, ArtifactVersions versions)
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

    return index.getArtifact(namespace, auid, url);
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

  /**
   * Returns a signal indicating whether a reindex should be started or resumed at
   * repository initialization. Defaults to {@code false}. Override this method to
   * customize its behavior.
   *
   * @return A {@code boolean} indicating whether a reindex is needed. Defaults to
   * {@code false}.
   */
  protected boolean needsReindex() {
    return false;
  }
}
