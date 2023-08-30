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

package org.lockss.rs.io.storage.warc;

import org.apache.commons.collections4.IterableUtils;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.ArtifactContainerStats;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.time.TimeBase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class WarcFilePool {
  private static final L4JLogger log = L4JLogger.getLogger();

  protected WarcArtifactDataStore store;
  protected Set<WarcFile> allWarcs = new HashSet<>();
  protected Set<WarcFile> fullWarcs = new HashSet<>();

  public WarcFilePool(WarcArtifactDataStore store) {
    this.store = store;
  }

  /**
   * Creates a new temporary WARC file under one of the temporary WARC directories configured
   * in the data store.
   */
  protected WarcFile createWarcFile() throws IOException {
    Path basePath = Arrays.stream(store.getBasePaths())
        .max((a, b) -> (int) (store.getFreeSpace(a) - store.getFreeSpace(b)))
        .orElse(null);

    Path tmpWarcDir = basePath.resolve(WarcArtifactDataStore.TMP_WARCS_DIR);

    WarcFile warcFile =
        new WarcFile(tmpWarcDir.resolve(generateTmpWarcFileName()), store.getUseWarcCompression());

    store.initWarc(warcFile.getPath());

    allWarcs.add(warcFile);

    return warcFile;
  }

  protected String generateTmpWarcFileName() {
    return UUID.randomUUID() + store.getWarcFileExtension();
  }

  /**
   * Checks out an existing WARC file from the pool or creates a new one.
   */
  public WarcFile checkoutWarcFileForWrite() throws IOException {
    synchronized (this) {
      Optional<WarcFile> optWarc = allWarcs.stream()
          .filter(warc -> warc.getStats().getArtifactsTotal() < store.getMaxArtifactsThreshold())
          // TODO: Implement separate thresholds for temp and permanent WARCs
          .filter(warc -> warc.getLength() < store.getThresholdWarcSize())
          .filter(warc -> warc.isCompressed() == store.getUseWarcCompression())
          .filter(warc -> !warc.isCheckedOut())
          .findAny();

      WarcFile warc = optWarc.isPresent() ?
          optWarc.get() : createWarcFile();

      warc.setCheckedOut(true);
      return warc;
    }
  }

  /**
   * Search for the WarcFile object in this pool that matches the given path. Returns {@code null} if one could not be
   * found.
   *
   * @param warcFilePath A {@link String} containing the path to the {@link WarcFile} to find.
   * @return The {@link WarcFile}, or {@code null} if one could not be found.
   */
  public WarcFile getWarcFile(Path warcFilePath) {
    synchronized (this) {
      return Stream.concat(allWarcs.stream(), fullWarcs.stream())
          .filter(x -> x.getPath().equals(warcFilePath))
          .findFirst()
          .orElse(null);
    }
  }

  /**
   * Makes an existing {@link WarcFile} available to this pool.
   *
   * @param warcFile The {@link WarcFile} to add back to this pool.
   */
  public void returnWarcFile(WarcFile warcFile) {
    // Q: Synchronize on the WarcFile? Should be unnecessary since this thread should have it exclusively
    boolean isSizeReached = warcFile.getLength() >= store.getThresholdWarcSize();
    boolean isArtifactsReached = warcFile.getStats().getArtifactsTotal() >= store.getMaxArtifactsThreshold();
    boolean readyForGC = isSizeReached || isArtifactsReached || warcFile.isReleased();

    synchronized (this) {
      warcFile.setCheckedOut(false);

      if (readyForGC) {
        fullWarcs.add(warcFile);
        allWarcs.remove(warcFile);
      }
    }
  }

  /**
   * Checks whether a {@link WarcFile} object is a member of this pool.
   *
   * @param warcFile The {@link WarcFile} to check.
   * @return A {@code boolean} indicating whether the {@link WarcFile} is a member of this pool.
   */
  public boolean isInPool(WarcFile warcFile) {
    synchronized (this) {
      return allWarcs.contains(warcFile) || fullWarcs.contains(warcFile);
    }
  }

  /**
   * Checks whether a {@link WarcFile} of a given path is a member of this pool.
   *
   * @param warcFilePath The {@link Path} to check.
   * @return A {@code boolean} indicating whether the {@link WarcFile} is a member of this pool.
   */
  public boolean isInPool(Path warcFilePath) {
    return isInPool(getWarcFile(warcFilePath));
  }

  /**
   * Removes an existing {@link WarcFile} from this pool.
   *
   * @param warcFile The instance of {@link WarcFile} to remove from this pool.
   */
  private void removeWarcFileFromPool(WarcFile warcFile) {
    synchronized (this) {
      fullWarcs.remove(warcFile);
      allWarcs.remove(warcFile);
    }
  }

  /**
   * Dumps a snapshot of all {@link WarcFile} objects in this pool. For debugging.
   */
  public void dumpWarcFilesPoolInfo() {
    long totalBlocksAllocated = 0;
    long totalBytesUsed = 0;
    long numWarcFiles = 0;

    // Iterate over WarcFiles in this pool
    synchronized (this) {
      for (WarcFile warcFile : allWarcs) {
        long blocks = (long) Math.ceil(new Float(warcFile.getLength()) / new Float(store.getBlockSize()));
        totalBlocksAllocated += blocks;
        totalBytesUsed += warcFile.getLength();

        // Log information per WarcFile
        log.debug2(
            "[path = {}, length = {}, blocks = {}, inUse = {}]",
            warcFile.getPath(),
            warcFile.getLength(),
            blocks,
            fullWarcs.contains(warcFile)
        );

        numWarcFiles++;
      }
    }

    // Log aggregate information about the pool of WarcFiles
    log.debug(String.format(
        "Summary: %d bytes allocated (%d blocks) using %d bytes (%.2f%%) in %d WARC files",
        totalBlocksAllocated * store.getBlockSize(),
        totalBlocksAllocated,
        totalBytesUsed,
        100.0f * (float) totalBytesUsed / (float) (totalBlocksAllocated * store.getBlockSize()),
        numWarcFiles
    ));
  }

  public void runGC() {
    // WARCs to GC
    List<WarcFile> removableWarcs = new ArrayList<>();

    // Determine which WARCs to GC; remove from pool while synchronized
    synchronized (this) {
      for (WarcFile warc : IterableUtils.chainedIterable(allWarcs, fullWarcs)) {
        if (TempWarcInUseTracker.INSTANCE.isInUse(warc.getPath()) || warc.isCheckedOut()) {
          continue;
        }

        synchronized (warc) {
          ArtifactContainerStats stats = warc.getStats();

          boolean pastExpiration = stats.getLatestExpiration() <= TimeBase.nowMs();

          int uncommitted = stats.getArtifactsUncommitted();
          int committed = stats.getArtifactsCommitted();
          int copied = stats.getArtifactsCopied();

          if (committed == copied && (uncommitted == 0 || pastExpiration)) {
            warc.setMarkedForGC();
            removableWarcs.add(warc);
          }
        }
      }

      // Remove WARCs from pool while synchronized on the pool - we do this to avoid concurrent
      // modification exceptions/errors
      for (WarcFile warc : removableWarcs) {
        removeWarcFileFromPool(warc);
      }
    }

    if (log.isDebug2Enabled()) {
      log.debug2("allWarcs.size() = {}, fullWarcs.size() = {}, removableWarcs.size = {}",
          allWarcs.size(), fullWarcs.size(), removableWarcs.size());

      log.debug2("allWarcs = {}", allWarcs);
      log.debug2("fullWarcs = {}", fullWarcs);
      log.debug2("removableWarcs = {}", removableWarcs);
    }

    // Remove WARCs marked for GC from data store and the index
    for (WarcFile warc : removableWarcs) {
      synchronized (warc) {
        ArtifactContainerStats stats = warc.getStats();

        try {
          // Remove index references if there are any expired artifacts
          if (stats.getArtifactsUncommitted() != 0) {
            ArtifactIndex index = store.getArtifactIndex();

            Map<ArtifactIdentifier, Artifact> indexedArtifacts =
                scanForIndexedArtifacts(warc.getPath(), index);

            for (Artifact artifact : indexedArtifacts.values()) {
              if (!artifact.isCommitted()) {
                index.deleteArtifact(artifact.getUuid());
              }
            }
          }

          // Remove WARC file from the data store
          store.removeWarc(warc.getPath());
        } catch (IOException e) {
          // Log error and leave to reload
          log.error("Could not remove WARC file " + warc.getPath(), e);
        }
      }
    }
  }

  /**
   * Builds a {@link Map<ArtifactIdentifier, Artifact>} where the keys are determined from the records contained
   * within a WARC file and mapped to their respective {@link Artifact} within the provided {@link ArtifactIndex}.
   * See {@link WarcArtifactDataUtil#buildArtifactIdentifier(ArchiveRecordHeader)} for how the
   * {@link ArtifactIdentifier} is constructed from a WARC record. If the WARC record (i.e., artifact) is not indexed,
   * the map will contain a {@code null} for that identifier.
   *
   * @param warcPath A {@link Path} to the WARC file to scan.
   * @param index    The {@link ArtifactIndex} to query for {@link Artifact}s.
   * @return A {@link Map<ArtifactIdentifier, Artifact>} as described above.
   * @throws IOException thrown upon I/O errors iterating over WARC records in the file.
   */
  private Map<ArtifactIdentifier, Artifact> scanForIndexedArtifacts(Path warcPath, ArtifactIndex index) throws IOException {
    Map<ArtifactIdentifier, Artifact> indexedArtifacts = new HashMap<>();

    try (InputStream warcStream = new BufferedInputStream(store.getInputStreamAndSeek(warcPath, 0L))) {
      ArchiveReader reader = store.getArchiveReader(warcPath, warcStream);
      reader.setDigest(false);

      for (ArchiveRecord record : reader) {
        // Get the WARC record type from its headers
        ArchiveRecordHeader headers = record.getHeader();
        String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

        switch (WARCConstants.WARCRecordType.valueOf(recordType)) {
          case response:
          case resource:
            // Get Artifact from index and add to map
            ArtifactIdentifier artifactId = WarcArtifactDataUtil.buildArtifactIdentifier(headers);
            // FIXME: This could be made faster if the API allowed getting artifacts for more than
            //  one artifact ID at a time:
            Artifact indexed = index.getArtifact(artifactId);
            if (indexed != null) {
              indexedArtifacts.put(artifactId, indexed);
            }
            break;

          default:
            // WARC record does not contain an artifact
            break;
        }
      }
    }

    return indexedArtifacts;
  }
}
