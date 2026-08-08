/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

public abstract class AbstractArtifactIndex implements ArtifactIndex {
  private final static L4JLogger log = L4JLogger.getLogger();

  public final static String INDEX_STATE_DIR = "index";
  public final static String INDEX_VERSION_FILE = INDEX_STATE_DIR + "/version";

  private final static ObjectMapper mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  protected BaseLockssRepository repository;

  protected ArtifactIndexState indexState = ArtifactIndexState.STOPPED;

  public void updateIndexToVersion(int existingVersion, int targetVersion) throws IOException {
    log.info("Updating from version " + existingVersion + " to " + targetVersion + "...");

    for (int from = existingVersion; from < targetVersion; from++) {
      boolean success = false;
      try {
        updateIndexToVersion(from + 1);
        success = true;
      } finally {
        if (success) {
          ArtifactIndexVersion lastRecordedVersion = getArtifactIndexTargetVersion();
          lastRecordedVersion.setIndexVersion(from + 1);

          Path stateDirPath = repository.getRepositoryStateDirPath();
          Path versionFilePath = stateDirPath.resolve(INDEX_VERSION_FILE);
          File versionFile = versionFilePath.toFile();
          recordArtifactIndexVersion(versionFile, lastRecordedVersion);

          log.debug("Index " + lastRecordedVersion.getIndexType()
              + " updated to version " + lastRecordedVersion.getIndexVersion());
        }
        else break;
      }
    }
  }

  public static void recordArtifactIndexVersion(File versionFile, ArtifactIndexVersion version) throws IOException {
    try (FileOutputStream fos = FileUtils.openOutputStream(versionFile)) {
      try (BufferedOutputStream bos = new BufferedOutputStream(fos)) {
        mapper.writeValue(bos, version);
      }
    }
  }

  protected void updateIndexToVersion(int targetVersion) {
    if (targetVersion == 1) {
      // NOP
    }
  }

  public enum ArtifactIndexState {
    INITIALIZED,
    RUNNING,
    STOPPED
  }

  public ArtifactIndexState getState() {
    return indexState;
  }

  public void setState(ArtifactIndexState indexState) {
    log.debug("Changing index state {} -> {}", this.indexState, indexState);
    this.indexState = indexState;
  }

  /**
   * How much of this index's write surface is currently open.
   *
   * <p>Two levels of closed, not one, because quiescing an AU has two distinct
   * steps and they need different answers to the same question. Draining an
   * AU's outstanding work means letting that work finish, so a mode that
   * refuses everything cannot be used before the drain -- it would reject the
   * very operations being waited for. See
   * {@link DispatchingArtifactIndex#finishBulkStore}.
   */
  public enum WriteMode {
    /** Every modification is allowed. */
    WRITABLE,

    /**
     * New modifications are refused, but work already accepted may still
     * complete. In practice that means an artifact's storage URL can still be
     * updated by a copy task that was queued before the drain began, while new
     * indexes, commits and deletes are turned away.
     */
    DRAINING,

    /** Every modification is refused. */
    FROZEN
  }

  /**
   * The index's current write mode, and why it is not {@link
   * WriteMode#WRITABLE}. Volatile because they are set and read from different
   * threads: the thread quiescing an AU sets them, and the threads it is
   * protecting the index from are the ones that check them.
   */
  private volatile WriteMode writeMode = WriteMode.WRITABLE;
  private volatile String readOnlyReason = null;

  /**
   * Restricts this index's write surface until {@link #setWritable()} is
   * called. Reads are unaffected by any mode.
   *
   * <p>Subclasses enforce this by calling {@link #checkWritable()} and {@link
   * #checkWritableForCompletion()} from their mutating operations; an index
   * that does not call them ignores the mode entirely, so callers should not
   * assume this works for every implementation.
   *
   * @param mode   what to refuse.
   * @param reason why, included in the exception message attempted
   *               modifications get. Should name the operation holding it and
   *               the AU it applies to.
   */
  public void setWriteMode(WriteMode mode, String reason) {
    if (mode == null) {
      throw new IllegalArgumentException("Null write mode");
    }
    if (mode == WriteMode.WRITABLE) {
      throw new IllegalArgumentException("Use setWritable() to lift a restriction");
    }
    if (reason == null) {
      throw new IllegalArgumentException("Null reason");
    }

    log.debug("Changing artifact index write mode {} -> {}: {}", writeMode, mode, reason);
    readOnlyReason = reason;
    // Published after the reason so a thread that sees the new mode also sees
    // the reason that goes with it.
    writeMode = mode;
  }

  /** Allows every modification again. */
  public void setWritable() {
    log.debug("Changing artifact index write mode {} -> WRITABLE: was {}", writeMode, readOnlyReason);
    writeMode = WriteMode.WRITABLE;
    readOnlyReason = null;
  }

  public WriteMode getWriteMode() {
    return writeMode;
  }

  /** Whether any modification is currently refused. */
  public boolean isReadOnly() {
    return writeMode != WriteMode.WRITABLE;
  }

  /**
   * Throws unless every modification is allowed. Called by mutating operations
   * that introduce new work -- indexing, committing, deleting, clearing --
   * before they change anything.
   *
   * @throws IllegalStateException if the index is not writable. Unchecked
   *         because several of the mutating operations on {@link ArtifactIndex}
   *         do not declare {@code IOException}, and because this is a caller
   *         error rather than an I/O failure -- it should not be swallowed by a
   *         {@code catch (IOException)} meant for the database being down.
   */
  protected void checkWritable() {
    if (writeMode != WriteMode.WRITABLE) {
      throw new IllegalStateException("Artifact index is read-only: " + readOnlyReason);
    }
  }

  /**
   * Throws only if every modification is refused. Called by mutating operations
   * that complete work the index already accepted, which must be allowed to
   * finish while an AU is being drained.
   *
   * @throws IllegalStateException if the index is frozen.
   */
  protected void checkWritableForCompletion() {
    if (writeMode == WriteMode.FROZEN) {
      throw new IllegalStateException("Artifact index is read-only: " + readOnlyReason);
    }
  }

  @Override
  public void setLockssRepository(BaseLockssRepository repository) {
    this.repository = repository;
  }

  @Override
  public void startBulkStore(String namespace, String auid) {
    throw new UnsupportedOperationException("Bulk Store not supported in this ArtifactIndex");
  }

  @Override
  public void finishBulkStore(String namespace, String auid,
                              int copyBatchSize) throws IOException {
    throw new UnsupportedOperationException("Bulk Store not supported in this ArtifactIndex");
  }
}
