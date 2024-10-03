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
          ArtifactIndexVersion lastRecordedVersion = new ArtifactIndexVersion()
              .setIndexType(this.getClass().getSimpleName())
              .setIndexVersion(from + 1);

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
    FileUtils.touch(versionFile);
    try (BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(versionFile))) {
      mapper.writeValue(fos, version);
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
