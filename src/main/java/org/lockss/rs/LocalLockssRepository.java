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

package org.lockss.rs;

import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.LocalArtifactIndex;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.util.rest.repo.LockssRepository;

import java.io.File;
import java.io.IOException;

/**
 * Local filesystem implementation of {@link LockssRepository}.
 */
public class LocalLockssRepository extends BaseLockssRepository {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Constructor which takes a content base path and name of the locally persisted index.
   *
   * @param repoStateDir       A {@link File} to the directory containing this repository's state.
   * @param basePath           A {@link File} containing the base path of this LOCKSS repository.
   * @param persistedIndexName A {@link String} with the name of the file where to persist the index.
   */
  public LocalLockssRepository(File repoStateDir, File basePath, String persistedIndexName) throws IOException {
    super(repoStateDir,
        new LocalArtifactIndex(basePath, persistedIndexName),
        new LocalWarcArtifactDataStore(new File[]{basePath}));
  }

  /**
   * Constructor which takes a base path and name of the local index to persist to disk.
   * <p>
   * Note: The local index is persisted to the first base path.
   *
   * @param repoStateDir       A {@link File} to the directory containing this repository's state.
   * @param basePaths          An array of {@link File} containing the base paths of this LOCKSS repository.
   * @param persistedIndexName A {@link String} with the name of the file where to persist the index.
   * @throws IOException
   */
  public LocalLockssRepository(File repoStateDir, File[] basePaths, String persistedIndexName) throws IOException {
    super(repoStateDir,
        new LocalArtifactIndex(basePaths[0], persistedIndexName),
        new LocalWarcArtifactDataStore(basePaths));
  }

  /**
   * Constructor which takes a local filesystem base path, and an instance of an ArtifactIndex implementation.
   *
   * @param repoStateDir       A {@link File} to the directory containing this repository's state.
   * @param index    An {@link ArtifactIndex} to use as this repository's artifact index.
   * @param basePath A {@link File} containing the base path of this LOCKSS repository.
   */
  public LocalLockssRepository(File repoStateDir, ArtifactIndex index, File basePath) throws IOException {
    super(repoStateDir, index, new LocalWarcArtifactDataStore(new File[]{basePath}));
  }

  /**
   * Constructor which takes a local filesystem base paths, and an instance of an ArtifactIndex implementation.
   *
   * @param repoStateDir       A {@link File} to the directory containing this repository's state.
   * @param index     An {@link ArtifactIndex} to use as this repository's artifact index.
   * @param basePaths An array of {@link File} containing the base path of this LOCKSS repository.
   * @throws IOException
   */
  public LocalLockssRepository(File repoStateDir, ArtifactIndex index, File[] basePaths) throws IOException {
    super(repoStateDir, index, new LocalWarcArtifactDataStore(basePaths));
  }
}
