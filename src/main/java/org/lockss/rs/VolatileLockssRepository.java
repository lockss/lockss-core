/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.util.io.FileUtil;

import java.io.IOException;
import java.io.File;

/**
 * Volatile ("in-memory") implementation of the LOCKSS Repository API.
 */
public class VolatileLockssRepository extends BaseLockssRepository {

  /**
   * Constructor.
   */
  public VolatileLockssRepository() throws IOException {
    this(false);
  }

  /**
   * Constructor.
   *
   * @param useWarcCompression A {@code boolean} indicating whether to use WARC compression.
   * @throws IOException
   */
  public VolatileLockssRepository(boolean useWarcCompression) throws IOException {
    super(new VolatileArtifactIndex(), new VolatileWarcArtifactDataStore());

    // Set compression use
    ((WarcArtifactDataStore)store).setUseWarcCompression(useWarcCompression);

    // Create a temporary repository state directory
    File stateDir = FileUtil.createTempDir("repostate", null);
    setRepositoryStateDir(stateDir);
    stateDir.deleteOnExit();
  }
}
