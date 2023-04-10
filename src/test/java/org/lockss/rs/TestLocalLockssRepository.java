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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.RepositoryInfo;
import org.lockss.util.storage.StorageInfo;
import org.springframework.util.FileSystemUtils;

import java.io.File;

/**
 * Test class for {@link LocalLockssRepository}
 */
public class TestLocalLockssRepository extends AbstractLockssRepositoryTest {
    private final static L4JLogger log = L4JLogger.getLogger();

  File repoStateDir;
  File repoBaseDir;

  @Override
  public LockssRepository makeLockssRepository() throws Exception {
    repoStateDir = getTempDir();
    repoBaseDir = getTempDir();
    return new LocalLockssRepository(repoStateDir, repoBaseDir, null);
  }

    /**
     * Run after the test is finished.
     */
    @AfterEach
    @Override
    public void tearDownArtifactDataStore() throws Exception {
        super.tearDownArtifactDataStore();

        // Clean up the local repository directory tree used in the test.
        log.info("Cleaning up local repository directory used for tests: {}", repoBaseDir);
        if (!FileSystemUtils.deleteRecursively(repoBaseDir)) {
          log.warn("Failed to delete temporary directory " + repoBaseDir);
        }
    }

  @Test
  public void testRepoInfo() throws Exception {
    RepositoryInfo ri = repository.getRepositoryInfo();
    log.debug("repoinfo: {}", ri);
    StorageInfo ind = ri.getIndexInfo();
    StorageInfo sto = ri.getStoreInfo();
    assertEquals(VolatileArtifactIndex.ARTIFACT_INDEX_TYPE, ind.getType());
    assertEquals(LocalWarcArtifactDataStore.ARTIFACT_DATASTORE_TYPE,
		 sto.getType());
    assertTrue(sto.getSizeKB() > 0);
    assertFalse(sto.isSameDevice(ind));
  }
}
