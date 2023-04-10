/*
 * Copyright (c) 2017-2020, Board of Trustees of Leland Stanford Jr. University,
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

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.RepositoryInfo;
import org.lockss.util.storage.StorageInfo;

/**
 * Test class for {@code org.lockss.rs.VolatileLockssRepository}
 */
public class TestVolatileLockssRepository extends AbstractLockssRepositoryTest {
    private final static L4JLogger log = L4JLogger.getLogger();

    @Override
    public LockssRepository makeLockssRepository() throws IOException {
      return new VolatileLockssRepository();
    }

  protected boolean wantTempTmpDir() {
    return true;
  }

  @Test
  public void testRepoInfo() throws Exception {
    RepositoryInfo ri = repository.getRepositoryInfo();
    log.debug("repoinfo: {}", ri);
    StorageInfo ind = ri.getIndexInfo();
    StorageInfo sto = ri.getStoreInfo();
    assertEquals(VolatileArtifactIndex.ARTIFACT_INDEX_TYPE, ind.getType());
    assertEquals(VolatileWarcArtifactDataStore.ARTIFACT_DATASTORE_TYPE,
		 sto.getType());
  }

}
