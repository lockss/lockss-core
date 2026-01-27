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
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.storage.StorageInfo;
import org.springframework.util.FileSystemUtils;

import java.io.*;

/**
 * Test class for {@link LocalLockssRepository}
 */
public class TestLocalLockssRepository extends AbstractBaseLockssRepositoryTest {
    private final static L4JLogger log = L4JLogger.getLogger();

  File repoStateDir;
  File repoBaseDir;

  @Override
  public BaseLockssRepository getLockssRepository() throws Exception {
    if (repository == null) {
      repoStateDir = getTempDir();
      repoBaseDir = getTempDir();
      repository = new LocalLockssRepository(repoStateDir, repoBaseDir, null);
    }

    return (BaseLockssRepository) repository;
  }

    /**
     * Run after the test is finished.
     */
    @AfterEach
    @Override
    public void tearDownArtifactDataStore() throws Exception {
      super.tearDownArtifactDataStore();

        // Clean up the local repository dirs created for the test
      FileUtil.delTree(repoBaseDir);
      FileUtil.delTree(repoStateDir);
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

  @Test
  public void testGetStorageInfo() throws Exception {
      getLockssRepository().getArtifactDataStore().getStorageInfo();
  }

  @Test
  public void testRealRecordUri() {
    BaseLockssRepository repo = (BaseLockssRepository)repository;
    assertEquals("foo", repo.realRecordUri("foo"));
    assertEquals("foo", repo.realRecordUri("<foo>"));
    assertEquals("<foo", repo.realRecordUri("<foo"));
    assertEquals("foo>", repo.realRecordUri("foo>"));
    assertEquals("http://foo.bar/path", repo.realRecordUri("<http://foo.bar/path>"));
  }

  @Test
  public void testRuntimeErrorInjection() throws Exception {
    BaseLockssRepository repo = (BaseLockssRepository)repository;
    String u1 = "http://host1.com/path1/bar";
    String rules = """
      {"cond": { "op" : "AddArtifact", "uri":".*path1.*"},
       "action":{"ex":"IllegalArgumentException", "msg":"path1 add error"}
      }
    """;
    repo.setErrorInjectionRulesFromSpecs(rules);
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl(NS1, AUID1, u1)
      .setContentLength(10);
    assertThrowsMatch(IllegalArgumentException.class, "path1 add error",
                      () -> addUncommitted(spec));
  }

  @Test
  public void testRuntimeErrorInjection2() throws Exception {
    // This is a different code path due to the fq class name
    BaseLockssRepository repo = (BaseLockssRepository)repository;
    String u1 = "http://host1.com/path1/bar";
    String rules = """
      {"cond": { "op" : "AddArtifact", "uri":".*path1.*"},
       "action":{"ex":"java.lang.IllegalArgumentException", "msg":"path1 add error"}
      }
    """;
    repo.setErrorInjectionRulesFromSpecs(rules);
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl(NS1, AUID1, u1)
      .setContentLength(10);
    assertThrowsMatch(IllegalArgumentException.class, "path1 add error",
                      () -> addUncommitted(spec));
  }

  @Test
  public void testErrorInjection() throws Exception {
    BaseLockssRepository repo = (BaseLockssRepository)repository;

    String u1 = "http://host1.com/path1/bar";
    String u2 = "http://host1.com/path2/bar";

    String badRule = """
      {"cond": { "op" : "CommitArtifact", "uri":".*path2.*", "ords":2,4"},
       "action":{"ex":"IOException", "msg":"path2 error"}
    }
    """;

    try {
      repo.setErrorInjectionRulesFromSpecs(badRule);
      fail("Bad error injection rules should throw IllegalArgumentException: "
           + badRule);
    } catch (IllegalArgumentException e) {
    }

    // The Get error will trigger on version 3 because the second
    // added artifact isn't committed

    String rules = """
      {"cond": { "op" : "AddArtifact", "uri":".*path1.*"},
       "action":{"ex":"IOException", "msg":"path1 add error"}
      };
    {"cond": { "op" : "CommitArtifact", "uri":".*path2.*", "ords":"2,4"},
        "action":{"ex":"IOException", "msg":"path2 commit error"}
    };
    {"cond": { "op" : "GetArtifact", "uri":".*path2.*", "version":"3"},
        "action":{"ex":"IOException", "msg":"path2 get error"}
    }
    """;

    repo.setErrorInjectionRulesFromSpecs(rules);
    ArtifactSpec spec1 = ArtifactSpec.forNsAuUrl(NS1, AUID1, u1)
      .setContentLength(10);
    assertThrowsMatch(IOException.class, "path1 add error", () -> addUncommitted(spec1));

    ArtifactSpec spec2a = ArtifactSpec.forNsAuUrl(NS1, AUID1, u2)
      .setContentLength(10);
    // Need to make multiple ArtifactSpecs as the version and
    // committed status get modified when adding and/or committing
    ArtifactSpec spec2b = ArtifactSpec.forNsAuUrl(NS1, AUID1, u2)
      .setContentLength(10);
    ArtifactSpec spec2c = ArtifactSpec.forNsAuUrl(NS1, AUID1, u2)
      .setContentLength(10);
    ArtifactSpec spec2d = ArtifactSpec.forNsAuUrl(NS1, AUID1, u2)
      .setContentLength(10);
    ArtifactSpec spec2e = ArtifactSpec.forNsAuUrl(NS1, AUID1, u2)
      .setContentLength(10);
    // And one used just for gets
    ArtifactSpec spec2 = ArtifactSpec.forNsAuUrl(NS1, AUID1, u2);

    Artifact newArt2a = add(spec2a);
    commit(newArt2a);
    getArtifact(repository, spec2, false);
    Artifact newArt2b = add(spec2b);
    assertThrowsMatch(IOException.class, "path2 commit error", () -> commit(newArt2b));
    // This is version 2 but returns null because it isn't committed
    getArtifact(repository, spec2, false);
    Artifact newArt2c = add(spec2c);
    commit(newArt2c);
    // version 3 should trigger an error
    assertThrowsMatch(IOException.class, "path2 get error",
                      () -> getArtifact(repository, spec2, false));
    Artifact newArt2d = add(spec2d);
    assertThrowsMatch(IOException.class, "path2 commit error", () -> commit(newArt2d));
    // version 3 still current
    assertThrowsMatch(IOException.class, "path2 get error",
                      () -> getArtifact(repository, spec2, false));
    Artifact newArt2e = add(spec2e);
    commit(newArt2e);
    getArtifact(repository, spec2, false);

    getArtifact(repository, spec2a, false);
    getArtifact(repository, spec2b, false);
    assertThrowsMatch(IOException.class, "path2 get error",
                      () -> getArtifact(repository, spec2c, false));
    getArtifact(repository, spec2d, false);

  }

  private Artifact add(ArtifactSpec spec) throws IOException {
    if (!spec.hasContent()) {
      spec.generateContent();
    }
    log.info("adding: " + spec);

    ArtifactData ad = spec.getArtifactData();
    Artifact res = repository.addArtifact(ad);
    spec.setVersion(res.getVersion());
    return res;
  }

  private Artifact commit(Artifact art) throws IOException {
    return repository.commitArtifact(art.getNamespace(), art.getUuid());
  }

}
