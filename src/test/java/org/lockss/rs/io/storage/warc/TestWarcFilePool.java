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

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.util.ListUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.util.time.TimeBase;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.lockss.rs.io.storage.warc.WarcArtifactState.PENDING_COPY;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link WarcFilePool}.
 */
class TestWarcFilePool extends LockssTestCase5 {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Tests for {@link WarcFilePool#createWarcFile()}.
   */
  @Test
  public void testCreateWarcFile() throws Exception {
    Path baseDir1 = Paths.get("/tmp1");
    Path baseDir2 = Paths.get("/tmp2");
    Path[] baseDirs = new Path[]{baseDir1, baseDir2};

    boolean useCompression = true;

    WarcArtifactDataStore store = Mockito.spy(WarcArtifactDataStore.class);
    when(store.getBasePaths()).thenReturn(baseDirs);
    doCallRealMethod().when(store).setUseWarcCompression(ArgumentMatchers.anyBoolean());
    doCallRealMethod().when(store).getWarcFileExtension();
    doCallRealMethod().when(store).getUseWarcCompression();

    store.setUseWarcCompression(useCompression);

    when(store.getFreeSpace(baseDir1)).thenReturn(1L);
    when(store.getFreeSpace(baseDir2)).thenReturn(2L);

    WarcFilePool pool = new WarcFilePool(store);

    WarcFile result = pool.createWarcFile();

    assertNotNull(result);
    assertFalse(result.isCheckedOut());
    assertFalse(result.isMarkedForGC());
    assertEquals(useCompression, result.isCompressed());
    assertEquals(0, result.getLength());
    assertEquals(0, result.getStats().getArtifactsTotal());

    Path filePath = result.getPath();
    String fileName = filePath.getFileName().toString();
    assertTrue(filePath.startsWith(baseDir2));
    assertTrue(StringUtils.endsWithIgnoreCase(fileName, useCompression ? ".warc.gz" : ".warc"));
  }

  /**
   * Test for {@link WarcFilePool#generateTmpWarcFileName()}.
   */
  @Test
  public void testGenerateTmpWarcFileName() throws Exception {
    WarcArtifactDataStore store = mock(WarcArtifactDataStore.class);
    doCallRealMethod().when(store).getWarcFileExtension();
    doCallRealMethod().when(store).setUseWarcCompression(ArgumentMatchers.anyBoolean());

    WarcFilePool pool = new WarcFilePool(store);

    // WARC compression disabled
    {
      store.setUseWarcCompression(false);
      String fileName = pool.generateTmpWarcFileName();

      assertNotNull(fileName);
      assertTrue(StringUtils.endsWithIgnoreCase(fileName, ".warc"));
    }

    // WARC compression enabled
    {
      store.setUseWarcCompression(true);
      String fileName = pool.generateTmpWarcFileName();

      assertNotNull(fileName);
      assertTrue(StringUtils.endsWithIgnoreCase(fileName, ".warc.gz"));
    }
  }

  /**
   * Test for {@link WarcFilePool#checkoutWarcFileForWrite()}.
   */
  @Test
  public void testCheckoutWarcFileForWrite() throws Exception {
    Path baseDir = Paths.get("/tmp");
    boolean useCompression = true;

    WarcArtifactDataStore store = mock(WarcArtifactDataStore.class);
    when(store.getBasePaths()).thenReturn(new Path[]{baseDir});
    when(store.getMaxArtifactsThreshold()).thenReturn(1);
    when(store.getThresholdWarcSize()).thenReturn(1L);
    when(store.getUseWarcCompression()).thenReturn(useCompression);

    // Assert an empty pool creates a new WARC
    {
      WarcFilePool pool = Mockito.spy(new WarcFilePool(store));

      assertEmpty(pool.allWarcs);

      WarcFile warcFile = pool.checkoutWarcFileForWrite();
      verify(pool, Mockito.atMost(1)).createWarcFile();

      assertNotNull(warcFile);
      assertTrue(warcFile.isCheckedOut());
    }

    // Assert a pool full of ineligible WARCs results in a new WARC
    WarcFilePool pool = Mockito.spy(new WarcFilePool(store));

    WarcFile warc1 = new WarcFile(baseDir.resolve("test-warc-1"), useCompression)
        .setLength(1);

    WarcFile warc2 = new WarcFile(baseDir.resolve("test-warc-2"), useCompression);
    warc2.getStats().setArtifactsTotal(1);

    WarcFile warc3 = new WarcFile(baseDir.resolve("test-warc-3"), useCompression)
        .setCheckedOut(true);

    WarcFile warc4 = new WarcFile(baseDir.resolve("test-warc-4"), !useCompression);

    List<WarcFile> warcFiles = ListUtil.list(warc1, warc2, warc3, warc4);
    pool.allWarcs.addAll(warcFiles);

    WarcFile result1 = pool.checkoutWarcFileForWrite();
    verify(pool, Mockito.atMost(1)).createWarcFile();
    clearInvocations(pool);

    assertNotNull(result1);
    assertTrue(result1.isCheckedOut());
    assertNotMember(result1, warcFiles);

    // Assert returning the WarcFile and calling checkoutWarcFileForWrite
    // again gives us the same WarcFile
    pool.returnWarcFile(result1);
    assertFalse(result1.isCheckedOut());

    WarcFile result2 = pool.checkoutWarcFileForWrite();
    verify(pool, never()).createWarcFile();
    clearInvocations(pool);

    assertNotNull(result2);
    assertTrue(result2.isCheckedOut());
    assertNotMember(result2, warcFiles);
    assertEquals(result1, result2);
  }

  /**
   * Assert an object is not a member of a collection.
   */
  private void assertNotMember(Object actual, Collection<?> objs) {
    objs.forEach(x -> assertNotEquals(x, actual));
  }

  /**
   * Test for {@link WarcFilePool#returnWarcFile(WarcFile)}.
   */
  @Test
  public void testReturnWarcFile() throws Exception {
    // Mock parameters (from a WarcArtifactDataStore) to the WarcFilePool
    WarcArtifactDataStore store = mock(WarcArtifactDataStore.class);
    when(store.getBlockSize()).thenReturn(4096L);
    when(store.getThresholdWarcSize()).thenReturn(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);
    when(store.getMaxArtifactsThreshold()).thenReturn(WarcArtifactDataStore.DEFAULT_THRESHOLD_ARTIFACTS);

    // Assert WarcFile membership in the pool when artifact counter threshold is met and returned
    {
      WarcFilePool pool = new WarcFilePool(store);
      WarcFile warc = new WarcFile(Paths.get("/lockss/test.warc"), false);
      warc.getStats().setArtifactsTotal(WarcArtifactDataStore.DEFAULT_THRESHOLD_ARTIFACTS);

      pool.returnWarcFile(warc);
      assertTrue(pool.isInPool(warc));

      pool.runGC();
      assertFalse(pool.isInPool(warc));
    }

    // Assert WarcFile membership in the pool when file size threshold is met and returned
    {
      WarcFilePool pool = new WarcFilePool(store);
      WarcFile warc = new WarcFile(Paths.get("/lockss/test.warc"), false);
      warc.setLength(WarcArtifactDataStore.DEFAULT_THRESHOLD_WARC_SIZE);

      pool.returnWarcFile(warc);
      assertTrue(pool.isInPool(warc));

      pool.runGC();
      assertFalse(pool.isInPool(warc));
    }

    // Assert WarcFile membership in the pool when marked for release and returned
    {
      WarcFilePool pool = new WarcFilePool(store);
      WarcFile warc = new WarcFile(Paths.get("/lockss/test.warc"), false);
      warc.release();

      pool.returnWarcFile(warc);
      assertTrue(pool.isInPool(warc));

      pool.runGC();
      assertFalse(pool.isInPool(warc));
    }
  }

  /**
   * Test for {@link WarcFilePool#runGC()}.
   */
  @Test
  public void testRunGC() throws Exception {
    BaseLockssRepository repository = mock(BaseLockssRepository.class);

    WarcArtifactDataStore ds = new VolatileWarcArtifactDataStore();
    ds.setLockssRepository(repository);

    ArtifactIndex index = new VolatileArtifactIndex();
    index.setLockssRepository(repository);

    when(repository.getArtifactIndex()).thenReturn(index);
    when(repository.getArtifactDataStore()).thenReturn(ds);

    WarcFilePool pool = ds.tmpWarcPool;

    // Assert an uncommitted and unexpired artifact results in a no-op and deleting
    // the artifact results in a removal of the temporary WARC
    {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid("test")
          .setUrl("http://lockss.org/test/")
          .setCollectionDate(1234L)
          .generateContent();

      Artifact a = ds.addArtifactData(spec.getArtifactData());
      pool.runGC();
      assertEquals(1, pool.allWarcs.size());

      ds.deleteArtifactData(a);
      pool.runGC();
      assertEquals(0, pool.allWarcs.size());
    }

    // Assert a copied artifact results in removal of the temporary WARC
    {
      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid("test")
          .setUrl("http://lockss.org/test/")
          .setCollectionDate(1234L)
          .generateContent();

      Artifact a = ds.addArtifactData(spec.getArtifactData());
      Future<Artifact> f = ds.commitArtifactData(a);
      f.get(TIMEOUT_SHOULDNT, TimeUnit.MILLISECONDS);

      pool.runGC();
      assertEquals(0, pool.allWarcs.size());
    }

    // Assert an expired artifact results in removal of the temporary WARC
    {
      TimeBase.setSimulated(1234L);

      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid(UUID.randomUUID().toString())
          .setUrl("http://lockss.org/test/")
          .setCollectionDate(1234L)
          .generateContent();

      ds.addArtifactData(spec.getArtifactData());
      pool.runGC();
      assertEquals(1, pool.allWarcs.size());

      TimeBase.step(ds.getUncommittedArtifactExpiration());
      pool.runGC();
      assertEquals(0, pool.allWarcs.size());
    }

    // Assert artifact in PENDING_COPY does not result in the removal of the temporary WARC
    {
      // Replace striped executor service with mock that does nothing
      ds.stripedExecutor =
          mock(WarcArtifactDataStore.FutureRecordingStripedExecutorService.class);

      ArtifactSpec spec = new ArtifactSpec()
          .setArtifactUuid("test")
          .setUrl("http://lockss.org/test/")
          .setCollectionDate(1234L)
          .generateContent();

      // Add and commit artifact
      Artifact uncommitted = ds.addArtifactData(spec.getArtifactData());
      ds.commitArtifactData(uncommitted);

      // Assert artifact's status is (stuck at) PENDING_COPY
      Artifact indexed = index.getArtifact(spec.getArtifactIdentifier());
      assertEquals(PENDING_COPY, ds.getWarcArtifactState(indexed, false));

      // Assert temporary WARC remains after running GC
      pool.runGC();
      assertEquals(1, pool.allWarcs.size());
    }
  }
}