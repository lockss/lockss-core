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

package org.lockss.rs.io.storage.warc;

import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.util.ListUtil;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.mockito.ArgumentMatchers;
import org.springframework.util.MultiValueMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.mockito.Mockito.*;

/**
 * Test class for {@link VolatileWarcArtifactDataStore}.
 */
public class TestVolatileWarcArtifactDataStore extends AbstractWarcArtifactDataStoreTest<VolatileWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
    VolatileWarcArtifactDataStore ds = new VolatileWarcArtifactDataStore();

    // Mock getArtifactIndex() called by data store
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    ds.setLockssRepository(repo);

    return ds;
  }

  @Override
  protected VolatileWarcArtifactDataStore makeWarcArtifactDataStore(
      ArtifactIndex index, VolatileWarcArtifactDataStore other) throws IOException {

    VolatileWarcArtifactDataStore n_store = new VolatileWarcArtifactDataStore();

    // Mock getArtifactIndex() called by data store
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    n_store.setLockssRepository(repo);

    n_store.warcs.putAll(other.warcs);
    return n_store;
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION SPECIFIC TEST UTILITY METHODS
  // *******************************************************************************************************************

  @Override
  protected Path[] expected_getBasePaths() {
    return new Path[]{VolatileWarcArtifactDataStore.DEFAULT_BASEPATH};
  }

  @Override
  protected Path[] expected_getTmpWarcBasePaths() {
    return new Path[]{expected_getBasePaths()[0].resolve(VolatileWarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH)};
  }

  @Override
  protected boolean pathExists(Path path) {
    return isFile(path) || isDirectory(path);
  }

  @Override
  protected boolean isDirectory(Path path) {
    return store.warcs.keySet().stream().anyMatch(x -> x.startsWith(path + "/"));
  }

  @Override
  protected boolean isFile(Path path) {
    return store.warcs.get(path) != null;
  }

  // *******************************************************************************************************************
  // * AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  /**
   * Test for {@link VolatileWarcArtifactDataStore#init()}.
   *
   * @throws Exception
   */
  @Override
  public void testInitDataStoreImpl() throws Exception {
    assertNotEquals(WarcArtifactDataStore.DataStoreState.STOPPED, store.getDataStoreState());
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#initNamespace(String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitNamespaceImpl() throws Exception {
    // NOP
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#initAu(String, String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitAuImpl() throws Exception {
    VolatileWarcArtifactDataStore ds = new VolatileWarcArtifactDataStore();
    assertEmpty(ds.initAu(NS1, AUID1));
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#makeStorageUrl(Path, MultiValueMap)}.
   *
   * @throws Exception
   */
  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    ArtifactIdentifier aid = new ArtifactIdentifier(NS1, AUID1, "http://example.com/u1", 1);

    Path activeWarcPath = store.getAppendablePermanentWarcInAU(aid.getNamespace(), aid.getAuid(), false, 4321L);

    URI expectedStorageUrl = URI.create(String.format(
        "volatile://%s?offset=%d&length=%d",
        activeWarcPath,
        1234L,
        5678L
    ));

    URI actualStorageUrl = store.makeWarcRecordStorageUrl(activeWarcPath, 1234L, 5678L);

    assertEquals(expectedStorageUrl, actualStorageUrl);
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#initWarc(Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitWarcImpl() throws Exception {
    // Mocks
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    Path warcPath = mock(Path.class);
    ds.warcs = new HashMap<>();

    // Mock behavior
    doCallRealMethod().when(ds).initWarc(warcPath);
    doCallRealMethod().when(ds).initFile(warcPath);
    doCallRealMethod().when(ds).getAppendableOutputStream(warcPath);

    // Call method
    ds.initWarc(warcPath);

    // Assert a ByteArrayOutputStream is in the map for this WARC path
    OutputStream output = ds.getAppendableOutputStream(warcPath);
    assertNotNull(output);
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#getWarcLength(Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testGetWarcLengthImpl() throws Exception {
    // Mocks
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    Path warcPath = mock(Path.class);

    ds.warcs = new HashMap<>();
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream output =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    // Write 123 bytes
    for (int i = 0; i < 123; i++) {
      output.write(i);
    }

    // Mock behavior
    doCallRealMethod().when(ds).getWarcLength(warcPath);

    // Assert WARC length is 0 if WARC is not found
    assertEquals(0L, ds.getWarcLength(warcPath));

    // Add a WARC the map of internal WARCs
    ds.warcs.put(warcPath, output);

    // Assert WARC length is the expected size if found
    assertEquals(123L, ds.getWarcLength(warcPath));
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#findWarcs(Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testFindWarcsImpl() throws Exception {
    Path basePath = Paths.get("/lockss");

    // Setup set of files
    Path[] files = {
        basePath.resolve("foo"),
        basePath.resolve("bar.warc.gz"),
        basePath.resolve("bar.warc"),
        basePath.resolve("xyzyy.txt"),
    };

    // Mocks
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    ds.warcs = mock(Map.class);

    // Mock behavior
    doCallRealMethod().when(ds).findWarcs(basePath);
    when(ds.warcs.keySet()).thenReturn(new HashSet<>(Arrays.asList(files)));

    // Assert only the WARCs are returned
    Collection<Path> result = ds.findWarcs(basePath);
    assertEquals(2, result.size());
    assertIterableEquals(ListUtil.list(files[1], files[2]), result);
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#removeWarc(Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testRemoveWarcImpl() throws Exception {
    // Mocks
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    ds.warcs = mock(Map.class);
    Path warcPath = mock(Path.class);

    // Mock behavior
    doCallRealMethod().when(ds).removeWarc(warcPath);

    // Call method
    ds.removeWarc(warcPath);

    // Assert that the WARC is removed from the internal map
    verify(ds.warcs).remove(warcPath);
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#getBlockSize()}.
   *
   * @throws Exception
   */
  @Override
  public void testGetBlockSizeImpl() throws Exception {
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    doCallRealMethod().when(ds).getBlockSize();

    // Assert we get the expected block size
    assertEquals(VolatileWarcArtifactDataStore.DEFAULT_BLOCKSIZE, ds.getBlockSize());
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#getFreeSpace(Path)}.
   *
   * @throws Exception
   */
  @Override
  public void testGetFreeSpaceImpl() throws Exception {
    Path randomPath = Paths.get(UUID.randomUUID().toString());

    // Setup spy of Runtime object
    Runtime s_runtime = spy(Runtime.getRuntime());
    when(s_runtime.freeMemory()).thenReturn(1234L);

    // Assert freeMemory() is called by getFreeSpace()
    assertEquals(s_runtime.freeMemory(), store.getFreeSpace(s_runtime, randomPath));

    // Assert valid freeMemory() output
    assertTrue(store.getFreeSpace(randomPath) > 0 && store.getFreeSpace(randomPath) <= s_runtime.maxMemory());
  }

  /**
   * Test for {@link VolatileWarcArtifactDataStore#initAuDir(Path, String, String)}.
   */
  @Override
  public void testInitAuDirImpl() throws Exception {
    Path basePath = Paths.get("/lockss");
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);

    doCallRealMethod().when(ds).initAuDir(eq(basePath), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    doCallRealMethod().when(ds).generateAUPath(eq(basePath), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    doCallRealMethod().when(ds).getNamespacePath(eq(basePath), ArgumentMatchers.anyString());
    doCallRealMethod().when(ds).getNamespacesBasePath(eq(basePath));

    Path expectedAuPath = Paths.get("/lockss/ns/ns1/au-116cf2bbfdcfbe0c9ad94987b00101cd");
    Path auPath = ds.initAuDir(basePath, NS1, AUID1);
    assertEquals(expectedAuPath, auPath);
  }

  // *******************************************************************************************************************
  // * TRUNCATION TESTS
  // *******************************************************************************************************************

  /**
   * Test that truncate() reduces size and retains the first M bytes.
   */
  @Test
  public void testTruncate_reducesSize() throws Exception {
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    byte[] data = new byte[]{10, 20, 30, 40, 50};
    out.write(data);
    assertEquals(5, out.size());

    out.truncate(3);
    assertEquals(3, out.size());

    byte[] result = out.toByteArray();
    assertEquals(3, result.length);
    assertEquals(10, result[0]);
    assertEquals(20, result[1]);
    assertEquals(30, result[2]);
  }

  /**
   * Test that truncate(0) results in an empty stream.
   */
  @Test
  public void testTruncate_toZero() throws Exception {
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    out.write(new byte[]{1, 2, 3});
    assertEquals(3, out.size());

    out.truncate(0);
    assertEquals(0, out.size());
    assertEquals(0, out.toByteArray().length);
  }

  /**
   * Test that truncate(count) (current length) is a no-op.
   */
  @Test
  public void testTruncate_toCurrentLength() throws Exception {
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    byte[] data = new byte[]{1, 2, 3, 4, 5};
    out.write(data);
    assertEquals(5, out.size());

    out.truncate(5);
    assertEquals(5, out.size());
    assertArrayEquals(data, out.toByteArray());
  }

  /**
   * Test that truncate() with a negative length throws IllegalArgumentException.
   */
  @Test
  public void testTruncate_negativeLength() throws Exception {
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    out.write(new byte[]{1, 2, 3});

    assertThrows(IllegalArgumentException.class, () -> out.truncate(-1));
  }

  /**
   * Test that truncate() with length greater than count throws IllegalArgumentException.
   */
  @Test
  public void testTruncate_lengthExceedsCount() throws Exception {
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    out.write(new byte[]{1, 2, 3});

    assertThrows(IllegalArgumentException.class, () -> out.truncate(10));
  }

  /**
   * Test that truncateWarc() throws FileNotFoundException when the WARC is not in the map.
   */
  @Test
  public void testTruncateWarc_notInMap() throws Exception {
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    ds.warcs = new HashMap<>();
    doCallRealMethod().when(ds).truncateWarc(ArgumentMatchers.any(), anyLong());

    Path warcPath = Paths.get("/lockss/test.warc");

    assertThrows(FileNotFoundException.class, () -> ds.truncateWarc(warcPath, 0));
  }

  /**
   * Test that truncateWarc() correctly truncates bytes in a WARC present in the map.
   */
  @Test
  public void testTruncateWarc_validTruncation() throws Exception {
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    ds.warcs = new HashMap<>();
    doCallRealMethod().when(ds).truncateWarc(ArgumentMatchers.any(), anyLong());

    Path warcPath = Paths.get("/lockss/test.warc");
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    byte[] data = new byte[]{10, 20, 30, 40, 50};
    out.write(data);
    ds.warcs.put(warcPath, out);

    ds.truncateWarc(warcPath, 3);

    assertEquals(3, out.size());
    byte[] result = out.toByteArray();
    assertEquals(10, result[0]);
    assertEquals(20, result[1]);
    assertEquals(30, result[2]);
  }

  /**
   * Test that truncateWarc() with length 0 empties the WARC.
   */
  @Test
  public void testTruncateWarc_toZero() throws Exception {
    VolatileWarcArtifactDataStore ds = mock(VolatileWarcArtifactDataStore.class);
    ds.warcs = new HashMap<>();
    doCallRealMethod().when(ds).truncateWarc(ArgumentMatchers.any(), anyLong());

    Path warcPath = Paths.get("/lockss/test.warc");
    VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream out =
        new VolatileWarcArtifactDataStore.TruncatableByteArrayOutputStream();

    out.write(new byte[]{1, 2, 3, 4, 5});
    ds.warcs.put(warcPath, out);

    ds.truncateWarc(warcPath, 0);

    assertEquals(0, out.size());
    assertEquals(0, out.toByteArray().length);
  }
}
