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

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.archive.format.warc.WARCConstants;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.rs.io.index.solr.TestSolrArtifactIndex;
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.time.TimeBase;
import org.mockito.ArgumentMatchers;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link LocalWarcArtifactDataStore}, the local filesystem based implementation of
 * {@link WarcArtifactDataStore}.
 */
public class TestLocalWarcArtifactDataStore extends AbstractWarcArtifactDataStoreTest<LocalWarcArtifactDataStore> {
  private final static L4JLogger log = L4JLogger.getLogger();
  private File testRepoBasePath;

  // *******************************************************************************************************************
  // * JUNIT
  // *******************************************************************************************************************

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index) throws IOException {
    testRepoBasePath = getTempDir();
    testRepoBasePath.mkdirs();

    LocalWarcArtifactDataStore ds =
        new LocalWarcArtifactDataStore(new File[]{testRepoBasePath});

    // Mock getArtifactIndex() called by data store
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    ds.setLockssRepository(repo);

    return ds;
  }

  @Override
  protected LocalWarcArtifactDataStore makeWarcArtifactDataStore(ArtifactIndex index, LocalWarcArtifactDataStore other)
      throws IOException {

    LocalWarcArtifactDataStore ds =
        new LocalWarcArtifactDataStore(other.getBasePaths());

    // Mock getArtifactIndex() called by data store
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    ds.setLockssRepository(repo);

    return ds;
  }

  // *******************************************************************************************************************
  // * IMPLEMENTATION-SPECIFIC TEST UTILITY METHODS
  // *******************************************************************************************************************

  @Override
  protected boolean pathExists(Path path) throws IOException {
    return path.toFile().exists();
  }

  @Override
  protected boolean isDirectory(Path path) {
    return path.toFile().isDirectory();
  }

  @Override
  protected boolean isFile(Path path) {
    return path.toFile().isFile();
  }

  // *******************************************************************************************************************

  @Override
  protected Path[] expected_getTmpWarcBasePaths() {
    return new Path[]{testRepoBasePath.toPath().resolve(WarcArtifactDataStore.DEFAULT_TMPWARCBASEPATH)};
  }

  @Override
  protected Path[] expected_getBasePaths() {
    return new Path[]{testRepoBasePath.toPath()};
  }

  // *******************************************************************************************************************
  // * TEST: Constructors
  // *******************************************************************************************************************

//  @Test
//  public void testLocalWarcArtifactDataStoreConstructor() throws Exception {
//  }

  // *******************************************************************************************************************
  // * TEST: AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    ArtifactIdentifier aid = new ArtifactIdentifier(NS1, AUID1,"http://example.com/u1", 1);
    long pendingArtifactSize = 1234L;

    Path activeWarcPath = store.getAuActiveWarcPath(aid.getNamespace(), aid.getAuid(), pendingArtifactSize, false);

    URI expectedStorageUrl = URI.create(String.format(
        "file://%s?offset=%d&length=%d",
        activeWarcPath,
        1234L,
        5678L
    ));

    URI actualStorageUrl = store.makeWarcRecordStorageUrl(activeWarcPath, 1234L, 5678L);

    assertEquals(expectedStorageUrl, actualStorageUrl);
  }

  @Override
  public void testInitWarcImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedWarcPath = mock(Path.class);
    File mockedWarcFile = mock(File.class);

    // Mock behavior
    when(mockedWarcPath.toFile()).thenReturn(mockedWarcFile);
    doCallRealMethod().when(ds).initWarc(mockedWarcPath);

    // Assert a new WARC is not initialized if the WARC already exists
    when(mockedWarcFile.exists()).thenReturn(true);
    ds.initWarc(mockedWarcPath);
    verify(ds, never()).initFile(mockedWarcFile);
    verify(ds, never()).getAppendableOutputStream(mockedWarcPath);

    // Assert a new WARC is initialized otherwise
    when(mockedWarcFile.exists()).thenReturn(false);
    ds.initWarc(mockedWarcPath);
    verify(ds, times(1)).initFile(mockedWarcFile);
  }

  @Override
  public void testGetWarcLengthImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    doCallRealMethod().when(ds).getWarcLength(mockedPath);

    // Assert length() is called on Path.toFile()
    ds.getWarcLength(mockedPath);
    verify(mockedFile, times(1)).length();
  }

  @Override
  public void testFindWarcsImpl() throws Exception {
    // Mocks
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Connect mocked File to mocked Path
    when(mockedPath.toFile()).thenReturn(mockedFile);

    // Assert findWarcs() returns empty set if path does not exist
    when(mockedFile.exists()).thenReturn(false);
    assertEmpty(store.findWarcs(mockedPath));

    // Assert findWarcs() returns empty set if path exists but is not a directory
    when(mockedFile.exists()).thenReturn(true);
    when(mockedFile.isDirectory()).thenReturn(false);
    assertThrows(IllegalStateException.class, () -> store.findWarcs(mockedPath));

    // Trigger an IOException because of an IOException in listFiles()
    when(mockedFile.exists()).thenReturn(true);
    when(mockedFile.isDirectory()).thenReturn(true);
    when(mockedFile.listFiles()).thenReturn(null);
    assertThrows(IOException.class, () -> store.findWarcs(mockedPath));

    // Setup to trigger a recursion of findWarcs()
    File mockedFileDir = mockFile(true, true, "test");
    when(mockedFile.listFiles()).thenReturn(new File[]{
        mockedFileDir
    });

    // Verify recursion of findWarcs()
    LocalWarcArtifactDataStore ds = spy(store);
    ds.findWarcs(mockedPath);
    verify(ds).findWarcs(mockedFileDir.toPath());

    // Setup WARC file discovery for current directory
    File[] mockedFiles = new File[]{
        mockFile(true, false, "test"),
        mockFile(true, false, "test1"),
        mockFile(true, false, "test.warc"),
        mockFile(true, false, "test.warc.gz"),
    };

    when(mockedFile.listFiles()).thenReturn(mockedFiles);

    Collection<Path> paths = store.findWarcs(mockedPath);

    log.trace("paths = {}", paths);

    // Assert findWarcs() returns only WARCs
    assertTrue(paths.stream().map(Path::toString)
        .allMatch(name ->
            name.endsWith(WARCConstants.DOT_WARC_FILE_EXTENSION) ||
            name.endsWith(WARCConstants.DOT_COMPRESSED_WARC_FILE_EXTENSION)
//            FilenameUtils.getExtension(name).equalsIgnoreCase(WARCConstants.WARC_FILE_EXTENSION) ||
//            FilenameUtils.getExtension(name).equalsIgnoreCase(WARCConstants.COMPRESSED_WARC_FILE_EXTENSION)
        )
    );
  }

  private File mockFile(boolean exists, boolean isDir, String name) {
    File mockedFile = mock(File.class);

    when(mockedFile.exists()).thenReturn(exists);
    when(mockedFile.isFile()).thenReturn(!isDir);
    when(mockedFile.isDirectory()).thenReturn(isDir);
    when(mockedFile.getName()).thenReturn(name);
    when(mockedFile.toPath()).thenReturn(Paths.get(name));

    return mockedFile;
  }

  @Override
  public void testRemoveWarcImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    doCallRealMethod().when(ds).removeWarc(mockedPath);

    // Assert delete() is called on Path.toFile()
    ds.removeWarc(mockedPath);
    verify(mockedFile, times(1)).delete();
  }

  @Override
  public void testGetBlockSizeImpl() throws Exception {
    assertEquals(LocalWarcArtifactDataStore.DEFAULT_BLOCKSIZE, store.getBlockSize());
  }

  @Override
  public void testGetFreeSpaceImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path mockedPath = mock(Path.class);
    File mockedFile = mock(File.class);

    // Mock behavior
    when(mockedPath.toFile()).thenReturn(mockedFile);
    doCallRealMethod().when(ds).getFreeSpace(mockedPath);

    // Assert getFreeSpace() is called on Path.toFile()
    ds.getFreeSpace(mockedPath);
    verify(mockedFile, times(1)).getFreeSpace();
  }

  /**
   * Test for {@link LocalWarcArtifactDataStore#initAuDir(String, String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitAuDirImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path basePath = mock(Path.class);
    Path auPath = mock(Path.class);
    File auPathFile = mock(File.class);

    // Mock behavior
    doCallRealMethod().when(ds).initAuDir(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    when(auPath.toFile()).thenReturn(auPathFile);
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath);

    // Assert IllegalStateException thrown if getBasePaths() returns null or is empty
    when(ds.getBasePaths()).thenReturn(null);
    assertThrows(IllegalStateException.class, () -> ds.initAuDir(NS1, AUID1));
    when(ds.getBasePaths()).thenReturn(new Path[]{});
    assertThrows(IllegalStateException.class, () -> ds.initAuDir(NS1, AUID1));

    when(ds.getBasePaths()).thenReturn(new Path[]{basePath});

    // Assert directory created if not directory
    when(auPathFile.isDirectory()).thenReturn(false);
    assertEquals(auPath, ds.initAuDir(NS1, AUID1));
    verify(ds).mkdirs(auPath);
    clearInvocations(ds);

    // Assert directory is *not* created if directory
    when(auPathFile.isDirectory()).thenReturn(true);
    assertEquals(auPath, ds.initAuDir(NS1, AUID1));
    verify(ds, never()).mkdirs(auPath);
    clearInvocations(ds);
  }

  @Override
  public void testInitDataStoreImpl() throws Exception {
    assertTrue(Arrays.stream(store.getBasePaths())
        .map(this::isDirectory)
        .allMatch(Predicate.isEqual(true)));

    assertNotEquals(WarcArtifactDataStore.DataStoreState.STOPPED, store.getDataStoreState());
  }

  @Override
  public void testInitNamespaceImpl() throws Exception {
    final Path[] nsPaths = new Path[]{Paths.get("/a"), Paths.get("/b")};

    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);

    // Mock behavior
    when(ds.getNamespacePaths(NS1)).thenReturn(nsPaths);

    // Initialize a namespace
    doCallRealMethod().when(ds).initNamespace(NS1);
    ds.initNamespace(NS1);

    // Assert directory structures were created
    verify(ds).mkdirs(nsPaths);
  }

  /**
   * Test for {@link LocalWarcArtifactDataStore#initAu(String, String)}.
   *
   * @throws Exception
   */
  @Override
  public void testInitAuImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path basePath = mock(Path.class);

    // Mock behavior
    doCallRealMethod().when(ds).clearAuMaps();
    doCallRealMethod().when(ds).initAu(NS1, AUID1);

    // Assert IllegalStateException thrown if no base paths configured in data store
    when(ds.getBasePaths()).thenReturn(null);
    assertThrows(IllegalStateException.class, () -> ds.initAu(NS1, AUID1));

    // Assert IllegalStateException thrown if empty base paths
    when(ds.getBasePaths()).thenReturn(new Path[]{});
    assertThrows(IllegalStateException.class, () -> ds.initAu(NS1, AUID1));

    // FIXME: Initialize maps
//    FieldSetter.setField(ds, ds.getClass().getDeclaredField("auPathsMap"), new HashMap<>());
//    FieldSetter.setField(ds, ds.getClass().getDeclaredField("auActiveWarcsMap"), new HashMap<>());
    ds.clearAuMaps();

    // Assert if no AU paths found then a new one is created
    when(ds.getBasePaths()).thenReturn(new Path[]{basePath});
    Path auPath = mockPathFile(false);
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath);
    ds.initAu(NS1, AUID1);
    verify(ds).initAuDir(NS1, AUID1);
    clearInvocations(ds);

    // Assert if existing AU paths are found on disk then they are just returned
    auPath = mockPathFile(true);
    when(ds.getAuPath(basePath, NS1, AUID1)).thenReturn(auPath);
    List<Path> auPaths = new ArrayList<>();
    auPaths.add(auPath);
    assertIterableEquals(auPaths, ds.initAu(NS1, AUID1));
    verify(ds, never()).initAuDir(NS1, AUID1);
    clearInvocations(ds);
  }

  private Path mockPathFile(boolean isDirectory) {
    Path path = mock(Path.class);
    File file = mock(File.class);
    when(path.toFile()).thenReturn(file);
    when(file.isDirectory()).thenReturn(isDirectory);
    return path;
  }

  /**
   * Instrumentation for debugging and profiling the reindex of WARCs in a local data store against
   * an (embedded) Solr index. Disabled by default.
   */
  @Test
  @Disabled
  public void testWarc() throws Exception {
    File baseDir = new File("/tmp/lockss");
    File stateDir = new File("/tmp/lockss/state");
    File indexStateDir = new File("/tmp/lockss/state/index");
    File reindexState = new File("/tmp/lockss/state/index/reindex");

    LocalWarcArtifactDataStore ds = new LocalWarcArtifactDataStore(baseDir);
    SolrArtifactIndex idx = makeEmbeddedSolr();
    BaseLockssRepository repo = new BaseLockssRepository(stateDir, idx, ds);

    ds.setLockssRepository(repo);
    idx.setLockssRepository(repo);

    FileUtil.delTree(indexStateDir);
    indexStateDir.mkdirs();
    FileUtils.touch(reindexState);

    TimeBase.setReal();
    repo.initRepository();
  }

  private static SolrArtifactIndex makeEmbeddedSolr() throws IOException {
    String TEST_SOLR_CORE_NAME = "test";
    String TEST_SOLR_HOME_RESOURCES = "/solr/.filelist";

    // Create a test Solr home directory and populate it
    File tmpSolrHome = FileUtil.createTempDir("testSolrHome", null);
    copyResourcesForTests(TEST_SOLR_HOME_RESOURCES, tmpSolrHome.toPath());

    // Start EmbeddedSolrServer
    EmbeddedSolrServer client =
        new EmbeddedSolrServer(tmpSolrHome.toPath(), TEST_SOLR_CORE_NAME);

    CoreContainer cc = client.getCoreContainer();

//    cc.unload(TEST_SOLR_CORE_NAME);
//    FileUtil.delTree(tmpSolrHome);
//    copyResourcesForTests(TEST_SOLR_HOME_RESOURCES, tmpSolrHome.toPath());
    cc.load();
    cc.waitForLoadingCore(TEST_SOLR_CORE_NAME, 1000);

    return new SolrArtifactIndex(client, TEST_SOLR_CORE_NAME);
  }

  private static void copyResourcesForTests(String filelistRes, Path dstPath) throws IOException {
    // Read file list
    try (InputStream input = TestSolrArtifactIndex.class
        .getResourceAsStream(filelistRes)) {

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {

        // Name of resource to load
        String resourceName;

        // Iterate over resource names from the list and copy each into the target directory
        while ((resourceName = reader.readLine()) != null) {
          // Source resource URL
          URL srcUrl = TestSolrArtifactIndex.class
              .getResource(String.format("/solr/%s", resourceName));

          log.info("Copying resource {}", srcUrl);

          // Destination file
          File dstFile = dstPath.resolve(resourceName).toFile();

          // Copy resource to file
          FileUtils.copyURLToFile(srcUrl, dstFile);
        }
      }
    }
  }
}