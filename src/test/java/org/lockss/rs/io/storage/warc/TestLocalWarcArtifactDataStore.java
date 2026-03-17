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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.*;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.archive.format.warc.WARCConstants;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.rs.io.index.solr.TestSolrArtifactIndex;
import org.lockss.util.ListUtil;
import org.lockss.util.PatternIntMap;
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.mockito.ArgumentMatchers;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Future;
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
  // * TEST: LocalWarcArtifactDataStore specific methods
  // *******************************************************************************************************************

  @Test
  public void testInitPermanentWarcsAndAUs() throws Exception {
    File baseDir1 = getTempDir();
    File baseDir2 = getTempDir();

    Path baseDirPath1 = baseDir1.toPath();
    Path baseDirPath2 = baseDir2.toPath();

    Path auBaseDir1 =
        baseDirPath1.resolve("ns/" + NS1 + "/au-" + DigestUtils.md5Hex(AUID1));
    Path auBaseDir2 =
        baseDirPath2.resolve("ns/" + NS1 + "/au-" + DigestUtils.md5Hex(AUID1));

    Map<Path, Integer> diskSpaceMap = new HashMap<>();
    int expectedWarcRecordSize = 1850; // WARC size empirically through running the test
    diskSpaceMap.put(baseDirPath1, expectedWarcRecordSize);
    diskSpaceMap.put(baseDirPath2, expectedWarcRecordSize * 3 + 1);

    TestingLocalWarcArtifactDataStore ds =
        new TestingLocalWarcArtifactDataStore(new File[]{baseDir1, baseDir2});

    // Set initial disk space map
    updateDiskSpaceMap(ds, diskSpaceMap, null);

    VolatileArtifactIndex index = new VolatileArtifactIndex();
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    ds.setLockssRepository(repo);

    // Verify that calling initAu on a AU with no existing AU directories results in
    // an empty list of AU base directories
    List<Path> auBaseDirs = ds.initAu(NS1, AUID1);
    assertEmpty(auBaseDirs);
    assertTrue(new File(baseDir1, "ns/" + NS1).isDirectory());
    assertTrue(new File(baseDir2, "ns/" + NS1).isDirectory());
    assertEquals(0, new File(baseDir1, "ns/" + NS1).listFiles().length);
    assertEquals(0, new File(baseDir2, "ns/" + NS1).listFiles().length);

    // Verify that adding an artifact in a new AU results in only one directory being created
    // on the disk with the most space (i.e., baseDirPath2)
    Artifact stored1 = createArtifactInAU(ds, NS1, AUID1, 1000L);
    updateDiskSpaceMap(ds, diskSpaceMap, stored1.getStorageUrl());
    assertEquals(List.of(auBaseDir2), ds.initAu(NS1, AUID1));
    assertEquals(List.of(auBaseDir2), ds.findExistingAUPaths(NS1, AUID1));
    assertTrue(isStorageUrlPathUnderBaseDirectory(stored1.getStorageUrl(), baseDirPath2));

    // Verify adding an artifact to a different AUID2 results in the directory being
    // created on the disk the most space (i.e., still baseDirPath2)
    Artifact stored2 = createArtifactInAU(ds, NS1, AUID2, 1000L);
    updateDiskSpaceMap(ds, diskSpaceMap, stored2.getStorageUrl());
    assertEquals(List.of(auBaseDir2), ds.initAu(NS1, AUID1));
    assertEquals(List.of(auBaseDir2), ds.findExistingAUPaths(NS1, AUID1));
    assertTrue(isStorageUrlPathUnderBaseDirectory(stored2.getStorageUrl(), baseDirPath2));

    // Verify that creating a second artifact AUID1 results in a write to a file in the
    // existing directory (i.e., baseDirPath2) because it has just enough space
    Artifact stored3 = createArtifactInAU(ds, NS1, AUID1, 1000L);
    updateDiskSpaceMap(ds, diskSpaceMap, stored3.getStorageUrl());
    assertEquals(List.of(auBaseDir2), ds.initAu(NS1, AUID1));
    assertEquals(List.of(auBaseDir2), ds.findExistingAUPaths(NS1, AUID1));
    assertTrue(isStorageUrlPathUnderBaseDirectory(stored3.getStorageUrl(), baseDirPath2));

    // Verify adding a third artifact AUID1 to a now filled filesystem results in a new
    // directory being created on the other filesystem (basePath1)
    Artifact stored4 = createArtifactInAU(ds, NS1, AUID1, 1000L);
    updateDiskSpaceMap(ds, diskSpaceMap, stored4.getStorageUrl());
    assertEquals(List.of(auBaseDir1, auBaseDir2), ds.initAu(NS1, AUID1));
    assertEquals(List.of(auBaseDir1, auBaseDir2), ds.findExistingAUPaths(NS1, AUID1));
    assertTrue(isStorageUrlPathUnderBaseDirectory(stored4.getStorageUrl(), baseDirPath1));
  }

  private boolean isStorageUrlPathUnderBaseDirectory(String storageUrl, Path auBasePath) {
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(storageUrl).build();
    return uriComponents.getPath().startsWith(auBasePath.toString());
  }

  private void updateDiskSpaceMap(TestingLocalWarcArtifactDataStore ds,
                                  Map<Path, Integer> diskSpaceMap,
                                  String storageUrl) throws Exception {

    List<String> strPatternIntMap = new ArrayList<>();

    for (Map.Entry<Path, Integer> entry : diskSpaceMap.entrySet()) {
      int currentSpace = entry.getValue();

      if (storageUrl != null) {
        UriComponents uriComponents = UriComponentsBuilder.fromUriString(storageUrl).build();
        if (uriComponents.getPath().startsWith(entry.getKey().toString())) {
          long storedLength = Long.parseLong(uriComponents.getQueryParams().getFirst("length"));
          currentSpace -= storedLength;

          log.info("Updating disk space map for {} to {}", entry.getKey(), currentSpace);
          diskSpaceMap.put(entry.getKey(), currentSpace);
        }
      }

      strPatternIntMap.add(entry.getKey().toString() + "," + currentSpace);
    }

    PatternIntMap spaceMap = new PatternIntMap(strPatternIntMap);
    ds.setTestingDiskSpaceMap(spaceMap);
  }

  private static Artifact createArtifactInAU(TestingLocalWarcArtifactDataStore ds,
                                             String namespace, String auid, long contentLength) throws Exception {
    String artifactUuid = UUID.randomUUID().toString();
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(artifactUuid)
        .setNamespace(namespace)
        .setAuid(auid)
        .setUrl("http://example.com/" + artifactUuid)
        .setVersion(1)
        .setContentLength(contentLength);

    spec.generateContent();

    ArtifactData ad = spec.getArtifactData();
    Artifact artifact = ds.addArtifactData(ad);
    Future<Artifact> future = ds.commitArtifactData(artifact);
    Artifact committedArtifact = future.get();

    return committedArtifact;
  }

  // *******************************************************************************************************************
  // * TEST: AbstractWarcArtifactDataStoreTest IMPLEMENTATION
  // *******************************************************************************************************************

  @Override
  public void testMakeStorageUrlImpl() throws Exception {
    ArtifactIdentifier aid = new ArtifactIdentifier(NS1, AUID1, "http://example.com/u1", 1);
    long pendingArtifactSize = 1234L;

    Path activeWarcPath =
        store.getAppendablePermanentWarcInAU(aid.getNamespace(), aid.getAuid(), false, pendingArtifactSize);

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
   * Test for {@link LocalWarcArtifactDataStore#initAuDir(Path, String, String)}.
   */
  @Override
  public void testInitAuDirImpl() throws Exception {
    // Mocks
    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    Path basePath = mock(Path.class);
    Path auPath = mock(Path.class);
    File auPathFile = mock(File.class);

    // Mock behavior
    doCallRealMethod().when(ds).initAuDir(eq(basePath), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
    when(ds.generateAUPath(basePath, NS1, AUID1)).thenReturn(auPath);
    when(auPath.toFile()).thenReturn(auPathFile);

    // Assert mkdirs is called iff the AU path does not exist and is not a directory
    when(auPathFile.exists()).thenReturn(false);
    when(auPathFile.isDirectory()).thenReturn(false);
    assertEquals(auPath, ds.initAuDir(basePath, NS1, AUID1));
    verify(ds).mkdirs(auPath);
    clearInvocations(ds);

    // Assert mkdirs is *not* called otherwise
    when(auPathFile.exists()).thenReturn(true);
    when(auPathFile.isDirectory()).thenReturn(false);
    assertEquals(auPath, ds.initAuDir(basePath, NS1, AUID1));
    verify(ds, never()).mkdirs(auPath);
    clearInvocations(ds);

    when(auPathFile.isDirectory()).thenReturn(true);
    assertEquals(auPath, ds.initAuDir(basePath, NS1, AUID1));
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
    String ns = "test-namespace";
    String auid = "test-auid ";

    LocalWarcArtifactDataStore ds = mock(LocalWarcArtifactDataStore.class);
    List<Path> auPaths = mock(List.class);

    doCallRealMethod().when(ds).initAu(ns, auid);
    when(ds.findExistingAUPaths(eq(ns), eq(auid))).thenReturn(auPaths);

    List<Path> result = ds.initAu(ns, auid);

    assertSame(auPaths, result);
    verify(ds).initNamespace(eq(ns));
    verify(ds).findExistingAUPaths(eq(ns), eq(auid));
    clearInvocations(ds);
  }

  /**
   * Test error handling in readV0StateFiles()
   */
  @Test
  public void testCreateWarcLocalJournalsForAUErrorHandling() throws Exception {
    String artId1 = "014f025c-3a3f-40d4-856f-911390590a31";
    String artId2 = "3a24e0db-02fa-4ec5-9e68-a084ea4a4e5e";
    String artId3 = "44f1fbc5-779f-440d-b7f7-963931369f84";
    String artId4 = "a6069496-a6ee-4d5d-8552-92e2612d092a";
    String artId5 = "742178bf-8532-47cc-80c3-2c7571772587";

    Path auDir1 = getTempDir().toPath();
    Path auDir2 = getTempDir().toPath();

    // The third WARC record (2nd artifact) in this file is corrupted
    Path artifactsWarc = auDir1.resolve("artifacts.warc");
    Path stateFile1 = auDir1.resolve("artifact_state.warc");
    Path stateFile2 = auDir2.resolve("artifact_state.warc");

    IOUtils.copy(getResource("corrupt-v0-journal.warc"), stateFile1.toFile());

    List<Path> auJournalFiles = new ArrayList<>();

    Map<String, WarcArtifactStateEntry> auJournal = store.readV0StateFiles(ListUtil.list(auDir1), auJournalFiles);

    log.debug2("auJournal: {}", auJournal);
    assertEquals(5, auJournal.size(), "auJournal error: " + auJournal);

    WarcArtifactStateEntry wase = auJournal.get(artId1);
    assertEquals(artId1, wase.getArtifactUuid());
    assertEquals(WarcArtifactState.COPIED, wase.getEntry());
    assertEquals(1703200751226L, wase.getEntryDate());

    wase = auJournal.get(artId2);
    assertEquals(artId2, wase.getArtifactUuid());
    assertEquals(WarcArtifactState.UNKNOWN, wase.getEntry());
    assertEquals(1703229571337L, wase.getEntryDate());

    wase = auJournal.get(artId3);
    assertEquals(artId3, wase.getArtifactUuid());
    assertEquals(WarcArtifactState.DELETED, wase.getEntry());
    assertEquals(1703201133195L, wase.getEntryDate());

    wase = auJournal.get(artId4);
    assertEquals(artId4, wase.getArtifactUuid());
    assertEquals(WarcArtifactState.DELETED, wase.getEntry());
    assertEquals(1703201142078L, wase.getEntryDate());

    wase = auJournal.get(artId5);
    assertEquals(artId5, wase.getArtifactUuid());
    assertEquals(WarcArtifactState.COPIED, wase.getEntry());
    assertEquals(1703201252235L, wase.getEntryDate());
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

  // *******************************************************************************************************************
  // * TRUNCATION TESTS
  // *******************************************************************************************************************

  @Test
  public void testTruncateWarc_truncatesFileToSpecifiedLength() throws Exception {
    // Create a temp file with random content
    File warcFile = new File(getTempDir(), "test.warc");
    byte[] data = new byte[1000];
    new Random().nextBytes(data);
    FileUtils.writeByteArrayToFile(warcFile, data);
    assertEquals(1000, warcFile.length());

    // Truncate to 500 bytes
    store.truncateWarc(warcFile.toPath(), 500);

    // Verify file length and content matches the first 500 bytes
    assertEquals(500, warcFile.length());
    byte[] remaining = FileUtils.readFileToByteArray(warcFile);
    assertArrayEquals(Arrays.copyOf(data, 500), remaining);
  }

  @Test
  public void testTruncateWarc_truncateToZero() throws Exception {
    // Create a temp file with known content
    File warcFile = new File(getTempDir(), "test.warc");
    byte[] data = new byte[500];
    Arrays.fill(data, (byte) 'B');
    FileUtils.writeByteArrayToFile(warcFile, data);
    assertEquals(500, warcFile.length());

    // Truncate to zero
    store.truncateWarc(warcFile.toPath(), 0);

    // Verify file is empty
    assertEquals(0, warcFile.length());
    byte[] remaining = FileUtils.readFileToByteArray(warcFile);
    assertEquals(0, remaining.length);
  }

  @Test
  public void testTruncateWarc_truncateToCurrentLength() throws Exception {
    // Create a temp file with random content
    File warcFile = new File(getTempDir(), "test.warc");
    byte[] data = new byte[750];
    new Random().nextBytes(data);
    FileUtils.writeByteArrayToFile(warcFile, data);
    assertEquals(750, warcFile.length());

    // Truncate to current length (no-op)
    store.truncateWarc(warcFile.toPath(), 750);

    // Verify file is unchanged
    assertEquals(750, warcFile.length());
    byte[] remaining = FileUtils.readFileToByteArray(warcFile);
    assertArrayEquals(data, remaining);
  }

  @Test
  public void testTruncateWarc_nonExistentFile() throws Exception {
    // Attempt to truncate a file that does not exist
    File nonExistent = new File(getTempDir(), "nonexistent.warc");
    assertFalse(nonExistent.exists());

    assertThrows(NoSuchFileException.class,
        () -> store.truncateWarc(nonExistent.toPath(), 100));
  }
}
