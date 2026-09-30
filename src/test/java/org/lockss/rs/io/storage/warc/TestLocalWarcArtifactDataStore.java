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
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
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
import org.lockss.util.rest.repo.LockssNoSuchArtifactIdException;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.NamespacedAuid;
import org.lockss.util.concurrent.stripedexecutor.StripedRunnable;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.mockito.ArgumentMatchers;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    File reindexState =
        stateDir.toPath().resolve(BaseLockssRepository.REINDEXING_STATE_FILE).toFile();

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

  @Test
  public void testTruncateWarc_truncatePastFileLength() throws Exception {
    // Create a temp file with random content
    File warcFile = new File(getTempDir(), "test.warc");
    byte[] data = new byte[500];
    new Random().nextBytes(data);
    FileUtils.writeByteArrayToFile(warcFile, data);
    assertEquals(500, warcFile.length());

    // Truncate past file length should throw IOException
    assertThrows(IOException.class,
        () -> store.truncateWarc(warcFile.toPath(), 1000));

    // Verify file is unchanged
    assertEquals(500, warcFile.length());
    byte[] remaining = FileUtils.readFileToByteArray(warcFile);
    assertArrayEquals(data, remaining);
  }

  @Test
  public void testTruncateWarc_negativeLength() throws Exception {
    // Create a temp file with known content
    File warcFile = new File(getTempDir(), "test.warc");
    byte[] data = new byte[100];
    Arrays.fill(data, (byte) 'A');
    FileUtils.writeByteArrayToFile(warcFile, data);
    assertEquals(100, warcFile.length());

    // Negative length should throw IllegalArgumentException
    assertThrows(IllegalArgumentException.class,
        () -> store.truncateWarc(warcFile.toPath(), -1));
  }

  // *******************************************************************************************************************
  // * ASYNCHRONOUS TEMPORARY WARC RELOAD
  // *******************************************************************************************************************

  /**
   * A data store whose background temporary WARC reload can be held open, to model the
   * window between {@link WarcArtifactDataStore#start()} returning and the reload
   * finishing. Reloading a temporary WARC whose journal is missing means reading the whole
   * WARC body, which takes minutes on a multi-gigabyte artifact; the latch stands in for
   * that read.
   */
  private static class LatchedReloadStore extends LocalWarcArtifactDataStore {
    /**
     * Counted down once per base path when its background reload starts. Sized to the
     * number of base paths so a test can confirm every base path's reload is running
     * concurrently (all reach the gate while it is still closed) rather than one after
     * another (which would never reach this count within the gate's timeout, since the
     * gate blocks the first one from finishing and letting the next one start).
     */
    final CountDownLatch reloadStarted;

    /** Blocks the background reload until the test opens it. */
    final CountDownLatch reloadGate = new CountDownLatch(1);

    /** Times the reload found an artifact in PENDING_COPY and considered requeuing its copy. */
    final AtomicInteger requeueDecisions = new AtomicInteger();

    /** Times the reload actually queued a copy task. */
    final AtomicInteger requeuedCopies = new AtomicInteger();

    LatchedReloadStore(Path[] basePaths) throws IOException {
      super(basePaths);
      reloadStarted = new CountDownLatch(basePaths.length);
    }

    @Override
    protected boolean requeueCopy(Artifact artifact) {
      requeueDecisions.incrementAndGet();
      boolean queued = super.requeueCopy(artifact);

      if (queued) {
        requeuedCopies.incrementAndGet();
      }

      return queued;
    }

    @Override
    protected void reloadTemporaryWarcs(ArtifactIndex index, Path tmpWarcBasePath, List<Path> tmpWarcs) {
      reloadStarted.countDown();

      try {
        if (!reloadGate.await(60, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Test did not open the reload gate");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      super.reloadTemporaryWarcs(index, tmpWarcBasePath, tmpWarcs);
    }
  }

  private ScheduledExecutorService reloadTestExecutor;

  /**
   * Builds a data store over the base paths of {@code other} whose background temporary
   * WARC reload is held until the test opens its gate.
   */
  private LatchedReloadStore makeLatchedReloadStore(ArtifactIndex index, LocalWarcArtifactDataStore other)
      throws IOException {
    return makeLatchedReloadStore(index, other.getBasePaths());
  }

  /**
   * Builds a data store over {@code basePaths} whose background temporary WARC reload is
   * held until the test opens its gate.
   */
  private LatchedReloadStore makeLatchedReloadStore(ArtifactIndex index, Path[] basePaths)
      throws IOException {

    LatchedReloadStore ds = new LatchedReloadStore(basePaths);

    if (reloadTestExecutor == null) {
      reloadTestExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    when(repo.getScheduledExecutorService()).thenReturn(reloadTestExecutor);
    ds.setLockssRepository(repo);

    return ds;
  }

  /**
   * A data store whose background reload can be held open at the seam just before {@link
   * WarcArtifactDataStore#finishTemporaryWarcReload}: after a temporary WARC's artifacts
   * have all been classified, but before that classification is turned into pooled {@link
   * WarcFile} stats. Models the window in which a commit or a reload-requeued copy for one
   * of those artifacts can complete without being reflected in the counts about to be
   * pooled.
   */
  private static class LateReconciliationStore extends LocalWarcArtifactDataStore {
    /** Counted down when the reload reaches the seam, just before pooling. */
    final CountDownLatch classifiedGate = new CountDownLatch(1);

    /** Blocks the reload at the seam until the test releases it. */
    final CountDownLatch releaseGate = new CountDownLatch(1);

    LateReconciliationStore(Path[] basePaths) throws IOException {
      super(basePaths);
    }

    @Override
    protected void beforeFinishTemporaryWarcReload(Path tmpWarc) {
      classifiedGate.countDown();

      try {
        if (!releaseGate.await(60, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Test did not release the gate");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Builds a data store over the base paths of {@code other} whose background reload is
   * held at the seam just before it pools a temporary WARC's classification.
   */
  private LateReconciliationStore makeLateReconciliationStore(ArtifactIndex index, LocalWarcArtifactDataStore other)
      throws IOException {

    LateReconciliationStore ds = new LateReconciliationStore(other.getBasePaths());

    if (reloadTestExecutor == null) {
      reloadTestExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    when(repo.getScheduledExecutorService()).thenReturn(reloadTestExecutor);
    ds.setLockssRepository(repo);

    return ds;
  }

  /**
   * A {@link VolatileArtifactIndex} that returns an independent {@link Artifact#copyOf()} from
   * every {@link #getArtifact(String)} call, instead of the shared mutable instance the plain
   * volatile index hands back. Needed for {@link #testJournalReloadRefetchesArtifactAfterAcquiringLock}:
   * against the plain volatile index, a "stale" reference mutates in place along with the
   * live one, so the bug that test exists to catch cannot be observed at all. A real
   * {@link org.lockss.rs.io.index.db.SQLArtifactIndex} already has this snapshot property,
   * since it reconstructs a fresh {@link Artifact} from a database row on every call.
   */
  private static class SnapshotArtifactIndex extends VolatileArtifactIndex {
    @Override
    public Artifact getArtifact(String artifactUuid) {
      Artifact artifact = super.getArtifact(artifactUuid);
      return (artifact != null) ? artifact.copyOf() : null;
    }
  }

  /**
   * A data store whose journal-based temporary WARC reload commits a target artifact (via
   * the shared index, not through this store) at the exact seam between that artifact's
   * provisional lookup and the acquisition of its lock -- the window fixed by re-fetching
   * the artifact after the lock is held. See {@link WarcArtifactDataStore#beforeJournalReloadArtifactLock}.
   */
  private static class ArtifactRaceOnReloadStore extends LocalWarcArtifactDataStore {
    private final ArtifactIndex index;
    private final String targetUuid;
    private boolean fired = false;

    ArtifactRaceOnReloadStore(Path[] basePaths, ArtifactIndex index, String targetUuid)
        throws IOException {
      super(basePaths);
      this.index = index;
      this.targetUuid = targetUuid;
    }

    @Override
    protected void beforeJournalReloadArtifactLock(String uuid) {
      if (uuid.equals(targetUuid) && !fired) {
        fired = true;
        try {
          index.commitArtifact(targetUuid);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A data store whose {@link #getArtifactData(Artifact)} pools a target temporary WARC (by
   * directly adding it to {@code tmpWarcPool}, standing in for the background reload actually
   * finishing) at the exact seam between the first pool check and the acquisition of the
   * reload lock -- the window fixed by re-checking the pool after the lock is held. See
   * {@link WarcArtifactDataStore#beforeTempWarcReloadRecheck}.
   */
  private static class PoolsDuringRecheckStore extends LocalWarcArtifactDataStore {
    private final Path targetWarc;
    private boolean fired = false;

    PoolsDuringRecheckStore(Path[] basePaths, Path targetWarc) throws IOException {
      super(basePaths);
      this.targetWarc = targetWarc;
    }

    @Override
    protected void beforeTempWarcReloadRecheck(Path warcFilePath) {
      if (warcFilePath.equals(targetWarc) && !fired) {
        fired = true;
        try {
          WarcFile warcFile = new WarcFile(targetWarc, isCompressedWarcFile(targetWarc));
          warcFile.setLength(Files.size(targetWarc));
          tmpWarcPool.addAsFullWarcFile(warcFile);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Adds a single uncommitted artifact and returns its spec. The artifact's temporary WARC
   * is what a restarted data store has to reload.
   */
  private ArtifactSpec addUncommittedArtifact() throws Exception {
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1);
    spec.setArtifactUuid(UUID.randomUUID().toString());
    spec.generateContent();

    ArtifactData ad = spec.getArtifactData();
    ad.setStorageUrl(new URI("test://artifacts.warc"));

    Artifact storedRef = store.addArtifactData(ad);
    assertNotNull(storedRef);
    spec.setStorageUrl(URI.create(storedRef.getStorageUrl()));

    return spec;
  }

  /**
   * Regression test: the journal-based reload's
   * classification of an artifact must reflect a commit that lands between the artifact's
   * provisional lookup and the acquisition of its lock, not the stale pre-lock snapshot.
   * <p>
   * Forces isExpired = true (via a zero uncommitted-artifact expiration) so the stale,
   * still-uncommitted view of the artifact would be classified EXPIRED and deleted from the
   * index, while the fresh, now-committed view is classified PENDING_COPY and kept.
   */
  @Test
  public void testJournalReloadRefetchesArtifactAfterAcquiringLock() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new SnapshotArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    Artifact indexed = index.getArtifact(spec.getArtifactUuid());
    assertNotNull(indexed);
    assertFalse(indexed.getCommitted());

    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());
    assertTrue(isFile(tmpWarcPath));

    // Record a journal entry for this artifact so the journal-based reload's fast path --
    // the one under test -- applies instead of the full WARC-body scan fallback.
    store.writeJournalEntryForArtifact(indexed,
        new WarcArtifactStateEntry(spec.getArtifactUuid(), WarcArtifactState.UNCOMMITTED)
            .setEntryDate(TimeBase.nowMs()));

    ArtifactRaceOnReloadStore raceStore =
        new ArtifactRaceOnReloadStore(store.getBasePaths(), index, spec.getArtifactUuid());
    raceStore.setUncommittedArtifactExpiration(-1L);

    try {
      boolean handled = raceStore.reloadOrRemoveTemporaryWarcFromJournal(index, tmpWarcPath);
      assertTrue(handled, "Expected the journal-based reload path to handle this WARC");
      assertTrue(raceStore.fired, "Test hook never fired");

      // The commit landed between the lookup and the lock; the reload must have classified
      // the artifact against the fresh (committed) view, not the stale one.
      Artifact after = index.getArtifact(spec.getArtifactUuid());
      assertNotNull(after,
          "Reload deleted an artifact that was committed just before its lock was acquired");
      assertTrue(after.getCommitted());
    } finally {
      stopQuietly(raceStore);
    }
  }

  /**
   * Regression test: a read of an artifact in a temporary
   * WARC must recheck the WARC-file pool after acquiring the WARC's reload lock, not act on
   * the pre-lock pool lookup alone.
   * <p>
   * Simulates the background reload finishing (pooling the WARC) in the window between the
   * read's first pool check and its acquisition of the reload lock. Without the recheck, the
   * read would misreport this WARC -- which just became available -- as expired.
   */
  @Test
  public void testGetArtifactDataRechecksPoolAfterAcquiringReloadLock() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());
    assertTrue(isFile(tmpWarcPath));

    PoolsDuringRecheckStore raceStore =
        new PoolsDuringRecheckStore(store.getBasePaths(), tmpWarcPath);
    BaseLockssRepository repo = mock(BaseLockssRepository.class);
    when(repo.getArtifactIndex()).thenReturn(index);
    raceStore.setLockssRepository(repo);

    try {
      Artifact indexed = index.getArtifact(spec.getArtifactUuid());
      assertNotNull(indexed);

      // Not pooled yet, and this store never ran a reload (so nothing is marked pending
      // either) -- the hook below pools it partway through the read, simulating the
      // background reload finishing in the gap between the two checks.
      assertNull(raceStore.tmpWarcPool.getWarcFile(tmpWarcPath));

      try (ArtifactData ad = raceStore.getArtifactData(indexed)) {
        assertNotNull(ad);
        spec.assertArtifactData(ad);
      }

      assertTrue(raceStore.fired, "Test hook never fired");
      assertFalse(TempWarcInUseTracker.INSTANCE.isInUse(tmpWarcPath));
    } finally {
      stopQuietly(raceStore);
    }
  }

  private void stopQuietly(LocalWarcArtifactDataStore ds) {
    try {
      ds.stop();
    } catch (Exception e) {
      log.warn("Could not stop data store", e);
    }

    if (reloadTestExecutor != null) {
      reloadTestExecutor.shutdownNow();
      reloadTestExecutor = null;
    }
  }

  /**
   * {@link WarcArtifactDataStore#start()} must not wait for the temporary WARCs of a
   * previous run to be reloaded.
   */
  @Test
  public void testStartDoesNotBlockOnTemporaryWarcReload() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    addUncommittedArtifact();

    LatchedReloadStore reloadedStore = makeLatchedReloadStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();

      // start() returned. The reload is provably still running: it has entered
      // reloadTemporaryWarcs() and is sitting on the gate, which is still closed.
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS),
          "Background reload never started");
      assertFalse(reloadedStore.awaitReloadComplete(100, TimeUnit.MILLISECONDS),
          "start() waited for the temporary WARC reload to finish");

      // The data store is nonetheless started and serving
      assertEquals(WarcArtifactDataStore.DataStoreState.RUNNING, reloadedStore.getDataStoreState());
      assertTrue(reloadedStore.isReady());

      // Letting the reload finish leaves nothing pending
      reloadedStore.reloadGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS),
          "Background reload did not finish");
    } finally {
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * A read of an artifact whose temporary WARC the background reload has not reached is
   * served by referencing the file directly, and the reload afterwards still puts the WARC
   * in the pool with the right state.
   */
  @Test
  public void testGetArtifactDataFromTemporaryWarcPendingReload() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());
    assertTrue(isFile(tmpWarcPath));

    LatchedReloadStore reloadedStore = makeLatchedReloadStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS));

      // The WARC is awaiting reload, so it is not in the pool yet
      assertTrue(reloadedStore.isPendingReload(tmpWarcPath));
      assertNull(reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath));

      Artifact indexed = index.getArtifact(spec.getArtifactUuid());
      assertNotNull(indexed);

      // Read it anyway: this is the case that used to fail with "expired and was GCed"
      try (ArtifactData ad = reloadedStore.getArtifactData(indexed)) {
        assertNotNull(ad);
        spec.assertArtifactData(ad);
      }

      // The read must not have left the WARC marked in use
      assertFalse(TempWarcInUseTracker.INSTANCE.isInUse(tmpWarcPath));

      // Now let the reload finish and assert it still does its job
      reloadedStore.reloadGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

      assertFalse(reloadedStore.isPendingReload(tmpWarcPath));
      assertTrue(isFile(tmpWarcPath), "Reload removed a WARC holding an uncommitted artifact");

      WarcFile pooled = reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath);
      assertNotNull(pooled, "Reloaded temporary WARC not in pool");
      assertEquals(1, pooled.getStats().getArtifactsTotal());
      assertEquals(1, pooled.getStats().getArtifactsUncommitted());

      // ... and that the artifact is still readable, now through the pool
      try (ArtifactData ad = reloadedStore.getArtifactData(index.getArtifact(spec.getArtifactUuid()))) {
        assertNotNull(ad);
        spec.assertArtifactData(ad);
      }
    } finally {
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * The direct-read path is only for WARCs awaiting reload: once a temporary WARC has been
   * reloaded and removed, a read of an artifact that was in it still fails.
   */
  @Test
  public void testGetArtifactDataAfterReloadRemovedTemporaryWarc() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());
    Artifact indexed = index.getArtifact(spec.getArtifactUuid());
    assertNotNull(indexed);

    // Delete it, so every record in the temporary WARC is removable on reload
    store.deleteArtifactData(indexed);
    index.deleteArtifact(spec.getArtifactUuid());

    LatchedReloadStore reloadedStore = makeLatchedReloadStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS));

      reloadedStore.reloadGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

      assertFalse(reloadedStore.isPendingReload(tmpWarcPath));
      assertFalse(isFile(tmpWarcPath), "Reload did not remove a fully removable temporary WARC");

      // A stale reference to the deleted artifact must not be served by the direct-read path
      assertThrows(LockssNoSuchArtifactIdException.class,
          () -> reloadedStore.getArtifactData(indexed));
    } finally {
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * A temporary WARC being read directly because it is still awaiting reload must not be
   * deleted by the reload underneath the reader: the reload leaves it to the garbage
   * collector, which already defers to the in-use tracker.
   */
  @Test
  public void testReloadDefersRemovalOfInUseTemporaryWarc() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());
    Artifact indexed = index.getArtifact(spec.getArtifactUuid());

    // Delete it, so the reload would otherwise remove this temporary WARC
    store.deleteArtifactData(indexed);
    index.deleteArtifact(spec.getArtifactUuid());

    LatchedReloadStore reloadedStore = makeLatchedReloadStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS));

      // Stand in for a read of this WARC that is still in flight when the reload runs
      TempWarcInUseTracker.INSTANCE.markUseStart(tmpWarcPath);

      try {
        reloadedStore.reloadGate.countDown();
        assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

        assertTrue(isFile(tmpWarcPath), "Reload removed a temporary WARC that was in use");

        // It was handed to the pool instead, so the GC can reap it later
        assertNotNull(reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath),
            "In-use removable temporary WARC was neither removed nor pooled");

        // The GC defers to the in-use tracker in the same way
        reloadedStore.tmpWarcPool.runGC();
        assertTrue(isFile(tmpWarcPath), "GC removed a temporary WARC that was in use");
      } finally {
        TempWarcInUseTracker.INSTANCE.markUseEnd(tmpWarcPath);
      }

      // Once the read is done the GC reaps it
      reloadedStore.tmpWarcPool.runGC();
      assertFalse(isFile(tmpWarcPath), "GC did not reap a removable temporary WARC");
    } finally {
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * Committing an artifact whose temporary WARC has not been reloaded yet must work: there
   * is no {@link WarcFile} in the pool whose stats to update, which used to be reported as
   * "Too late to commit artifact". The copy must also happen exactly once, not again when
   * the reload later reaches the WARC.
   */
  @Test
  public void testCommitArtifactInTemporaryWarcPendingReload() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());

    LatchedReloadStore reloadedStore = makeLatchedReloadStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS));
      assertTrue(reloadedStore.isPendingReload(tmpWarcPath));

      Artifact indexed = index.getArtifact(spec.getArtifactUuid());
      assertNotNull(indexed);

      Future<Artifact> future = reloadedStore.commitArtifactData(indexed);
      assertNotNull(future, "Commit refused while the temporary WARC was pending reload");

      Artifact committed = future.get(30, TimeUnit.SECONDS);
      assertNotNull(committed);
      assertTrue(committed.getCommitted());

      spec.setCommitted(true);
      spec.setStorageUrl(URI.create(committed.getStorageUrl()));

      Path permWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(URI.create(committed.getStorageUrl()));
      assertFalse(reloadedStore.isTmpStorage(permWarcPath));
      long permWarcLength = permWarcPath.toFile().length();
      assertTrue(permWarcLength > 0);

      // Let the reload reach this WARC now that the artifact has been copied
      reloadedStore.reloadGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

      // The reload must not have copied the artifact into permanent storage a second time
      assertEquals(permWarcLength, permWarcPath.toFile().length(),
          "Reload copied an already-copied artifact again");

      try (ArtifactData ad = reloadedStore.getArtifactData(index.getArtifact(spec.getArtifactUuid()))) {
        assertNotNull(ad);
        spec.assertArtifactData(ad);
      }
    } finally {
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * Counts the WARC records of a given type in a WARC file.
   */
  private int countWarcRecordsOfType(LocalWarcArtifactDataStore ds, Path warcPath,
                                     WARCConstants.WARCRecordType type) throws IOException {
    int count = 0;

    try (InputStream is = new BufferedInputStream(ds.getInputStreamAndSeek(warcPath, 0))) {
      ArchiveReader reader = ds.getArchiveReader(warcPath, is);
      reader.setDigest(false);

      for (ArchiveRecord record : reader) {
        if (type.name().equals(record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE))) {
          count++;
        }
      }
    }

    return count;
  }

  /**
   * The background reload must not queue a second copy for an artifact whose copy a
   * concurrent commit has already queued: the artifact would be written into permanent
   * storage twice.
   * <p>
   * The interleaving is forced rather than raced for: a barrier task occupies the AU's
   * stripe so the commit's copy task is still queued -- and so the artifact is still
   * PENDING_COPY -- when the reload reaches its temporary WARC.
   */
  @Test
  public void testReloadDoesNotRequeueACopyAlreadyQueued() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();

    LatchedReloadStore reloadedStore = makeLatchedReloadStore(index, store);

    CountDownLatch stripeBarrier = new CountDownLatch(1);

    try {
      reloadedStore.init();
      reloadedStore.start();
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS));

      // Occupy the AU's stripe so the copy task queued by the commit below cannot run
      reloadedStore.stripedExecutor.submit(new StripedRunnable() {
        @Override
        public Object getStripe() {
          return new NamespacedAuid(NS1, AUID1);
        }

        @Override
        public void run() {
          try {
            stripeBarrier.await(60, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      });

      Artifact indexed = index.getArtifact(spec.getArtifactUuid());
      Future<Artifact> future = reloadedStore.commitArtifactData(indexed);
      assertNotNull(future);

      // The artifact is now PENDING_COPY with its copy queued but not started. Let the
      // reload reach its temporary WARC in exactly that state.
      reloadedStore.reloadGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

      // The reload did find the artifact pending copy -- i.e. the interleaving really was
      // forced -- and left the queued copy alone rather than queuing a second one.
      assertEquals(1, reloadedStore.requeueDecisions.get(),
          "Reload did not reach the PENDING_COPY artifact; the test did not force the interleaving");
      assertEquals(0, reloadedStore.requeuedCopies.get(),
          "Reload queued a second copy for an artifact whose copy was already queued");

      // Now let the copy run
      stripeBarrier.countDown();

      Artifact committed = future.get(60, TimeUnit.SECONDS);
      assertNotNull(committed);
      assertTrue(reloadedStore.waitForCommitTasks(NS1, AUID1));

      Path permWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(URI.create(committed.getStorageUrl()));
      assertFalse(reloadedStore.isTmpStorage(permWarcPath));

      assertEquals(1,
          countWarcRecordsOfType(reloadedStore, permWarcPath, WARCConstants.WARCRecordType.response),
          "Artifact was copied into permanent storage more than once");
    } finally {
      stripeBarrier.countDown();
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * If a live commit -- including the copy to permanent storage -- completes for an
   * artifact the reload has already classified as {@code UNCOMMITTED}, but before the
   * reload pools the temporary WARC's {@link WarcFile}, the pooled stats must reflect the
   * artifact's true final state rather than the stale classification. Without
   * reconciliation in {@code finishTemporaryWarcReload()}, such a WarcFile is stuck with
   * {@code committed != copied}, which {@link WarcFilePool#runGC()} -- which requires them
   * equal unconditionally -- never reclaims short of a full restart.
   */
  @Test
  public void testFinishTemporaryWarcReloadReconcilesLateCommit() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();
    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());

    LateReconciliationStore reloadedStore = makeLateReconciliationStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();

      // The reload has classified this artifact as UNCOMMITTED and is sitting at the seam
      // just before pooling.
      assertTrue(reloadedStore.classifiedGate.await(30, TimeUnit.SECONDS),
          "Reload never reached the point just before pooling");

      Artifact indexed = index.getArtifact(spec.getArtifactUuid());
      Future<Artifact> future = reloadedStore.commitArtifactData(indexed);
      assertNotNull(future, "Commit refused while the temporary WARC was pending reload");

      Artifact committed = future.get(30, TimeUnit.SECONDS);
      assertNotNull(committed);
      assertTrue(committed.getCommitted());

      Path permWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(URI.create(committed.getStorageUrl()));
      assertFalse(reloadedStore.isTmpStorage(permWarcPath),
          "Commit did not complete before the reload finished pooling");

      // Now let the reload finish pooling.
      reloadedStore.releaseGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

      WarcFile tmpWarcFile = reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath);
      assertNotNull(tmpWarcFile, "Temporary WARC was not pooled");
      assertEquals(0, tmpWarcFile.getStats().getArtifactsUncommitted());
      assertEquals(1, tmpWarcFile.getStats().getArtifactsCommitted());
      assertEquals(1, tmpWarcFile.getStats().getArtifactsCopied());

      // With committed == copied and uncommitted == 0, the GC must reclaim it.
      reloadedStore.tmpWarcPool.runGC();
      assertNull(reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath),
          "Temporary WARC was not reclaimed by the GC despite committed == copied");
    } finally {
      reloadedStore.releaseGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * The same reconciliation, forced via the ordinary crash-recovery path instead of a live
   * commit: the reload itself finds an artifact {@code PENDING_COPY} (as it would after a
   * crash between the commit's journal write and its copy), requeues the copy, and that
   * requeued copy completes before the rest of the WARC's classification finishes and it is
   * pooled. No live commit is needed to trigger this ordering -- it is the ordinary
   * requeue-races-the-scan case.
   */
  @Test
  public void testFinishTemporaryWarcReloadReconcilesLateRequeuedCopy() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    store = makeWarcArtifactDataStore(index);
    ArtifactSpec spec = addUncommittedArtifact();
    Path tmpWarcPath = WarcArtifactDataStore.getPathFromStorageUrl(spec.getStorageUrl());

    // Commit through the original store, but hold its copy task back on store's own
    // stripe: the shared index must still show PENDING_COPY (temporary storage URL) when
    // the "restarted" store below starts, exactly the state a real crash between the
    // journal write and the copy would leave behind.
    CountDownLatch originalCopyBarrier = new CountDownLatch(1);
    store.stripedExecutor.submit(new StripedRunnable() {
      @Override
      public Object getStripe() {
        return new NamespacedAuid(NS1, AUID1);
      }

      @Override
      public void run() {
        try {
          originalCopyBarrier.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });

    Artifact indexed = index.getArtifact(spec.getArtifactUuid());
    Future<Artifact> originalCommit = store.commitArtifactData(indexed);
    assertNotNull(originalCommit);

    LateReconciliationStore reloadedStore = makeLateReconciliationStore(index, store);

    try {
      reloadedStore.init();
      reloadedStore.start();

      // The reload classified this artifact as PENDING_COPY and requeued its copy (on
      // reloadedStore's own executor, unaffected by store's barrier above), and is sitting
      // at the seam just before pooling.
      assertTrue(reloadedStore.classifiedGate.await(30, TimeUnit.SECONDS),
          "Reload never reached the point just before pooling");

      // Let the requeued copy run to completion while pooling is still paused.
      assertTrue(reloadedStore.waitForCommitTasks(NS1, AUID1));

      Artifact recopied = index.getArtifact(spec.getArtifactUuid());
      assertFalse(reloadedStore.isTmpStorage(
              WarcArtifactDataStore.getPathFromStorageUrl(URI.create(recopied.getStorageUrl()))),
          "Requeued copy did not complete before the reload finished pooling");

      // Now let the reload finish pooling.
      reloadedStore.releaseGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS));

      WarcFile tmpWarcFile = reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath);
      assertNotNull(tmpWarcFile, "Temporary WARC was not pooled");
      assertEquals(0, tmpWarcFile.getStats().getArtifactsUncommitted());
      assertEquals(1, tmpWarcFile.getStats().getArtifactsCommitted());
      assertEquals(1, tmpWarcFile.getStats().getArtifactsCopied());

      reloadedStore.tmpWarcPool.runGC();
      assertNull(reloadedStore.tmpWarcPool.getWarcFile(tmpWarcPath),
          "Temporary WARC was not reclaimed by the GC despite committed == copied");
    } finally {
      originalCopyBarrier.countDown();
      reloadedStore.releaseGate.countDown();
      stopQuietly(reloadedStore);
    }
  }

  /**
   * {@link WarcArtifactDataStore#reloadDataStoreState()} fans out one background reload per
   * temporary WARC base path. The original work's test harness only ever configured a
   * single base path, so that fan-out itself -- base paths reloaded concurrently, not one
   * after another -- went unverified.
   * <p>
   * Two independent single-base-path stores write one artifact each, so each artifact's
   * temporary WARC lands in a base path known deterministically -- this does not depend on
   * how a multi-base-path store would itself choose among them, which is a separate
   * concern. A restart is then simulated over both base paths at once.
   */
  @Test
  public void testReloadDataStoreStateProcessesBasePathsConcurrently() throws Exception {
    teardownDataStore();

    ArtifactIndex index = new VolatileArtifactIndex();
    index.init();

    File baseDir1 = getTempDir();
    File baseDir2 = getTempDir();

    LocalWarcArtifactDataStore writer1 = new LocalWarcArtifactDataStore(new File[]{baseDir1});
    BaseLockssRepository repo1 = mock(BaseLockssRepository.class);
    when(repo1.getArtifactIndex()).thenReturn(index);
    writer1.setLockssRepository(repo1);

    LocalWarcArtifactDataStore writer2 = new LocalWarcArtifactDataStore(new File[]{baseDir2});
    BaseLockssRepository repo2 = mock(BaseLockssRepository.class);
    when(repo2.getArtifactIndex()).thenReturn(index);
    writer2.setLockssRepository(repo2);

    ArtifactSpec spec1 = ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1);
    spec1.setArtifactUuid(UUID.randomUUID().toString());
    spec1.generateContent();
    ArtifactData ad1 = spec1.getArtifactData();
    ad1.setStorageUrl(new URI("test://artifacts.warc"));
    Artifact stored1 = writer1.addArtifactData(ad1);
    assertNotNull(stored1);
    Path tmpWarc1 = WarcArtifactDataStore.getPathFromStorageUrl(URI.create(stored1.getStorageUrl()));

    ArtifactSpec spec2 = ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2);
    spec2.setArtifactUuid(UUID.randomUUID().toString());
    spec2.generateContent();
    ArtifactData ad2 = spec2.getArtifactData();
    ad2.setStorageUrl(new URI("test://artifacts.warc"));
    Artifact stored2 = writer2.addArtifactData(ad2);
    assertNotNull(stored2);
    Path tmpWarc2 = WarcArtifactDataStore.getPathFromStorageUrl(URI.create(stored2.getStorageUrl()));

    // Simulate a restart that sees both base paths at once.
    LatchedReloadStore reloadedStore =
        makeLatchedReloadStore(index, new Path[]{baseDir1.toPath(), baseDir2.toPath()});

    try {
      reloadedStore.init();
      reloadedStore.start();

      // Both base paths' reload tasks must reach the gate: if they ran one after another
      // instead of concurrently, the second would never start while the first sits on the
      // still-closed gate, and this would time out rather than count down twice.
      assertTrue(reloadedStore.reloadStarted.await(30, TimeUnit.SECONDS),
          "Both base paths' background reloads never started concurrently");
      assertFalse(reloadedStore.awaitReloadComplete(100, TimeUnit.MILLISECONDS),
          "start() waited for the temporary WARC reload to finish");

      reloadedStore.reloadGate.countDown();
      assertTrue(reloadedStore.awaitReloadComplete(60, TimeUnit.SECONDS),
          "Background reload did not finish");

      assertFalse(reloadedStore.isPendingReload(tmpWarc1));
      assertFalse(reloadedStore.isPendingReload(tmpWarc2));
    } finally {
      reloadedStore.reloadGate.countDown();
      stopQuietly(reloadedStore);
    }
  }
}
