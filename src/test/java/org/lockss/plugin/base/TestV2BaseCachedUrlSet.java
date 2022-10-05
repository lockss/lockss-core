/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.plugin.base;

import java.io.*;
import java.util.*;

import org.lockss.app.*;
import org.lockss.config.ConfigManager;
import org.lockss.daemon.*;
import org.lockss.hasher.HashService;
import org.lockss.plugin.*;
import org.lockss.repository.*;
import org.lockss.scheduler.SchedService;
import org.lockss.state.*;
import org.lockss.test.*;
import org.lockss.util.*;

import org.apache.http.*;
import org.apache.http.message.*;
import org.springframework.http.HttpHeaders;
import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;

public class TestV2BaseCachedUrlSet extends LockssTestCase {
  static Logger log = Logger.getLogger();

  private HashService hashService;
  private MockArchivalUnit mau;
  private MockLockssDaemon theDaemon;
  private MockPlugin plugin;
  private SystemMetrics metrics;

  protected LockssRepository v2Repo;
  protected String v2Coll;

  static final int HASH_SPEED = 100;

  public void setUp() throws Exception {
    super.setUp();
    String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    Properties props = new Properties();
    props.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
		      tempDirPath);
    props.setProperty(SystemMetrics.PARAM_DEFAULT_HASH_SPEED,
		      Integer.toString(HASH_SPEED));
    ConfigurationUtil.setCurrentConfigFromProps(props);

    theDaemon = getMockLockssDaemon();
    hashService = theDaemon.getHashService();
    hashService.startService();
    metrics = theDaemon.getSystemMetrics();
    metrics.startService();

    mau = new MyMockArchivalUnit();
    plugin = new MockPlugin();
    plugin.initPlugin(theDaemon);
    mau.setPlugin(plugin);


    useV2Repo();
    RepositoryManager repomgr =
      LockssDaemon.getLockssDaemon().getRepositoryManager();
    v2Repo = repomgr.getV2Repository().getRepository();
    v2Coll = repomgr.getV2Repository().getCollection();

    // don't require all tests to set up mau crawl rules
    ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_INCLUDED_ONLY, "false");
  }

  public void tearDown() throws Exception {
    hashService.stopService();
    theDaemon.stopDaemon();
    super.tearDown();
  }

  public void testExcludeByDate() throws Exception {
    String base = "http://www.example.com/testDir";
    createLeaf(base + "/leaf1", null, null);
    createLeaf(base + "/leaf2", null,
	       CIProperties.fromArgs(CachedUrl.PROPERTY_FETCH_TIME, "55555"));
    createLeaf(base + "/leaf3", null,
	       CIProperties.fromArgs(CachedUrl.PROPERTY_FETCH_TIME, "33333"));
    createLeaf(base + "/leaf4", null,
	       CIProperties.fromArgs(CachedUrl.PROPERTY_FETCH_TIME, "bogus"));

    CachedUrlSetSpec rSpec = new RangeCachedUrlSetSpec(base);
    BaseCachedUrlSet cus = (BaseCachedUrlSet)mau.makeCachedUrlSet(rSpec);
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf1")));
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf2")));
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf3")));
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf4")));
    cus.setExcludeFilesUnchangedAfter(44444);
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf1")));
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf2")));
    assertTrue(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf3")));
    assertFalse(cus.isExcludedByDate(mau.makeCachedUrl(base + "/leaf4")));
  }


  // XXXREPO
  public void testFlatSetIterator() throws Exception {
    createLeaf("http://www.example.com/testDir/leaf4", null, null);
    createLeaf("http://www.example.com/testDir/branch1/leaf1", null, null);
    createLeaf("http://www.example.com/testDir/branch2/leaf3", null, null);
    createLeaf("http://www.example.com/testDir/branch1/leaf2", null, null);

    CachedUrlSet top = mau.getAuCachedUrlSet();
    assertIsomorphic(ListUtil.list("http://www.example.com/"),
		     PluginTestUtil.urlsOf(top.flatSetIterator()));

    CachedUrlSetSpec rSpec =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir/");

    CachedUrlSet set1 = mau.makeCachedUrlSet(rSpec);
    assertIsomorphic(ListUtil.list("http://www.example.com/testDir/branch1/",
				   "http://www.example.com/testDir/branch2/",
				   "http://www.example.com/testDir/leaf4"),
		     PluginTestUtil.urlsOf(set1.flatSetIterator()));

  }

  public void testHashIterator() throws Exception {
    String[] urls = {
      "http://www.example.com/testDir/branch1/leaf1",
      "http://www.example.com/testDir/branch1/leaf2",
      "http://www.example.com/testDir/branch2/leaf3",
      "http://www.example.com/testDir/leaf4",
      "http://www.example.com/testDir/\u00C1rv\u00EDzt\u0171r\u0151 t\u00FCk\u00F6rf\u00FAr\u00F3g\u00E9p", // Árvíztűrő tükörfúrógép (Hungarian)
      "http://www.example.com/testDir/\u03BA\u1F79\u03C3\u03BC\u03B5", // κόσμε
    };

    for (String s : urls) {
      createLeaf(s, "test stream: " + s, null);
    }

    CachedUrlSetSpec rSpec =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(rSpec);
    assertIsomorphic(urls, PluginTestUtil.urlsOf(fileSet.contentHashIterator()));

    CachedUrlSet cus = mau.getAuCachedUrlSet();
    assertIsomorphic(urls, PluginTestUtil.urlsOf(cus.contentHashIterator()));

    rSpec = new RangeCachedUrlSetSpec("http://www.example.com/testDir/branch1");
    cus = mau.makeCachedUrlSet(rSpec);
    assertIsomorphic(ListUtil.list(urls[0], urls[1]),
		     PluginTestUtil.urlsOf(cus.contentHashIterator()));

    rSpec = new RangeCachedUrlSetSpec("http://www.example.com/testDir/branch1/");
    cus = mau.makeCachedUrlSet(rSpec);
    assertIsomorphic(ListUtil.list(urls[0], urls[1]),
		     PluginTestUtil.urlsOf(cus.contentHashIterator()));

    // Prefix must be a whole path component
    rSpec = new RangeCachedUrlSetSpec("http://www.example.com/testDir/bran");
    cus = mau.makeCachedUrlSet(rSpec);
    assertEmpty(PluginTestUtil.urlsOf(cus.contentHashIterator()));


    // test getCuIterator and getCuIterable
    assertIsomorphic(urls,
                     PluginTestUtil.urlsOf(ListUtil.fromIterator(fileSet.getCuIterator())));
    assertEquals(PluginTestUtil.urlsOf(ListUtil.fromIterator(fileSet.getCuIterator())),
		 PluginTestUtil.urlsOf(ListUtil.fromIterable(fileSet.getCuIterable())));


  }

  public void testHashIteratorPruned() throws Exception {
    createLeaf("http://www.example.com/testDir/branch1/leaf2",
               "test stream", null);
    createLeaf("http://www.example.com/testDir/leaf4", "test stream", null);
    createLeaf("http://www.example.com/testDir/branch2/leaf3",
               "test stream", null);
    createLeaf("http://www.example.com/testDir/branch1/leaf1",
               "test stream", null);

    CachedUrlSetSpec rSpec =
      PrunedCachedUrlSetSpec.includeMatchingSubTrees("http://www.example.com/testDir",
						     "http://www.example.com/testDir/branch1");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(rSpec);
    Iterator setIt = fileSet.contentHashIterator();
    ArrayList childL = new ArrayList(7);
    while (setIt.hasNext()) {
      childL.add(((CachedUrlSetNode)setIt.next()).getUrl());
    }
    // should be sorted
    String[] expectedA = new String[] {
//       "http://www.example.com/testDir",
//       "http://www.example.com/testDir/branch1",
      "http://www.example.com/testDir/branch1/leaf1",
      "http://www.example.com/testDir/branch1/leaf2",
      };
    assertIsomorphic(expectedA, childL);
  }

  public void testHashIteratorSingleNode() throws Exception {
    createLeaf("http://www.example.com/testDir/branch1/leaf2",
               "test stream", null);
    createLeaf("http://www.example.com/testDir/leaf4", "test stream", null);
    createLeaf("http://www.example.com/testDir/branch2/leaf3",
               "test stream", null);
    createLeaf("http://www.example.com/testDir/branch1/leaf1",
               "test stream", null);

    CachedUrlSetSpec spec =
      new SingleNodeCachedUrlSetSpec("http://www.example.com/testDir/branch2/leaf3");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(spec);
    Iterator setIt = fileSet.contentHashIterator();
    ArrayList childL = new ArrayList(1);
    while (setIt.hasNext()) {
      childL.add(((CachedUrlSetNode)setIt.next()).getUrl());
    }

    String[] expectedA = new String[] {
      "http://www.example.com/testDir/branch2/leaf3",
    };
    assertIsomorphic(expectedA, childL);
  }

  String expMultSchemeHostPort[] = {
    "http://aa/leaf",
    "http://aa.com/leaf",
    "http://aa.com:7000/leaf",
    "http://aa.com:8000/leaf",
    "http://aa.comx/leaf",
    "http://aa:7000/leaf",
    "http://aa:8000/leaf",
    "http://bb/leaf",
    "http://bb:7000/leaf",
    "http://bb:8000/leaf",
    "https://aa/leaf",
    "https://aa:7000/leaf",
    "https://aa:8000/leaf",
    "https://bb/leaf",
    "https://bb:7000/leaf",
    "https://bb:8000/leaf",
    // Some unicode names
    "\u00C1rv\u00EDzt\u0171r\u0151 t\u00FCk\u00F6rf\u00FAr\u00F3g\u00E9p", // Árvíztűrő tükörfúrógép (Hungarian)
    "\u03BA\u1F79\u03C3\u03BC\u03B5", // κόσμε
  };

  public void testHashIteratorMultipleSchemeHostPort() throws Exception {
    Set preOrderHasContent = new TreeSet(PreOrderComparator.INSTANCE);
    for (String s : expMultSchemeHostPort) {
      createLeaf(s, "test stream", null);
      preOrderHasContent.add(s);
    }

    CachedUrlSet fileSet = mau.getAuCachedUrlSet();
    Iterator setIt = fileSet.contentHashIterator();
    ArrayList nodes = new ArrayList();
    ArrayList contentNodes = new ArrayList();

    while (setIt.hasNext()) {
      CachedUrlSetNode node = ((CachedUrlSetNode)setIt.next());
      String url = node.getUrl();
      nodes.add(url);
      if (node.hasContent()) {
	contentNodes.add(url);
      }
    }
    // should be sorted
    assertIsomorphic(expMultSchemeHostPort, nodes);
    assertIsomorphic(preOrderHasContent, contentNodes);

  }

  // TK any reason to duplicate this behavior?
  public void notestHashIteratorThrowsOnBogusUrl() throws Exception {
    createLeaf("http://www.example.com/testDir/branch1/leaf2",
               "test stream", null);

    CachedUrlSetSpec spec =
      new RangeCachedUrlSetSpec("bad_protocol://www.example.com/testDir");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(spec);
    try {
      Iterator setIt = fileSet.contentHashIterator();
      fail("Bogus url should have caused a RuntimeException");
    } catch (RuntimeException e){
    }
  }

  void doNonExistentNode(CachedUrlSetSpec spec, boolean isRanged)
      throws Exception {
    CachedUrlSet cus = mau.makeCachedUrlSet(spec);

    Iterator flatIter = cus.flatSetIterator();
    assertFalse(flatIter.hasNext());

    Iterator hashIter = cus.contentHashIterator();
    if (!isRanged) {
      assertTrue(hashIter.hasNext());
      CachedUrlSet first = (CachedUrlSet)hashIter.next();
      assertEquals(cus, first);
    }
    assertFalse(hashIter.hasNext());

    assertFalse(cus.isLeaf());
    assertFalse(cus.hasContent());
    // Estimate won't be 0 because of padding.  Just ensure it doesn't throw
    cus.estimatedHashDuration();
  }

  public void testHashIteratorVariations() throws Exception {
    createLeaf("http://www.example.com/testDir/branch1/leaf2",
               "test stream", null);
    createLeaf("http://www.example.com/testDir/branch1/leaf1",
               "test stream", null);

    CachedUrlSetSpec rSpec =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir/branch1",
                                  "/leaf1", "/leaf2");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(rSpec);
    Iterator setIt = fileSet.contentHashIterator();
    ArrayList childL = new ArrayList(2);
    while (setIt.hasNext()) {
      childL.add(((CachedUrlSetNode)setIt.next()).getUrl());
    }
    // should exclude 'branch1'
    String[] expectedA = new String[] {
      "http://www.example.com/testDir/branch1/leaf1",
      "http://www.example.com/testDir/branch1/leaf2"
    };
    assertIsomorphic(expectedA, childL);

    CachedUrlSetSpec snSpec =
        new SingleNodeCachedUrlSetSpec("http://www.example.com/testDir/branch1");
    fileSet = mau.makeCachedUrlSet(snSpec);
    setIt = fileSet.contentHashIterator();
    childL = new ArrayList(1);
    while (setIt.hasNext()) {
      childL.add(((CachedUrlSetNode)setIt.next()).getUrl());
    }
    assertEmpty(childL);
  }

  public void testNodeCounting() throws Exception {
    createLeaf("http://www.example.com/testDir/branch1/leaf1",
               "test streamAA", null);
    createLeaf("http://www.example.com/testDir/branch1/leaf2",
               "test stream", null);
    createLeaf("http://www.example.com/testDir/branch2/leaf3",
               "test streamB", null);
    createLeaf("http://www.example.com/testDir/leaf4", "first ver", null);
    createLeaf("http://www.example.com/testDir/leaf4", "test streamC", null);

    CachedUrlSetSpec rSpec =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir");
    BaseCachedUrlSet fileSet =
        (BaseCachedUrlSet)mau.makeCachedUrlSet(rSpec);
    assertEquals(48, fileSet.getContentSize());
    assertEquals(48, AuUtil.getAuContentSize(mau, true));
    assertEquals(48, mau.getAuCachedUrlSet().getContentSize());
    assertEquals(57, mau.getAuCachedUrlSet().getContentSizeAllVersions());
    assertEquals(57, AuUtil.getAuContentSizeAllVersions(mau));

    CachedUrlSetSpec spec2 =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir/branch1");
    CachedUrlSet cus2 = mau.makeCachedUrlSet(spec2);
    assertEquals(24, cus2.getContentSize());
  }

  public void testHashEstimation() throws Exception {
    byte[] bytes = new byte[100];
    Arrays.fill(bytes, (byte)1);
    String testString = new String(bytes);
    for (int ii=0; ii<10; ii++) {
      createLeaf("http://www.example.com/testDir/leaf"+ii, testString, null);
    }
    CachedUrlSetSpec rSpec =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir");
    CachedUrlSet cus = mau.makeCachedUrlSet(rSpec);
    AuState node = AuUtil.getAuState(mau);
    long estimate = cus.estimatedHashDuration();
    assertTrue(estimate > 0);

    assertEquals(hashService.padHashEstimate(node.getAverageHashDuration()),
		 estimate);

    // test return of stored duration
    long estimate2 = cus.estimatedHashDuration();
    assertEquals(estimate, estimate2);

    // test averaging of durations
    long lastActual = node.getAverageHashDuration();
    cus.storeActualHashDuration(lastActual + 200, null);
    assertEquals(hashService.padHashEstimate(lastActual + 100),
        cus.estimatedHashDuration());

    // test special SetEstimate marker exception
    lastActual = node.getAverageHashDuration();
    cus.storeActualHashDuration(lastActual + 12345,
				    new HashService.SetEstimate());
    assertEquals(hashService.padHashEstimate(lastActual + 12345),
        cus.estimatedHashDuration());
  }

  public void testSingleNodeHashEstimation() throws Exception {
    byte[] bytes = new byte[1000];
    Arrays.fill(bytes, (byte)1);
    String testString = new String(bytes);
    // check that estimation is special for single nodes, and isn't stored
    createLeaf("http://www.example.com/testDir", testString, null);
    CachedUrlSetSpec sSpec =
        new SingleNodeCachedUrlSetSpec("http://www.example.com/testDir");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(sSpec);
    long estimate = fileSet.estimatedHashDuration();
    log.critical("estimate: " + estimate);
    // SystemMetrics set speed to avoid slow machine problems
    assertTrue(estimate > 0);
    long expectedEstimate = 1000 / HASH_SPEED;
    log.critical("expectedEstimate: " + expectedEstimate);
    log.critical("pad: " + hashService.padHashEstimate(expectedEstimate));

    assertEquals(estimate, hashService.padHashEstimate(expectedEstimate));
    // check that estimation isn't stored for single node sets
    assertEquals(-1, AuUtil.getAuState(mau).getAverageHashDuration());
  }

  public void testIrregularHashStorage() throws Exception {
    // check that estimation isn't changed for single node sets
    createLeaf("http://www.example.com/testDir", "sdflkajsdfas", null);
    CachedUrlSetSpec sSpec =
        new SingleNodeCachedUrlSetSpec("http://www.example.com/testDir");
    CachedUrlSet fileSet = mau.makeCachedUrlSet(sSpec);
    fileSet.storeActualHashDuration(123, null);
    assertEquals(-1, AuUtil.getAuState(mau).getAverageHashDuration());

    // check that estimation isn't changed for ranged sets
    CachedUrlSetSpec rSpec =
        new RangeCachedUrlSetSpec("http://www.example.com/testDir", "ab", "yz");
    fileSet = mau.makeCachedUrlSet(rSpec);
    fileSet.storeActualHashDuration(123, null);
    assertEquals(-1, AuUtil.getAuState(mau).getAverageHashDuration());

    // check that estimation isn't changed for exceptions
    rSpec = new RangeCachedUrlSetSpec("http://www.example.com/testDir");
    fileSet = mau.makeCachedUrlSet(rSpec);
    fileSet.storeActualHashDuration(123, new Exception("bad"));
    assertEquals(-1, AuUtil.getAuState(mau).getAverageHashDuration());

    // check that estimation is grown for timeout exceptions
    rSpec = new RangeCachedUrlSetSpec("http://www.example.com/testDir");
    fileSet = mau.makeCachedUrlSet(rSpec);
    fileSet.storeActualHashDuration(100, null);
    assertEquals(100, AuUtil.getAuState(mau).getAverageHashDuration());
    // simulate a timeout
    fileSet.storeActualHashDuration(200, new SchedService.Timeout("test"));
    assertEquals(300, AuUtil.getAuState(mau).getAverageHashDuration());
    // and another,less than current estimate, shouldn't change it
    fileSet.storeActualHashDuration(100, new HashService.Timeout("test"));
    assertEquals(300, AuUtil.getAuState(mau).getAverageHashDuration());
  }

  public void testCusCompare() throws Exception {
    CachedUrlSetSpec spec1 =
        new RangeCachedUrlSetSpec("http://www.example.com/test");
    CachedUrlSetSpec spec2 =
        new RangeCachedUrlSetSpec("http://www.example.com");
    MockCachedUrlSet cus1 = new MockCachedUrlSet(mau, spec1);
    MockCachedUrlSet cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.BELOW, cus1.cusCompare(cus2));

    spec1 = new RangeCachedUrlSetSpec("http://www.example.com/test");
    spec2 = new RangeCachedUrlSetSpec("http://www.example.com/test/subdir");
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.ABOVE, cus1.cusCompare(cus2));

    spec1 = new RangeCachedUrlSetSpec("http://www.example.com/test", "/a", "/b");
    spec2 = new RangeCachedUrlSetSpec("http://www.example.com/test", "/c", "/d");
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.SAME_LEVEL_NO_OVERLAP,
                 cus1.cusCompare(cus2));

    spec2 = new RangeCachedUrlSetSpec("http://www.example.com/test", "/b", "/d");
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.SAME_LEVEL_OVERLAP,
                 cus1.cusCompare(cus2));

    spec1 = new RangeCachedUrlSetSpec("http://www.example.com/test/subdir2");
    spec2 = new RangeCachedUrlSetSpec("http://www.example.com/subdir");
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.NO_RELATION, cus1.cusCompare(cus2));

    // test for single node specs
    spec1 = new SingleNodeCachedUrlSetSpec("http://www.example.com");
    spec2 = new RangeCachedUrlSetSpec("http://www.example.com/test");
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.SAME_LEVEL_NO_OVERLAP, cus1.cusCompare(cus2));
    // reverse
    assertEquals(CachedUrlSet.SAME_LEVEL_NO_OVERLAP, cus2.cusCompare(cus1));

    // test for Au urls
    spec1 = new AuCachedUrlSetSpec();
    spec2 = new AuCachedUrlSetSpec();
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.SAME_LEVEL_OVERLAP, cus1.cusCompare(cus2));

    spec2 = new RangeCachedUrlSetSpec("http://www.example.com");
    cus2 = new MockCachedUrlSet(mau, spec2);
    assertEquals(CachedUrlSet.ABOVE, cus1.cusCompare(cus2));
    // reverse
    assertEquals(CachedUrlSet.BELOW, cus2.cusCompare(cus1));

    // test for different AUs
    spec1 = new RangeCachedUrlSetSpec("http://www.example.com");
    spec2 = new RangeCachedUrlSetSpec("http://www.example.com");
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(new MockArchivalUnit(), spec2);
    assertEquals(CachedUrlSet.NO_RELATION, cus1.cusCompare(cus2));

    // test for exclusive ranges
    spec1 = new RangeCachedUrlSetSpec("http://www.example.com", "/abc", "/xyz");
    spec2 = new RangeCachedUrlSetSpec("http://www.example.com/test");
    cus1 = new MockCachedUrlSet(mau, spec1);
    cus2 = new MockCachedUrlSet(mau, spec2);
    // this range is inclusive, so should be parent
    assertEquals(CachedUrlSet.ABOVE, cus1.cusCompare(cus2));
    assertEquals(CachedUrlSet.BELOW, cus2.cusCompare(cus1));
    spec1 = new RangeCachedUrlSetSpec("http://www.example.com", "/abc", "/mno");
    cus1 = new MockCachedUrlSet(mau, spec1);
    // this range is exclusive, so should be no relation
    assertEquals(CachedUrlSet.NO_RELATION, cus1.cusCompare(cus2));
    // reverse
    assertEquals(CachedUrlSet.NO_RELATION, cus2.cusCompare(cus1));
  }


  protected void createLeaf(String url, String content,
			    CIProperties props) throws Exception {
    if (props == null) props = new CIProperties();
    InputStream in = null;
    if (content != null) in = new StringInputStream(content);
    if (in == null) in = new StringInputStream("foo");
    V2RepoUtil.storeArt(v2Repo, v2Coll, mau.getAuId(), url, in, props);
  }
  

  private class MyMockArchivalUnit extends MockArchivalUnit {

    public CachedUrlSet makeCachedUrlSet(CachedUrlSetSpec cuss) {
      return new BaseCachedUrlSet(this, cuss);
    }

    public CachedUrl makeCachedUrl(String url) {
      return new BaseCachedUrl(this, url);
    }

    public UrlCacher makeUrlCacher(UrlData ud) {
      return new DefaultUrlCacher(this, ud);
    }

    public FilterRule getFilterRule(String mimeType) {
      return null;
    }

    public boolean shouldBeCached(String url) {
      return true;
    }
  }

}
