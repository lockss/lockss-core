/*

Copyright (c) 2000-2022 Board of Trustees of Leland Stanford Jr. University,.
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
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.*;
import java.math.BigInteger;
import junit.framework.*;

import de.schlichtherle.truezip.file.*;

import org.apache.commons.io.output.NullOutputStream;
import org.lockss.plugin.*;
import org.lockss.plugin.PluginManager.CuContentReq;
import org.lockss.plugin.simulated.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.io.*;
import org.lockss.util.time.TimeBase;
import org.lockss.truezip.*;
import org.lockss.repository.*;

/** Tests for CachedUrls that refer to archive members */
public class TestArchiveMembers extends LockssTestCase {
  protected static Logger log = Logger.getLogger();

  private MockLockssDaemon daemon;
  PluginManager pluginMgr;
  private String tempDirPath;
  private SimulatedArchivalUnit simau;
  MySimulatedArchivalUnit msau;

  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();
    ConfigurationUtil.addFromArgs(TrueZipManager.PARAM_CACHE_DIR, tempDirPath);
    daemon = getMockLockssDaemon();
    pluginMgr = daemon.getPluginManager();
    pluginMgr.setLoadablePluginsReady(true);
    daemon.setDaemonInited(true);
    pluginMgr.startService();
    daemon.getTrueZipManager().startService();
    daemon.getAlertManager();
    daemon.getCrawlManager();

//     // make and start a UrlManager to set up the URLStreamHandlerFactory
//     UrlManager uMgr = new UrlManager();
//     uMgr.initService(daemon);
//     daemon.setDaemonInited(true);
//     uMgr.startService();

    TConfig config = TConfig.get();
    config.setLenient(false);

    simau = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin.class,
					       simAuConfig(tempDirPath));
    simau.generateContentTree();
    msau = (MySimulatedArchivalUnit)simau;
    msau.setArchiveFileTypes(ArchiveFileTypes.DEFAULT);
  }

  public void tearDown() throws Exception {
    simau.deleteContentTree();
    getMockLockssDaemon().stopDaemon();
    daemon.getTrueZipManager().stopService();
    super.tearDown();
  }

  Configuration simAuConfig(String rootPath) {
    Configuration conf = ConfigManager.newConfiguration();
    conf.put("root", rootPath);
    conf.put("depth", "2");
    conf.put("branch", "8");
    conf.put("numFiles", "2");
    conf.put("fileTypes", "" + (SimulatedContentGenerator.FILE_TYPE_HTML +
				SimulatedContentGenerator.FILE_TYPE_XML));
    conf.put("redirectDirToIndex", "true");
    conf.put("autoGenIndexHtml", "true");

    return conf;
  }

  Configuration simAuConfig2(String rootPath) {
    Configuration conf = simAuConfig(rootPath);
    conf.put("depth", "1");
    conf.put("branch", "1");
    conf.put("numFiles", "1");
    conf.put("fileTypes", "" + SimulatedContentGenerator.FILE_TYPE_XML);
    return conf;
  }

  CachedUrl memberCu(String url, String memberName) throws IOException {
    CachedUrl cu0 = simau.makeCachedUrl(url);
    assertTrue(cu0.hasContent());
    return cu0.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu0, memberName));
  }    

  void assertNoArchive(String url, String memberName)
      throws IOException {
    CachedUrl cu0 = simau.makeCachedUrl(url);
    assertFalse(cu0.hasContent());
    CachedUrl cu =
      cu0.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu0, memberName));
    assertFalse(cu.hasContent());
    assertNull(cu.getUnfilteredInputStream());
  }

  void assertNoArchiveMember(String url, String memberName)
      throws IOException {
    CachedUrl cu0 = simau.makeCachedUrl(url);
    assertTrue(cu0.hasContent());
    CachedUrl cu =
      cu0.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu0, memberName));
    assertFalse(cu.hasContent());
    assertNull(cu.getUnfilteredInputStream());
  }

  void assertArchiveMember(String expContentRe, String expMime, long expSize,
			   String url, String memberName)
      throws IOException {
    CachedUrl cu0 = simau.makeCachedUrl(url);
    assertTrue(cu0.hasContent());
    CachedUrl cu =
      cu0.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu0, memberName));
    assertArchiveMemberCu(expContentRe, expMime, expSize,
			  url + "!/" + memberName, cu);
  }

  void assertArchiveMemberByHash(String expHash, String expMime, long expSize, String url,
                                 String memberName) throws IOException, NoSuchAlgorithmException {

    CachedUrl cu0 = simau.makeCachedUrl(url);
    assertTrue("Archive file not found: " + url, cu0.hasContent());

    CachedUrl cu = cu0.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu0, memberName));

    assertArchiveMemberCuByHash(expHash, expMime, expSize, url + "!/" + memberName, cu, null);
  }

  void assertArchiveMemberCuByHash(String expHash, String expMime, long expSize, String expMembUrl,
                                   CachedUrl cu, CachedUrl arcCu) throws IOException, NoSuchAlgorithmException {

    assertClass(BaseCachedUrl.Member.class, cu);

    assertTrue("Should have content: " + cu, cu.hasContent());
    assertEquals(expMembUrl, cu.getUrl());
    assertEquals(expSize, cu.getContentSize());

    // Compute MD5 hash and assert it matches
    try (InputStream is = cu.getUnfilteredInputStream()) {
      assertNotNull("getUnfilteredInputStream was null: " + cu, is);
      assertEquals(expHash, hashInputStream(is, "MD5"));
    }

    Properties props = cu.getProperties();
    assertEquals(expSize, props.get("Length"));

    assertEquals(expMembUrl, props.get(CachedUrl.PROPERTY_NODE_URL));
    assertEquals(expMime, cu.getContentType());

    if (arcCu != null) {
      Properties arcProps = arcCu.getProperties();

      // Last-Modified should be present and not the same as that of the
      // archive (see BaseCachedUrl.synthesizeProperties() )
      assertNotEquals(arcProps.get(CachedUrl.PROPERTY_LAST_MODIFIED), props.get(CachedUrl.PROPERTY_LAST_MODIFIED));
    }

    assertNotNull(props.get(CachedUrl.PROPERTY_LAST_MODIFIED));
  }

  String hashInputStream(InputStream is, String alg) throws IOException, NoSuchAlgorithmException {
    MessageDigest dig = MessageDigest.getInstance(alg);
    HashedInputStream.Hasher hasher = new HashedInputStream.Hasher(dig);
    HashedInputStream his = new HashedInputStream(is, hasher);
    StreamUtil.copy(his, NullOutputStream.NULL_OUTPUT_STREAM);
    byte[] hashOfContent = hasher.getDigest().digest();

    return ByteArray.toHexString(hashOfContent);
  }

  void assertArchiveMemberCu(String expContentRe, String expMime, long expSize,
			     String expMembUrl, CachedUrl cu)
      throws IOException {
    assertArchiveMemberCu(expContentRe, expMime, expSize, expMembUrl, cu, null);
  }

  void assertArchiveMemberCu(String expContentRe, String expMime, long expSize,
			     String expMembUrl, CachedUrl cu, CachedUrl arcCu)
      throws IOException {
    assertClass(BaseCachedUrl.Member.class, cu);
    assertTrue("Should have content: " + cu, cu.hasContent());
    assertEquals(expMembUrl, cu.getUrl());
    InputStream is = cu.getUnfilteredInputStream();
    assertNotNull("getUnfilteredInputStream was null: " + cu, is);
    String s = StringUtil.fromInputStream(is);
    is.close();
    assertMatchesRE(expContentRe, s);
    assertEquals(expSize, cu.getContentSize());

    Properties props = cu.getProperties();
    assertEquals(expSize, props.get("Length"));

    assertEquals(expMembUrl, props.get(CachedUrl.PROPERTY_NODE_URL));
    assertEquals(expMime, cu.getContentType());

    if (arcCu != null) {
      Properties arcProps = arcCu.getProperties();

      // Last-Modified should be present and not the same as that of the
      // archive (see BaseCachedUrl.synthesizeProperties() )
      assertNotEquals(arcProps.get(CachedUrl.PROPERTY_LAST_MODIFIED),
		      props.get(CachedUrl.PROPERTY_LAST_MODIFIED));
    }
    assertNotNull(props.get(CachedUrl.PROPERTY_LAST_MODIFIED));
  }

  public void testReadMember() throws Exception {
    PluginTestUtil.crawlSimAu(simau);
    String aurl = "http://www.example.com/branch1/branch1/zip5.zip";

    assertArchiveMember("file 1, depth 0, branch 0", "text/html", 226,
			aurl, "001file.html");

    assertArchiveMember("file 2, depth 0, branch 0", "text/html", 226,
			aurl, "002file.html");

    assertArchiveMember("<key>file</key><value>2</value>.*<key>depth</key><value>2</value>",
			"application/xml", 230,
			aurl, "branch5/branch2/002file.xml");
    assertNoArchiveMember(aurl, "none.html");
    assertNoArchiveMember(aurl, "branch0/002file.html");

    assertNoArchive(aurl + "bogus", "branch0/002file.html");
    assertNoArchive("bogus" + aurl, "branch0/002file.html");

    assertArchiveMember("this is bin",
			null, 12,
			aurl, "branch5/branch2/001file.bin");
  }

  public void testSplitZips() throws Exception {
    PluginTestUtil.crawlSimAu(simau);

    String aurl = "http://www.example.com/splitzip/lockss-core-daemon.src.zip";
    String burl = "http://www.example.com/splitzip2/FOO.ZIP?xyzzy=foo&wolf=woof";
    String curl = "http://www.example.com/single-part-split-zip.zip";

    assertArchiveMemberByHash("212FBD3A7965442ED3CCA401A0D9BD06", null,
        4765, aurl, "daemon/ArchiveEntry.java");

    assertArchiveMemberByHash("99EF7447C09A6DBED0D21054FE01CA91", null,
        14165, aurl, "daemon/LockssThread.java");

    assertArchiveMemberByHash("B8248EE9F450516DFB9B6D298712C9AD", "text/html",
        119, aurl, "daemon/status/package.html");


    assertArchiveMemberByHash("5DA12A97178F6A612FE0D3AEA7054C86", null,
        32768, burl, "foo/d");

    assertNoArchiveMember(aurl, "none.html");

    assertArchiveMember("<tag>text in tag</tag>",
			"application/xml", 23,
			curl, "dir1/bar.xml");
    assertArchiveMember("This is foo.txt",
			"text/plain", 16,
			curl, "foo.txt");
  }

  public void testReplaceZipExtension() throws Exception {
    assertEquals("http://www.example.com/foo/zip1.z01",
        TFileCache.replaceZipExtension("http://www.example.com/foo/zip1.zip", "z", 2, 1));

    assertEquals("http://www.example.com/foo/zip1.Z01?",
        TFileCache.replaceZipExtension("http://www.example.com/foo/zip1.zip?", "Z", 2, 1));

    assertEquals("http://www.example.com/foo/zip1.z01?bar=xyzzy",
        TFileCache.replaceZipExtension("http://www.example.com/foo/zip1.zip?bar=xyzzy", "z", 2, 1));
  }

  public void testInferContentType() throws Exception {
    PluginTestUtil.crawlSimAu(simau);
    String aurl = "http://www.example.com/branch1/branch1/zip5.zip";

    msau.setUrlMimeTypeMap(PatternStringMap.fromSpec("\\.bin$,application/beans"));

    assertArchiveMember("file 1, depth 0, branch 0", "text/html", 226,
			aurl, "001file.html");

    assertArchiveMember("this is bin",
			"application/beans", 12,
			aurl, "branch5/branch2/001file.bin");
  }

  public void testIll() throws Exception {
    PluginTestUtil.crawlSimAu(simau);
    String aurl = "http://www.example.com/branch1/branch1/zip5.zip";
    String memberName = "001file.html";
    CachedUrl cu0 = simau.makeCachedUrl(aurl);
    assertTrue(cu0.hasContent());
    CachedUrl cu =
      cu0.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu0, memberName));
    try {
      cu.getArchiveMemberCu(ArchiveMemberSpec.fromCu(cu, memberName));
      fail("Shouldn't be able to create a CU member from a CU member");
    } catch (UnsupportedOperationException e) {
    }
    try {
      cu.getCuVersion(1);
      fail("Shouldn't be able to get a version of a CU member");
    } catch (UnsupportedOperationException e) {
    }
    try {
      cu.getCuVersions(3);
      fail("Shouldn't be able to get versions of a CU member");
    } catch (UnsupportedOperationException e) {
    }
  }

  public void testIter1() throws Exception {
    PluginTestUtil.crawlSimAu(simau);
    String aurl = "http://www.example.com/branch1/branch1/zip5.zip";
    CachedUrlSet cus = simau.makeCachedUrlSet(new RangeCachedUrlSetSpec(aurl));
    Iterator<CachedUrl> iter = cus.archiveMemberIterator();
    int cnt = 0;
    while (iter.hasNext()) {
      CachedUrl cu = iter.next();
      assertTrue(cu.hasContent());
      cnt++;
    }
    assertEquals(17, cnt);
  }

  List<String> readLinesFromResource(String resource) throws IOException {
    InputStream urlsIn = getResourceAsStream(resource);
    BufferedReader rdr = new BufferedReader(StringUtil.getLineReader(urlsIn));
    List<String> res = new ArrayList<String>();
    for (String line = rdr.readLine(); line != null; line = rdr.readLine()) {
      line = line.trim();
      if (StringUtil.startsWithIgnoreCase(line, "#")) {
	continue;
      }
      res.add(line);
    }
    return res;
  }

  Pattern pat = Pattern.compile(".*?(?:branch([0-9])/)?(?:branch([0-9])/)?00([0-9])file\\.html");

  public void testIter2() throws Exception {
    PluginTestUtil.crawlSimAu(simau);
    CachedUrlSet cus = simau.makeCachedUrlSet(new AuCachedUrlSetSpec());
    // XXXREPO srcpub_urls.txt edited to include final slash on dirs
    List urls = readLinesFromResource("srcpub_urls.txt");
    Iterator<CachedUrl> cuIter = cus.archiveMemberIterator();
    Iterator<String> urlIter = urls.iterator();

    int htmlcnt = 0;

    boolean didCheckDelete = false;

    List<String> cusFiles = new ArrayList<>();
    while (cuIter.hasNext()) {
      CachedUrl cu = cuIter.next();
      String url = cu.getUrl();
      assertTrue(cu.hasContent());
      cusFiles.add(url);

      // This won't work until we add the necessary logic to recreate
      // TFiles that have been umounted

//       // While traversing an archive, delete the temp file backing that
//       // archive, then continue the iteration to ensure that the TFile
//       // remains usable even though its file has ben deleted.  Just
//       // documenting that it works in this case - I don't know whether the
//       // TFile contract allows it or if we need a locking protocol to
//       // prevent it.

//       String arcurl = "http://www.example.com/branch1/branch1/zip5.zip";
//       String trigger = "branch5/002file.xml";
//       if (url.equals(arcurl + "!/" + trigger)) {
// 	CachedUrl arccu = simau.makeCachedUrl(arcurl);
// 	org.lockss.truezip.TFileCache tfc =
// 	  getMockLockssDaemon().getTrueZipManager().getTFileCache();

// 	org.lockss.truezip.TFileCache.Entry ent =
// 	  tfc.getCachedTFileEntry(arccu);
// 	assertTrue(ent.isValid());
// 	assertTrue(ent.exists());
// 	tfc.flushEntry(ent);
// 	assertFalse(ent.isValid());
// 	assertFalse(ent.exists());
// 	didCheckDelete = true;
//       }

      Matcher m1 = pat.matcher(url);
      if (m1.matches()) {
	htmlcnt++;
	int depth =
	  (m1.group(1) != null ? 1 : 0) + (m1.group(2) != null ? 1 : 0);
	log.debug("Checking member: " + url);
	String expContent =
	  String.format("This is file %s, depth %s, branch %s",
			m1.group(3),
			depth,
			(m1.group(2) != null
			 ? m1.group(2)
			 : (m1.group(1) != null ? m1.group(1) : "0")));
	String content = stringFromCu(cu);
	assertEquals(content.length(), cu.getContentSize());
	assertMatchesRE(url, expContent, content);
      }
    }
    assertEquals(urls,cusFiles);
    assertEquals(170, htmlcnt);

//     assertTrue(didCheckDelete);
  }

  public void testIterPruned() throws Exception {
    PluginTestUtil.crawlSimAu(simau);
    CachedUrlSetSpec cuss =
      PrunedCachedUrlSetSpec.excludeMatchingSubTrees("http://www.example.com/",
						     "http://www.example.com/.*\\.zip");
    CachedUrlSet cus = simau.makeCachedUrlSet(cuss);
    List urls = new ArrayList();
    for (String s : readLinesFromResource("srcpub_urls.txt")) {
      if (!s.matches(".*\\.zip.*")) {
	urls.add(s);
      }
    }
    
    Iterator<CachedUrl> cuIter = cus.archiveMemberIterator();
    Iterator<String> urlIter = urls.iterator();

    int cnt = 0;
    int htmlcnt = 0;

    while (cuIter.hasNext()) {
      CachedUrl cu = cuIter.next();
      String url = cu.getUrl();
      assertTrue(cu.hasContent());
      assertEquals(url, urlIter.next(), url);
      cnt++;

      Matcher m1 = pat.matcher(url);
      if (m1.matches()) {
	htmlcnt++;
	int depth =
	  (m1.group(1) != null ? 1 : 0) + (m1.group(2) != null ? 1 : 0);
	String expContent =
	  String.format("This is file %s, depth %s, branch %s",
			m1.group(3),
			depth,
			(m1.group(2) != null
			 ? m1.group(2)
			 : (m1.group(1) != null ? m1.group(1) : "0")));
	String content = stringFromCu(cu);
	assertEquals(content.length(), cu.getContentSize());
	assertMatchesRE(url, expContent, content);
      }
    }
    assertEquals(urls.size(), cnt++);
    assertEquals(106, htmlcnt);
  }

  void copyCu(String fromUrl, String toUrl) throws IOException {
    CachedUrl cu = simau.makeCachedUrl(fromUrl);
    InputStream ins = cu.getUnfilteredInputStream();
    CIProperties props = cu.getProperties();
    props.setProperty(CachedUrl.PROPERTY_FETCH_TIME,
		      Long.toString(TimeBase.nowMs()));
    UrlCacher uc = simau.makeUrlCacher(new UrlData(ins, props, toUrl));
    uc.storeContent();
  }

  // Sample of URLs in recent archives 
  String[] shouldContain = {
    "http://www.example.com/branch1/branch1/newzip5.zip!/001file.html",
    "http://www.example.com/branch1/branch1/newzip5.zip!/branch5/002file.xml",
    "http://www.example.com/branch1/branch1/newzip5.zip!/branch5/branch1/001file.html",
    "http://www.example.com/branch1/branch1/newzip5.zip!/branch5/branch2/002file.xml",
    "http://www.example.com/branchnew/newtgz1.tgz!/001file.html",
    "http://www.example.com/branchnew/newtgz1.tgz!/001file.xml",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch1/branch1/zip5.zip/002file.xml",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch1/branch1/zip5.zip/branch5/001file.html",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch1/branch1/zip6.zip/branch6/branch2/002file.html",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch1/branch2/001file.xml",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch1/branch2/zip7.zip/001file.html",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch1/branch2/zip8.zip/branch8/branch2/002file.xml",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch2/001file.html",
    "http://www.example.com/branchnew/newtgz1.tgz!/branch2/branch2/002file.xml",
  };

  // Sample of URLs in old archives 
  String[] shouldNotContain = {
    "http://www.example.com/001file.html",
    "http://www.example.com/branch1",
    "http://www.example.com/branch1/001file.html",
    "http://www.example.com/branch1/branch1/index.html",
    "http://www.example.com/branch1/branch1/zip5.zip!/001file.html",
    "http://www.example.com/branch1/branch1/zip6.zip!/branch6/branch2/002file.xml",
    "http://www.example.com/branch1/branch2/001file.html",
    "http://www.example.com/branch1/branch2/zip7.zip!/001file.html",
  };

  public void testIterExcludeFilesUnchangedAfter() throws Exception {
    TimeBase.setSimulated(100000);
    PluginTestUtil.crawlSimAu(simau);
    TimeBase.setSimulated(500000);
    copyCu("http://www.example.com/branch1/branch1/zip5.zip",
	   "http://www.example.com/branch1/branch1/newzip5.zip");
    TimeBase.setSimulated(700000);
    copyCu("http://www.example.com/tgz1.tgz",
	   "http://www.example.com/branchnew/newtgz1.tgz");
    CachedUrlSet cus = simau.makeCachedUrlSet(new AuCachedUrlSetSpec());
    cus.setExcludeFilesUnchangedAfter(200000);
    Set urls = new HashSet();
    Iterator<CachedUrl> cuIter = cus.archiveMemberIterator();
    while (cuIter.hasNext()) {
      CachedUrl cu = cuIter.next();
      urls.add(cu.getUrl());
    }
    for (String url : shouldContain) {
      assertContains(urls, url);
    }
    for (String url : shouldNotContain) {
      assertDoesNotContain(urls, url);
    }
    assertEquals(109, urls.size());

    cus.setExcludeFilesUnchangedAfter(600000);
    Set urls2 = new HashSet();
    Iterator<CachedUrl> cuIter2 = cus.archiveMemberIterator();
    while (cuIter2.hasNext()) {
      CachedUrl cu = cuIter2.next();
      urls2.add(cu.getUrl());
    }
    assertEquals(92, urls2.size());
  }

  public void testFindCu() throws Exception {
    PluginTestUtil.crawlSimAu(simau);

    // Generate a second sim AU
    String tmp2 = getTempDir().getAbsolutePath() + File.separator;
    SimulatedArchivalUnit simau2 =
      PluginTestUtil.createAndStartSimAu(MySimulatedPlugin.class,
					 simAuConfig2(tmp2));
    MySimulatedArchivalUnit msau2 = (MySimulatedArchivalUnit)simau2;
    msau2.setArchiveFileTypes(ArchiveFileTypes.DEFAULT);
    simau2.generateContentTree();
    PluginTestUtil.crawlSimAu(simau2);

    log.debug2("simau: " + simau);
    log.debug2("simau2: " + simau2);
    // Tests below rely on simau AUID sorting before simau2's AUID, in
    // order to guarantee expected result order from
    // getArtifact...AllAus().  (It should, because of counter in temdir
    // name in AUID)
    assertTrue(simau.getAuId().compareTo(simau2.getAuId()) < 0);

    CachedUrl cu;
    String arcUrl = "http://www.example.com/branch1/branch1/zip5.zip";
    
    // Search for a member URL
    cu = pluginMgr.findCachedUrl(arcUrl + "!/001file.html");
    assertArchiveMemberCu("file 1, depth 0, branch 0", "text/html", 226,
			  arcUrl + "!/001file.html", cu);
    // Should be from 1st AU
    assertSame(simau, cu.getArchivalUnit());

    // The archive file iteslf
    cu = pluginMgr.findCachedUrl(arcUrl);
    assertTrue(cu.hasContent());
    assertEquals(5392, cu.getContentSize());
    assertSame(simau, cu.getArchivalUnit());
    assertEquals(2, pluginMgr.getRecentCuMisses());
    assertEquals(0, pluginMgr.getRecentCuHits());

    cu = pluginMgr.findCachedUrl(arcUrl + "!/no/such/member");
    assertNull(cu);

    cu = pluginMgr.findCachedUrl(arcUrl + "!/no/such/member",
				 CuContentReq.DontCare);
    assertFalse(cu.hasContent());

    cu = pluginMgr.findCachedUrl(arcUrl + "nofile!/no/such/member");
    assertNull(cu);

    String membUrl = arcUrl + "nofile!/no/such/member";
    cu = pluginMgr.findCachedUrl(membUrl, CuContentReq.DontCare);
    assertFalse(cu.hasContent());
    assertEquals(membUrl, cu.getUrl());

    // Disable archive processing in 1st AU, ensure that member is found in
    // 2nd AU.  Do this for both possible AuSearchSet orderings
    msau.setArchiveFileTypes(null);

    pluginMgr.flushRecentCuCache();
    pluginMgr.promoteAuInSearchSets(msau);
    cu = pluginMgr.findCachedUrl(arcUrl + "!/001file.html");
    assertArchiveMemberCu("file 1, depth 0, branch 0", "text/html", 226,
			  arcUrl + "!/001file.html", cu);
    assertSame(simau2, cu.getArchivalUnit());
    assertEquals(7, pluginMgr.getRecentCuMisses());
    assertEquals(0, pluginMgr.getRecentCuHits());

    pluginMgr.flushRecentCuCache();
    pluginMgr.promoteAuInSearchSets(msau2);
    cu = pluginMgr.findCachedUrl(arcUrl + "!/001file.html");
    assertArchiveMemberCu("file 1, depth 0, branch 0", "text/html", 226,
			  arcUrl + "!/001file.html", cu);
    assertSame(simau2, cu.getArchivalUnit());
    assertEquals(8, pluginMgr.getRecentCuMisses());
    assertEquals(0, pluginMgr.getRecentCuHits());

    // Test recentCuMap cache
    cu = pluginMgr.findCachedUrl(arcUrl + "!/001file.html");
    assertArchiveMemberCu("file 1, depth 0, branch 0", "text/html", 226,
			  arcUrl + "!/001file.html", cu);
    assertSame(simau2, cu.getArchivalUnit());
    assertEquals(8, pluginMgr.getRecentCuMisses());
    assertEquals(1, pluginMgr.getRecentCuHits());

    // If AU has no archive file types, currently CU will still be a Member
    // but with no content.  This time use a different member name to evade
    // recentCuMap cache.
    msau2.setArchiveFileTypes(null);
    cu = pluginMgr.findCachedUrl(arcUrl + "!/002file.html");
    assertNull(cu);

    membUrl = arcUrl + "!/002file.html";
    cu = pluginMgr.findCachedUrl(membUrl, CuContentReq.DontCare);
    assertEquals(membUrl, cu.getUrl());

    assertNotClass(BaseCachedUrl.Member.class, cu);
    assertFalse(cu.hasContent());
  }    

  String stringFromCu(CachedUrl cu) throws IOException {
    InputStream is = cu.getUnfilteredInputStream();
    try {
      return StringUtil.fromInputStream(is);
    } finally {
      IOUtil.safeClose(is);
    }
  }

  public static class MySimulatedPlugin extends SimulatedPlugin {
    public SimulatedContentGenerator getContentGenerator(Configuration cf,
							 String fileRoot) {
      return new MySimulatedContentGenerator(fileRoot);
    }

    public ArchivalUnit createAu0(Configuration auConfig)
      throws ArchivalUnit.ConfigurationException {
        ArchivalUnit au = new MySimulatedArchivalUnit(this);
        au.setConfiguration(auConfig);
        return au;
      }
  }

  public static class MySimulatedArchivalUnit extends SimulatedArchivalUnit {
    ArchiveFileTypes aft = null;
    PatternStringMap urlMimeMap = PatternStringMap.EMPTY;

    public MySimulatedArchivalUnit(Plugin owner) {
      super(owner);
    }

    public ArchiveFileTypes getArchiveFileTypes() {
      return aft;
    }

    public void setArchiveFileTypes(ArchiveFileTypes aft) {
      this.aft = aft;
    }

    @Override
    public PatternStringMap makeUrlMimeTypeMap() {
      return urlMimeMap;
    }

    @Override
    public PatternStringMap makeUrlMimeValidationMap() {
      return urlMimeMap;
    }

    public void setUrlMimeTypeMap(PatternStringMap map) {
      urlMimeMap = map;
    }
  }

  public static class MySimulatedContentGenerator
    extends SimulatedContentGenerator {
    protected MySimulatedContentGenerator(String fileRoot) {
      super(fileRoot);
    }

    @Override
    public String generateContentTree() {
      File rootDir = new File(contentRoot);
      InputStream in = this.getClass().getResourceAsStream("srcpub.zip");
      try {
	ZipUtil.unzip(in, rootDir);
      } catch (Exception e) {
	throw new RuntimeException("Couldn't unzip prototype srcpub tree", e);
      } finally {
	IOUtil.safeClose(in);
      }
      return rootDir.toString();
    }

  }



  private static class MyCachedUrl extends BaseCachedUrl {
    private boolean gotUnfilteredStream = false;
    private boolean gotFilteredStream = false;
    private CIProperties props = new CIProperties();

    public MyCachedUrl(ArchivalUnit au, String url) {
      super(au, url);
      props.setProperty(PROPERTY_CONTENT_TYPE, "text/html");
    }


    public InputStream getUnfilteredInputStream() {
      gotUnfilteredStream = true;
      return null;
    }

    public boolean gotUnfilteredStream() {
      return gotUnfilteredStream;
    }

    protected InputStream getFilteredStream() {
      gotFilteredStream = true;
      return super.getFilteredStream();
    }

    public boolean hasContent() {
      throw new UnsupportedOperationException("Not implemented");
    }

    public Reader openForReading() {
      return new StringReader("Test");
    }

    public CIProperties getProperties() {
      return props;
    }

    public void setProperties(CIProperties props) {
      this.props = props;
    }
  }

}
