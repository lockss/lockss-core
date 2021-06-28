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
import java.nio.charset.Charset;
import java.util.*;
import java.security.MessageDigest;
import junit.framework.*;
import org.lockss.app.*;
import org.lockss.plugin.*;
import org.lockss.config.Configuration;
import org.lockss.daemon.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.StreamUtil.IgnoreCloseInputStream;
import org.lockss.repository.*;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.http.*;
import org.apache.http.message.*;
import org.springframework.http.HttpHeaders;
import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;

/** Variants test "current" BaseCachedUrl and version-specific
 * BaseCachedUrl instances */
public class TestV2BaseCachedUrl extends LockssTestCase {
  private static final String PARAM_SHOULD_FILTER_HASH_STREAM =
    Configuration.PREFIX+"baseCachedUrl.filterHashStream";

  private static final Logger logger = Logger.getLogger();

  protected MockArchivalUnit mau;
  protected MockLockssDaemon theDaemon;
  protected MockPlugin plugin;

  protected LockssRepository v2Repo;
  protected String v2Coll;

  String url1 = "http://www.example.com/testDir/leaf1";
  String url2 = "http://www.example.com/testDir/leaf2";
  String url3 = "http://www.example.com/testDir/leaf3";
  String url4 = "http://www.example.com/testDir/leaf4";
  String urlparent = "http://www.example.com/testDir";
  String content1 = "test content 1";
  String content2 = "test content 2 longer";
  String badcontent = "this is the wrong content string";

  public void setUp() throws Exception {
    super.setUp();
    setUpDiskSpace();
    theDaemon = getMockLockssDaemon();
    theDaemon.getHashService();

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
    super.tearDown();
  }

  InputStream getEncodedInputStream(String content, String charset)
      throws IOException {
    return new ByteArrayInputStream(content.getBytes(charset));
  }

  /** Tests that are independent of versioning */
  public static class NotVersionedTests extends TestV2BaseCachedUrl {
    public NotVersionedTests() {
    }

    public void testFilterParamDefault() {
      MyCachedUrl cu = new MyCachedUrl(new MyAu(), null);
      cu.openForHashing();
      assertTrue(cu.gotFilteredStream);
    }

    public void testFilterParamFilterOn() throws IOException {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      MyCachedUrl cu = new MyCachedUrl(new MyAu(), null);
      cu.openForHashing();
      assertTrue(cu.gotFilteredStream);
    }

    public void testFilterParamFilterOff() throws IOException {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "false");
      MyCachedUrl cu = new MyCachedUrl(new MyAu(), null);
      cu.openForHashing();
      assertFalse(cu.gotFilteredStream);
    }

    public void testNoVersions() throws IOException {
      CachedUrl cu = mau.makeCachedUrl(url1);
      assertFalse(cu.hasContent());
      CachedUrl[] all = cu.getCuVersions();
      assertEquals(0, all.length);
//       assertFalse(all[0].hasContent());
//       assertEquals(cu.getUrl(), all[0].getUrl());
      try {
	cu.getUnfilteredInputStream();
	fail("getUnfilteredInputStream() should fail when no content");
      } catch (UnsupportedOperationException e) {
	assertMatchesRE("No content", e.getMessage());
      }
      try {
	cu.getProperties();
	fail("getProperties() should fail when no content");
      } catch (UnsupportedOperationException e) {
	assertMatchesRE("No content", e.getMessage());
      }
    }
  }

  // Instrumented BaseCachedUrl.  In order to do analogous for versioned
  // CU's, need to implement factory in BaseCachedUrl
  private static class InstrumentedBaseCachedUrl extends BaseCachedUrl {
    private List<ArtifactData> releasedAds = new ArrayList<>();
    private List<ArtifactData> obtainedAds = new ArrayList<>();
    private List<LockssRepository.IncludeContent> includeArgs =
      new ArrayList<>();

    public InstrumentedBaseCachedUrl(ArchivalUnit au, String url) {
      super(au, url);
    }

    @Override
    ArtifactData getArtifactData(LockssRepository repo, Artifact art,
				 LockssRepository.IncludeContent includeContent)
	throws IOException {
      includeArgs.add(includeContent);
      ArtifactData res = super.getArtifactData(repo, art, includeContent);
      obtainedAds.add(res);
      return res;
    }

    @Override
    void releaseArtifactData(ArtifactData ad) {
      releasedAds.add(ad);
      super.releaseArtifactData(ad);
    }

    public List<ArtifactData> getObtainedAds() {
      return obtainedAds;
    }

    public List<ArtifactData> getReleasedAds() {
      return releasedAds;
    }

    public List<LockssRepository.IncludeContent> getIncludeArgs() {
      return includeArgs;
    }

  }

  // helper for above tests
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

    public InputStream getUnfilteredInputStream(MessageDigest md) {
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

    protected InputStream getFilteredStream(HashedInputStream.Hasher hasher) {
      gotFilteredStream = true;
      return super.getFilteredStream(hasher);
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

  /** Tests that run with the current version and with an older version */
  public abstract static class VersionedTests extends TestV2BaseCachedUrl {
    public VersionedTests() {
    }

    /** Concrete class must create either a single version or multiple
     * versions here */
    void createLeaf(String url, String content, CIProperties props)
	throws Exception {
      if (content == null) content = "";
      createLeaf(url, new StringInputStream(content), props);
    }

    abstract void createLeaf(String url, InputStream contentStream,
			     CIProperties props)
	throws Exception;

    /** Concrete class must return either the current version or an older
     * version here */
    abstract CachedUrl getTestCu(String url);

    public void testGetUrl() throws Exception {
      createLeaf(url1, content1, null);

      CachedUrl cu = getTestCu(url1);
      assertEquals(url1, cu.getUrl());
    }

    public void testGetUrlSlash() throws Exception {
      String urlslash = "http://www.example.com/testDir/dir/";

      createLeaf(urlslash, content1, null);

      CachedUrl cu = getTestCu(urlslash);
      assertEquals(urlslash, cu.getUrl());
    }

    public void testIsLeaf() throws Exception {
      createLeaf(url1, content1, null);
      createLeaf(url2, (String)null, null);

      CachedUrl cu = getTestCu(url1);
      assertTrue(cu.isLeaf());
      cu = getTestCu(url2);
      assertTrue(cu.isLeaf());
    }

    public void testHasContent() throws Exception {
      // In version tests, getTestCu() not applicable before node created
      assertFalse(mau.makeCachedUrl(url1).hasContent());

      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_INCLUDED_ONLY, "true");

      createLeaf(url1, content1, null);
      createLeaf(url2, content2, null);
      createLeaf(url3, "", null);
      mau.addUrlToBeCached(url1);
      mau.addUrlToBeCached(url3);

      CachedUrl cu = getTestCu(url1);
      assertTrue(cu.hasContent());

      cu = getTestCu(url2);
      // not in crawl rules
      assertFalse(cu.hasContent());
      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_INCLUDED_ONLY, "false");
      assertTrue(cu.hasContent());
      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_INCLUDED_ONLY, "true");
      assertFalse(cu.hasContent());

      // IncludedOnly option should override config param
      cu.setOption(CachedUrl.OPTION_INCLUDED_ONLY, "false");
      assertTrue(cu.hasContent());
      cu.setOption(CachedUrl.OPTION_INCLUDED_ONLY, "true");
      assertFalse(cu.hasContent());
      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_INCLUDED_ONLY, "false");
      assertFalse(cu.hasContent());
      cu.setOption(CachedUrl.OPTION_INCLUDED_ONLY, "neither");
      assertTrue(cu.hasContent());


      cu = getTestCu(url3);
      assertTrue(cu.hasContent());
    }

    public void testGetUnfilteredInputStream() throws Exception {
      createLeaf(url1, content1, null);
      createLeaf(url2, content2, null);
      createLeaf(url3, "", null);

      CachedUrl cu = getTestCu(url1);
      InputStream urlIs = cu.getUnfilteredInputStream();
      assertEquals(content1, StringUtil.fromInputStream(urlIs));

      cu = getTestCu(url2);
      urlIs = cu.getUnfilteredInputStream();
      assertEquals(content2, StringUtil.fromInputStream(urlIs));
      // Read a second time
      urlIs = cu.getUnfilteredInputStream();
      assertEquals(content2, StringUtil.fromInputStream(urlIs));

      cu = getTestCu(url3);
      urlIs = cu.getUnfilteredInputStream();
      assertEquals("", StringUtil.fromInputStream(urlIs));

    }

    // Ensure all resources (multiple InputStreams) are released

    // Currently runs only in highest-version variants, as there's no
    // mechanism to create a custom subclass of BaseCachedUrl.Version.
    // Need to change BaseCachedUrl to use a factory.
    public void doTestGetMultipleStreams() throws Exception {
      createLeaf(url1, content1, null);

      CachedUrl cu1 = getTestCu(url1);
      InputStream u1i1 = cu1.getUnfilteredInputStream();
      InputStream u1i2 = cu1.getUnfilteredInputStream();
      assertNotSame(u1i1, u1i2);
      assertEquals(content1, StringUtil.fromInputStream(u1i1));
      assertEquals(content1, StringUtil.fromInputStream(u1i2));
      InputStream u1i3 = cu1.getUnfilteredInputStream();
      assertNotSame(u1i1, u1i3);
      assertNotSame(u1i2, u1i3);
      assertEquals(content1, StringUtil.fromInputStream(u1i3));

      InstrumentedBaseCachedUrl ibcu1 = (InstrumentedBaseCachedUrl)cu1;
      assertEmpty(ibcu1.getReleasedAds());
      u1i2.close();	     // close one explcitly, should change behavior
      cu1.release();
      assertEquals(ibcu1.getObtainedAds(), ibcu1.getReleasedAds());
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.ALWAYS,
				 LockssRepository.IncludeContent.ALWAYS,
				 LockssRepository.IncludeContent.ALWAYS),
		   ibcu1.getIncludeArgs());
      // XXX would be better to check that InputStreams get closed but no
      // good way to get ahold of them.
    }

    public void testGZipped() throws Exception {
      String content = "this is some text to be compressssssed";
      String clen = ""+content.length();
      String clenz = ""+StreamUtil.countBytes(new GZIPpedInputStream(content));
      createLeaf(url1, new GZIPpedInputStream(content),
		 fromArgs(CachedUrl.PROPERTY_CONTENT_ENCODING,
			  "gzip",
			  CachedUrl.PROPERTY_CONTENT_LENGTH,
			  clenz));
      createLeaf(url2, new GZIPpedInputStream(content),
		 fromArgs(CachedUrl.PROPERTY_CONTENT_LENGTH, clenz));
      createLeaf(url3, content,
		 fromArgs(CachedUrl.PROPERTY_CONTENT_ENCODING,
			  "gzip",
			  CachedUrl.PROPERTY_CONTENT_LENGTH,
			  clen));

      CachedUrl cu = getTestCu(url1);
      assertSameBytes(new GZIPpedInputStream(content),
		      cu.getUnfilteredInputStream());
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUncompressedInputStream()));
      assertEquals(content,
		   StringUtil.fromReader(cu.openForReading()));
      Properties props = cu.getProperties();
      assertEquals("gzip",
		   props.getProperty(CachedUrl.PROPERTY_CONTENT_ENCODING));
      assertEquals(clenz,
		   props.getProperty(CachedUrl.PROPERTY_CONTENT_LENGTH));

      // no content-encoding header, shouldn't get uncompressed
      cu = getTestCu(url2);
      assertSameBytes(new GZIPpedInputStream(content),
		      cu.getUnfilteredInputStream());
      assertSameBytes(new GZIPpedInputStream(content),
		      cu.getUncompressedInputStream());

      // 1.67 and 1.68 uncompressed but didn't remove Content-Encoding
      // header; ensure we don't fail when uncompressor fails
      cu = getTestCu(url3);
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUnfilteredInputStream()));
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUncompressedInputStream()));
      assertEquals(content,
		   StringUtil.fromReader(cu.openForReading()));
    }

    public void testDeflated() throws Exception {
      String content = "this is some text to be compressssssed";
      String clen = ""+content.length();
      String clenz = ""+StreamUtil.countBytes(new DeflatedInputStream(content));
      createLeaf(url1, new DeflatedInputStream(content),
		 fromArgs(CachedUrl.PROPERTY_CONTENT_ENCODING,
			  "deflate",
			  CachedUrl.PROPERTY_CONTENT_LENGTH,
			  clenz));
      createLeaf(url2, new DeflatedInputStream(content),
		 fromArgs(CachedUrl.PROPERTY_CONTENT_LENGTH, clenz));
      createLeaf(url3, content,
		 fromArgs(CachedUrl.PROPERTY_CONTENT_ENCODING,
			  "deflate",
			  CachedUrl.PROPERTY_CONTENT_LENGTH,
			  clen));

      CachedUrl cu = getTestCu(url1);
      assertSameBytes(new DeflatedInputStream(content),
		      cu.getUnfilteredInputStream());
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUncompressedInputStream()));
      assertEquals(content,
		   StringUtil.fromReader(cu.openForReading()));
      Properties props = cu.getProperties();
      assertEquals("deflate",
		   props.getProperty(CachedUrl.PROPERTY_CONTENT_ENCODING));
      assertEquals(clenz,
		   props.getProperty(CachedUrl.PROPERTY_CONTENT_LENGTH));

      // no content-encoding header, shouldn't get uncompressed
      cu = getTestCu(url2);
      assertSameBytes(new DeflatedInputStream(content),
		      cu.getUnfilteredInputStream());
      assertSameBytes(new DeflatedInputStream(content),
		      cu.getUncompressedInputStream());

      // 1.67 and 1.68 uncompressed but didn't remove Content-Encoding
      // header; ensure we don't fail when uncompressor fails
      cu = getTestCu(url3);
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUnfilteredInputStream()));
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUncompressedInputStream()));
      assertEquals(content,
		   StringUtil.fromReader(cu.openForReading()));
    }

    public void testIdentity() throws Exception {
      String content = "this is some text not to be compressed";
      String clen = ""+content.length();
      String clenz = ""+StreamUtil.countBytes(new GZIPpedInputStream(content));
      createLeaf(url1, content,
		 fromArgs(CachedUrl.PROPERTY_CONTENT_ENCODING,
			  "identity",
			  CachedUrl.PROPERTY_CONTENT_LENGTH,
			  clen));
      createLeaf(url2, new GZIPpedInputStream(content),
		 fromArgs(CachedUrl.PROPERTY_CONTENT_ENCODING,
			  "identity",
			  CachedUrl.PROPERTY_CONTENT_LENGTH,
			  clenz));

      CachedUrl cu = getTestCu(url1);
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUnfilteredInputStream()));
      assertEquals(content,
		   StringUtil.fromInputStream(cu.getUncompressedInputStream()));
      Properties props = cu.getProperties();
      assertEquals("identity", props.getProperty(CachedUrl.PROPERTY_CONTENT_ENCODING));
      assertEquals(clen, props.getProperty(CachedUrl.PROPERTY_CONTENT_LENGTH));

      cu = getTestCu(url2);
      assertSameBytes(new GZIPpedInputStream(content),
		      cu.getUnfilteredInputStream());
      assertSameBytes(new GZIPpedInputStream(content),
		      cu.getUncompressedInputStream());
    }

    public void testOpenForHashingDefaultsToNoFiltering() throws Exception {
      createLeaf(url1, "<test stream>", null);
      String str = "This is a filtered stream";
      mau.setFilterRule(new MyMockFilterRule(new StringReader(str)));

      CachedUrl cu = getTestCu(url1);
      InputStream urlIs = cu.getUnfilteredInputStream();
      assertNotEquals(str, StringUtil.fromInputStream(urlIs));

      mau.setHashFilterFactory(new MyMockFilterFactory(new StringInputStream(str)));
      urlIs = cu.getUnfilteredInputStream();
      assertNotEquals(str, StringUtil.fromInputStream(urlIs));
    }

    public void testOpenForHashingWontFilterIfConfiguredNotTo()
	throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "false");
      createLeaf(url1, "<test stream>", null);
      String str = "This is a filtered stream";
      mau.setFilterRule(new MyMockFilterRule(new StringReader(str)));

      CachedUrl cu = getTestCu(url1);
      InputStream urlIs = cu.openForHashing();
      assertNotEquals(str, StringUtil.fromInputStream(urlIs));

      mau.setHashFilterFactory(new MyMockFilterFactory(new StringInputStream(str)));
      urlIs = cu.openForHashing();
      assertNotEquals(str, StringUtil.fromInputStream(urlIs));
    }

    public void testOpenForHashingUsesFilterRule()
	throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      createLeaf(url1, "blah <test stream>", null);
      String str = "This is a filtered stream";
      mau.setFilterRule(new MyMockFilterRule(new StringReader(str)));

      CachedUrl cu = getTestCu(url1);
      InputStream urlIs = cu.openForHashing();
      assertEquals(str, StringUtil.fromInputStream(urlIs));
    }

    public void testOpenForHashingUsesFilterFactory()
	throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      createLeaf(url1, "blah <test stream>", null);
      String str = "This is a filtered stream";
      mau.setHashFilterFactory(new MyMockFilterFactory(new StringInputStream(str)));

      CachedUrl cu = getTestCu(url1);
      InputStream urlIs = cu.openForHashing();
      assertEquals(str, StringUtil.fromInputStream(urlIs));
    }

    public void testOpenForHashingWithUnfilteredHash()
	throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      String unfiltered = "This is the content before filtering";
      createLeaf(url1, unfiltered, null);
      String str = "This is a filtered stream";
      mau.setHashFilterFactory(new MyMockFilterFactory(new StringInputStream(str)));
      MockMessageDigest md = new MockMessageDigest();
      HashedInputStream.Hasher hasher = new HashedInputStream.Hasher(md);
      CachedUrl cu = getTestCu(url1);
      assertFalse(hasher.isValid());
      InputStream urlIs = cu.openForHashing(hasher);
      // hasher.isValid() not checked yet because it will be true iff
      // filter pre-reads input (which it happens to).
      String result = StringUtil.fromInputStream(urlIs);
      logger.debug3("Want: " + str);
      logger.debug3("Get: " + result);
      assertEquals(str, result);
      assertTrue(hasher.isValid());
      InputStream hashedStream = new ByteArrayInputStream(md.getUpdatedBytes());
      String hashInput = StringUtil.fromInputStream(hashedStream);
      logger.debug3("Hasher gets: " + hashInput);
      assertEquals(unfiltered, hashInput);
    }

    public void testOpenForHashingUsesFilterFactoryBeforeRule()
	throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      createLeaf(url1, "blah <test stream>", null);
      String strRule = "This is a filtered stream";
      mau.setFilterRule(new MyMockFilterRule(new StringReader(strRule)));
      String strFact = "This is a filtered stream";
      mau.setHashFilterFactory(new MyMockFilterFactory(new StringInputStream(strFact)));

      CachedUrl cu = getTestCu(url1);
      InputStream urlIs = cu.openForHashing();
      assertEquals(strFact, StringUtil.fromInputStream(urlIs));
    }

    CIProperties fromArgs(String prop, String val) {
      CIProperties props = new CIProperties();
      props.put(prop, val);
      return props;
    }

    CIProperties fromArgs(String prop1, String val1,
			  String prop2, String val2) {
      CIProperties props = new CIProperties();
      props.put(prop1, val1);
      props.put(prop2, val2);
      return props;
    }

    public void testFilterUsesCharsetOn() throws Exception {
      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_FILTER_USE_CHARSET,
				    "true",
				    PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      createLeaf(url1, "blah1 <test stream>",
		 fromArgs(CachedUrl.PROPERTY_CONTENT_TYPE,
			  "text/html;charset=utf-16be"));
      createLeaf(url2, "blah2 <test stream>",
		 fromArgs(CachedUrl.PROPERTY_CONTENT_TYPE,
			  "text/html"));
      String str = "This is a filtered stream";
      MyMockFilterFactory fact =
	new MyMockFilterFactory(new StringInputStream(str));
      mau.setHashFilterFactory(fact);
      CachedUrl cu1 = getTestCu(url1);
      CachedUrl cu2 = getTestCu(url2);
      assertEquals(str, StringUtil.fromInputStream(cu1.openForHashing()));
      cu2.openForHashing();
      List args = fact.getArgs();
      assertEquals(ListUtil.list(mau, "utf-16be"), args.get(0));
      assertEquals(ListUtil.list(mau, Constants.DEFAULT_ENCODING),
		   args.get(1));
    }

    public void testFilterUsesCharsetOff() throws Exception {
      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_FILTER_USE_CHARSET,
				    "false",
				    PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      createLeaf(url1, "blah1 <test stream>",
		 fromArgs(CachedUrl.PROPERTY_CONTENT_TYPE,
			  "text/html;charset=utf-16be"));
      createLeaf(url2, "blah2 <test stream>",
		 fromArgs(CachedUrl.PROPERTY_CONTENT_TYPE,
			  "text/html"));
      String str = "This is a filtered stream";
      MyMockFilterFactory fact =
	new MyMockFilterFactory(new StringInputStream(str));
      mau.setHashFilterFactory(fact);
      CachedUrl cu1 = getTestCu(url1);
      CachedUrl cu2 = getTestCu(url2);
      assertEquals(str, StringUtil.fromInputStream(cu1.openForHashing()));
      cu2.openForHashing();
      List args = fact.getArgs();
      assertEquals(ListUtil.list(mau, Constants.DEFAULT_ENCODING),
		   args.get(0));
      assertEquals(ListUtil.list(mau, Constants.DEFAULT_ENCODING),
		   args.get(1));
    }

    String randomString(int len) {
      return org.apache.commons.lang3.RandomStringUtils.randomAlphabetic(len);
    }

    public void testFilterReset() throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      String unfilt = randomString(100000); // must be longer than will be buffered
      createLeaf(url1, unfilt, null);
      mau.setHashFilterFactory(new MarkResetFilterFactory(20000, 18000));
      MockMessageDigest md = new MockMessageDigest();
      HashedInputStream.Hasher hasher = new HashedInputStream.Hasher(md);
      CachedUrl cu = getTestCu(url1);
      assertFalse(hasher.isValid());
      InputStream urlIs = cu.openForHashing(hasher);
      // hasher.isValid() not checked yet because it will be true iff
      // filter pre-reads input (which it happens to).
      String result = StringUtil.fromInputStream(urlIs);
      assertEquals(unfilt.substring(0, 18000) + unfilt, result);
      assertTrue(hasher.isValid());
      InputStream hashedStream = new ByteArrayInputStream(md.getUpdatedBytes());
      String hashInput = StringUtil.fromInputStream(hashedStream);
      assertEquals(unfilt, hashInput);
    }

    public void testFilterIllegalReset() throws Exception {
      ConfigurationUtil.addFromArgs(PARAM_SHOULD_FILTER_HASH_STREAM, "true");
      String unfilt = randomString(20000);
      createLeaf(url1, unfilt, null);
      mau.setHashFilterFactory(new MarkResetFilterFactory(5000, 10000));
      MockMessageDigest md = new MockMessageDigest();
      HashedInputStream.Hasher hasher = new HashedInputStream.Hasher(md);
      CachedUrl cu = getTestCu(url1);
      assertFalse(hasher.isValid());
      try {
	InputStream urlIs = cu.openForHashing(hasher);
	fail("reset to invalid mark should throw");
      } catch (LockssUncheckedException e) {
	assertMatchesRE("IOException: Resetting to invalid mark", e.toString());
      }
      assertFalse(hasher.isValid());
    }

    public void testGetContentSize() throws Exception {
      createLeaf(url1, content1, null);
      createLeaf(url2, content2, null);
      createLeaf(url3, "", null);

      CachedUrl cu = getTestCu(url1);
      assertEquals(content1.length(), cu.getContentSize());

      cu = getTestCu(url2);
      assertEquals(content2.length(), cu.getContentSize());

      cu = getTestCu(url3);
      assertEquals(0, cu.getContentSize());
    }

    public void testGetContentType() throws Exception {
      createLeaf(url1, content1, null);
      createLeaf(url2, content2,
		 fromArgs("X-Lockss-content-type", "application/nim"));
      createLeaf(url3, "", fromArgs("Content-Type", "text/ugly"));
      createLeaf(url4, content2,
		 fromArgs("Content-Type", "foo/bar",
			  "X-Lockss-content-type", "bar/foo"));

      assertEquals(null, getTestCu(url1).getContentType());
      assertEquals("application/nim", getTestCu(url2).getContentType());
      assertEquals("text/ugly", getTestCu(url3).getContentType());
      assertEquals("bar/foo", getTestCu(url4).getContentType());
    }

    public void testGetContentTypeCompat() throws Exception {
      ConfigurationUtil.addFromArgs(BaseCachedUrl.PARAM_USE_RAW_CONTENT_TYPE,
				    "false");
      createLeaf(url1, content1, null);
      createLeaf(url2, content2,
		 fromArgs("X-Lockss-content-type", "application/nim"));
      createLeaf(url3, "", fromArgs("Content-Type", "text/ugly"));
      createLeaf(url4, content2,
		 fromArgs("Content-Type", "foo/bar",
			  "X-Lockss-content-type", "bar/foo"));

      assertEquals(null, getTestCu(url1).getContentType());
      assertEquals("application/nim", getTestCu(url2).getContentType());
      assertEquals(null, getTestCu(url3).getContentType());
      assertEquals("bar/foo", getTestCu(url4).getContentType());
    }

    public void testGetProperties() throws Exception {
      CIProperties newProps = new CIProperties();
      newProps.setProperty("test", "value");
      newProps.setProperty("test2", "value2");
      createLeaf(url1, (String)null, newProps);

      CachedUrl cu = getTestCu(url1);
      CIProperties urlProps = cu.getProperties();
      assertEquals("value", urlProps.getProperty("test"));
      assertEquals("value2", urlProps.getProperty("test2"));
    }

    public void notestAddProperty() throws Exception {
      CIProperties newProps = new CIProperties();
      newProps.setProperty("test", "value");
      newProps.setProperty("test2", "value2");
      createLeaf(url1, (String)null, newProps);

      CachedUrl cu = getTestCu(url1);
      cu.addProperty(CachedUrl.PROPERTY_CHECKSUM, "foobar");
      CIProperties urlProps = cu.getProperties();
      assertEquals("value", urlProps.getProperty("test"));
      assertEquals("value2", urlProps.getProperty("test2"));
      assertEquals("foobar", urlProps.getProperty(CachedUrl.PROPERTY_CHECKSUM));

      CachedUrl cu2 = getTestCu(url1);
      CIProperties urlProps2 = cu2.getProperties();
      assertEquals("value", urlProps2.getProperty("test"));
      assertEquals("value2", urlProps2.getProperty("test2"));
      assertEquals("foobar",
		   urlProps2.getProperty(CachedUrl.PROPERTY_CHECKSUM));

      try {
	cu2.addProperty(CachedUrl.PROPERTY_CHECKSUM, "22222");
	fail("2nd attempt to add checksum property should fail");
      } catch (IllegalStateException e) {
      }

      try {
	cu2.addProperty("illegal prop", "123");
	fail("Attempt to add unapproved property should fail");
      } catch (IllegalArgumentException e) {
      }
    }

    public void testOpenForReading() throws Exception {
      createLeaf(url1, content1, null);

      CachedUrl cu = getTestCu(url1);
      Reader reader = cu.openForReading();
      assertEquals(content1, StringUtil.fromReader(reader));
    }

    public void testOpenForReadingCharset(String charset) throws Exception {
      createLeaf(url1, getEncodedInputStream(content1, charset), null);
      CachedUrl cu = getTestCu(url1);
      Reader reader = cu.openForReading();
      assertEquals(content1, StringUtil.fromReader(reader));
    }

    public void testOpenForReadingCharset1() throws Exception {
      testOpenForReadingCharset("UTF-8");
    }
    public void testOpenForReadingCharset2() throws Exception {
      testOpenForReadingCharset("UTF-16");
    }
    public void testOpenForReadingCharset3() throws Exception {
      testOpenForReadingCharset("UTF-32");
    }

  }

  class MyMockArchivalUnit extends MockArchivalUnit {
    public CachedUrlSet makeCachedUrlSet(CachedUrlSetSpec cuss) {
      return new BaseCachedUrlSet(this, cuss);
    }

    public CachedUrl makeCachedUrl(String url) {
      return new InstrumentedBaseCachedUrl(this, url);
    }

    public UrlCacher makeUrlCacher(UrlData ud) {
      return new DefaultUrlCacher(this, ud);
    }
  }

  class MyMockFilterRule
    implements FilterRule {
    Reader reader;

    public MyMockFilterRule(Reader reader) {
      this.reader = reader;
    }

    public Reader createFilteredReader(Reader reader) {
      return this.reader;
    }
  }

  class MyMockFilterFactory implements FilterFactory {
    InputStream in;
    List args = new ArrayList();

    public MyMockFilterFactory(InputStream in) {
      this.in = in;
    }

    public InputStream createFilteredInputStream(ArchivalUnit au,
						 InputStream unfilteredIn,
						 String encoding) {
      args.add(ListUtil.list(au, encoding));
      try {
	// read to EOF
	byte[] buf = new byte[100];
	while (unfilteredIn.read(buf) >= 0);
      } catch (IOException e) {
	fail("threw: " + e);
      }
      return this.in;
    }

    List getArgs() {
      return args;
    }
  }

  static class MarkResetFilterFactory implements FilterFactory {
    int mark;
    int resetAt;

    MarkResetFilterFactory(int mark, int resetAt) {
      this.mark = mark;
      this.resetAt = resetAt;
    }

    public InputStream createFilteredInputStream(ArchivalUnit au,
						 InputStream in,
						 String encoding) {
      in.mark(mark);
      UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream();
      try {
	StreamUtil.copy(in, baos, resetAt);
	in.reset();
	StreamUtil.copy(in, baos);
      } catch (IOException e) {
	throw new LockssUncheckedIOException(e);
      }
      return new ByteArrayInputStream(baos.toByteArray());
    }
  }
  class MyAu extends NullPlugin.ArchivalUnit {
    public FilterRule getFilterRule(String mimeType) {
      return new FilterRule() {
	  public Reader createFilteredReader(Reader reader) {
	    return reader;
	  }
	};
    }
    public FilterFactory getHashFilterFactory(String mimeType) {
      return new FilterFactory() {
	  public InputStream createFilteredInputStream(ArchivalUnit au,
						       InputStream in,
						       String encoding) {
	    return in;
	  }
	};
    }
  }

  protected Artifact storeArt(String url, String content,
			      CIProperties props) throws Exception {
    return storeArt(url, new StringInputStream(content), props);
  }

  protected Artifact storeArt(String url, InputStream in,
			      CIProperties props) throws Exception {
    return V2RepoUtil.storeArt(v2Repo, v2Coll, mau.getAuId(), url, in, props);
  }

  /** Varient that performs the tests when there's only a single version */
  public static class OnlyVersion extends VersionedTests {
    public OnlyVersion() {
    }

    protected void createLeaf(String url, InputStream in,
			      CIProperties props) throws Exception {
      if (props == null) props = new CIProperties();
      storeArt(url, in, props);
    }

    CachedUrl getTestCu(String url) {
      return mau.makeCachedUrl(url);
    }

    public void testVersionNumber() throws Exception {
      createLeaf(url1, content1, null);
      CachedUrl cu = getTestCu(url1);
      assertEquals(1, cu.getVersion());
    }

    public void testGetMultipleStreams() throws Exception {
      doTestGetMultipleStreams();
    }
}

  /** Variant that performs the tests with the current version when there's
   * a previous version */
  public static class CurrentVersion extends VersionedTests {
    public CurrentVersion() {
    }

    protected void createLeaf(String url, InputStream contentStream,
			      CIProperties props) throws Exception {
      if (props == null) props = new CIProperties();
      CIProperties p = new CIProperties();
      p.put("wrongkey", "wrongval");
      storeArt(url, badcontent+"1", p);
      storeArt(url, contentStream, props);
    }

    CachedUrl getTestCu(String url) {
      CachedUrl cu = mau.makeCachedUrl(url);
      return cu;
    }

    public void testVersionNumber() throws Exception {
      createLeaf(url1, content1, null);
      CachedUrl cu = getTestCu(url1);
      CachedUrl curcu = mau.makeCachedUrl(url1);
      assertEquals(2, cu.getVersion());
      assertEquals(2, curcu.getVersion());
      assertEquals("[BCU: http://www.example.com/testDir/leaf1]",
		   curcu.toString());
      assertEquals("[BCU: http://www.example.com/testDir/leaf1]",
		   cu.toString());
    }

    public void testGetMultipleStreams() throws Exception {
      doTestGetMultipleStreams();
    }

    /** Ensure that BaseCachedUrl handles setNeedContent() correctly.  This
     * doesn't test the situation where the repo returns an ArtifactData
     * without content (InputStream), as the LockssRepository
     * implementation used in these tests ignores the includeContent arg,
     * but it does test the case where the ArtifactData has no InputStream
     * because it's been consumed, which is basically the same logic.
     */
    public void testNeedContent() throws Exception {
      createLeaf(url1, "c1", null); createLeaf(url2, "c2", null);
      createLeaf(url3, "c3", null);

      // Default is NeedContent.UNSURE
      InstrumentedBaseCachedUrl ibcu1 =
	(InstrumentedBaseCachedUrl)getTestCu(url1);
      assertEmpty(ibcu1.getIncludeArgs());
      ibcu1.getProperties();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.IF_SMALL),
		   ibcu1.getIncludeArgs());
      ibcu1.getUnfilteredInputStream();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.IF_SMALL),
		   ibcu1.getIncludeArgs());
      ibcu1.getUnfilteredInputStream();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.IF_SMALL,
				 LockssRepository.IncludeContent.ALWAYS),
		   ibcu1.getIncludeArgs());

      InstrumentedBaseCachedUrl ibcu2 =
	(InstrumentedBaseCachedUrl)getTestCu(url2);
      ibcu2.setNeedContent(CachedUrl.NeedContent.NO);
      ibcu2.getProperties();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.NEVER),
		   ibcu2.getIncludeArgs());
      ibcu2.getUnfilteredInputStream();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.NEVER),
		   ibcu2.getIncludeArgs());
      ibcu2.getUnfilteredInputStream();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.NEVER,
				 LockssRepository.IncludeContent.ALWAYS),
		   ibcu2.getIncludeArgs());

      InstrumentedBaseCachedUrl ibcu3 =
	(InstrumentedBaseCachedUrl)getTestCu(url3);
      ibcu3.setNeedContent(CachedUrl.NeedContent.YES);
      ibcu3.getProperties();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.ALWAYS),
		   ibcu3.getIncludeArgs());
      ibcu3.getUnfilteredInputStream();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.ALWAYS),
		   ibcu3.getIncludeArgs());
      ibcu3.getUnfilteredInputStream();
      assertEquals(ListUtil.list(LockssRepository.IncludeContent.ALWAYS,
				 LockssRepository.IncludeContent.ALWAYS),
		   ibcu3.getIncludeArgs());
    }

  }

  /** Varient that performs the tests with version 2 of 3 versions */
  public static class PreviousVersion extends VersionedTests {
    public PreviousVersion() {
    }

    protected void createLeaf(String url, InputStream contentStream,
			      CIProperties props) throws Exception {
      if (props == null) props = new CIProperties();
      CIProperties p = new CIProperties();
      p.put("wrongkey", "wrongval");
      storeArt(url, badcontent+"1", p);
      storeArt(url, contentStream, props);
      storeArt(url, badcontent+"3", p);
    }

    CachedUrl getTestCu(String url) {
      CachedUrl cu = mau.makeCachedUrl(url);
      CachedUrl[] all = cu.getCuVersions();
      assertEquals(3, all.length);

      return all[1];
    }

    public void testVersionNumber() throws Exception {
      createLeaf(url1, content1, null);
      CachedUrl cu = getTestCu(url1);
      CachedUrl curcu = mau.makeCachedUrl(url1);
      assertEquals(2, cu.getVersion());
      assertEquals(3, curcu.getVersion());
      assertEquals("[BCU: http://www.example.com/testDir/leaf1]",
		   curcu.toString());
      assertEquals("[BCU: v=2 http://www.example.com/testDir/leaf1]",
		   cu.toString());
    }

    public void testGetCuVersion() throws Exception {
      createLeaf(url1, content1, null);
      CachedUrl cu = mau.makeCachedUrl(url1);
      assertEquals(1, cu.getCuVersion(1).getVersion());
      assertEquals(2, cu.getCuVersion(2).getVersion());
      assertEquals(3, cu.getCuVersion(3).getVersion());
      CachedUrl noncu = cu.getCuVersion(4);
      assertEquals(4, noncu.getVersion());
      try {
	noncu.getContentSize();
	fail("No version 4, getContentSize() should throw");
      } catch (UnsupportedOperationException e) { }
      try {
	noncu.getUnfilteredInputStream();
	fail("No version 4, getUnfilteredInputStream() should throw");
      } catch (UnsupportedOperationException e) { }
    }

    public void testGetCuVersions() throws Exception {
      createLeaf(url1, content1, null);
      CachedUrl cu = mau.makeCachedUrl(url1);
      CachedUrl[] all = cu.getCuVersions();
      assertEquals(3, all.length);
      assertEquals(3, all[0].getVersion());
      assertEquals(2, all[1].getVersion());
      assertEquals(1, all[2].getVersion());
    }

    public void testGetCuVersionsMax() throws Exception {
      createLeaf(url1, content1, null);
      CachedUrl cu = mau.makeCachedUrl(url1);
      CachedUrl[] all = cu.getCuVersions(2);
      assertEquals(2, all.length);
      assertEquals(3, all[0].getVersion());
      assertEquals(2, all[1].getVersion());
    }



  }

  public static Test suite() {
    return variantSuites(new Class[] {NotVersionedTests.class,
				      OnlyVersion.class,
				      CurrentVersion.class,
				      PreviousVersion.class,
				      });
  }

}
