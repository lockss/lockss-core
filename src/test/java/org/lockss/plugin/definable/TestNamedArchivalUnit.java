/*

Copyright (c) 2000-2023 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.plugin.definable;

import java.util.*;
import org.apache.commons.collections4.*;

import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.test.*;
import org.lockss.plugin.base.*;
import org.lockss.plugin.*;
import org.lockss.repository.*;
import org.lockss.util.*;
import org.lockss.state.*;
import org.lockss.util.rest.repo.LockssRepository;

/**
 * Test class for NamedArchivalUnit
 */
public class TestNamedArchivalUnit extends LockssTestCase {

  private static final Logger log = Logger.getLogger();
  private static final String plugName = NamedArchivalUnit.NAMED_PLUGIN_NAME;

  private Plugin plugin;
  private MockLockssDaemon daemon;
  private PluginManager mgr;
  private Plugin plug;
  private LockssRepository v2Repo;
  private String v2Coll;

  public void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();
    // make and init a real Pluginmgr
    mgr = daemon.getPluginManager();
    daemon.setDaemonInited(true);
    String plugKey = PluginManager.pluginKeyFromId(plugName);
    mgr.ensurePluginLoaded(plugKey);
    plug = mgr.getPlugin(plugKey);
    useV2Repo();
    RepositoryManager repomgr = daemon.getRepositoryManager();
    v2Repo = repomgr.getV2Repository().getRepository();
    v2Coll = repomgr.getV2Repository().getCollection();
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  String randomString(int len) {
    return org.apache.commons.lang3.RandomStringUtils.randomAlphabetic(len);
  }

  public void testNamedAu() throws  Exception {
    Configuration auConfig = ConfigurationUtil.fromArgs("handle", "foo");
    ArchivalUnit au = PluginTestUtil.createAu(plug, auConfig);
    assertTrue(au.isNamedArchivalUnit());
//     assertTrue(au.getPlugin().isNamedPlugin());
    assertEquals(ConfigurationUtil.fromArgs("handle", "foo"),
                 au.getConfiguration());
    assertEquals("Named AU: foo", au.getName());
    assertEmpty(au.getStartUrls());
    assertEmpty(au.getUrlStems());
    AuState aus = AuUtil.getAuState(au);
    assertFalse(au.shouldCrawlForNewContent(aus));
    assertTrue(au.shouldBeCached(""));
    assertTrue(au.shouldBeCached("asdfsd"));
    assertTrue(au.shouldBeCached("http://nor.mal/url"));
    assertTrue(au.shouldBeCached("\u092A\u0936"));

    String[] names = { "http://foo.bar/baz", "nonURL.1",
                       "nonURL.2", "worse URL", "nonURL.2" };
    for (String name : names) {
      // Ensure unique content so 2nd version gets created for
      // repeated name
      String cont = "Content of " + name + "\n" + randomString(8);
      UrlData ud = new UrlData(new StringInputStream(cont),
                               new CIProperties(), name);
      UrlCacher uc = new DefaultUrlCacher(au, ud);
      uc.storeContent();
      CachedUrl cu = au.makeCachedUrl(name);
      assertTrue(cu.hasContent());
      assertEquals(name, cu.getUrl());
      assertInputStreamMatchesString(cont,
                                   cu.getUnfilteredInputStream());
    }
    CachedUrl cu = au.makeCachedUrl(names[2]);
    assertEquals(2, cu.getVersion());
  }

  public void testFeatureCrawledAu() throws  Exception {
    Configuration auConfig =
      ConfigurationUtil.fromArgs("handle", "foo", "features", "walked;crawledAu");
    ArchivalUnit au = PluginTestUtil.createAu(plug, auConfig);
    assertTrue(au.isNamedArchivalUnit());
    assertEquals(ConfigurationUtil.fromArgs("handle", "foo",
                                            "features", "walked;crawledAu"),
                 au.getConfiguration());
    assertEquals("Named AU: foo", au.getName());
    assertEmpty(au.getStartUrls());
    AuState aus = AuUtil.getAuState(au);
    assertTrue(au.shouldCrawlForNewContent(aus));
    assertTrue(au.shouldBeCached(""));
    assertTrue(au.shouldBeCached("asdfsd"));
    assertTrue(au.shouldBeCached("http://nor.mal/url"));
    assertTrue(au.shouldBeCached("\u092A\u0936"));

    String[] names = { "http://foo.bar/baz", "nonURL.1",
                       "nonURL.2", "worse URL", "nonURL.2" };
    for (String name : names) {
      // Ensure unique content so 2nd version gets created for
      // repeated name
      String cont = "Content of " + name + "\n" + randomString(8);
      UrlData ud = new UrlData(new StringInputStream(cont),
                               new CIProperties(), name);
      UrlCacher uc = new DefaultUrlCacher(au, ud);
      uc.storeContent();
      CachedUrl cu = au.makeCachedUrl(name);
      assertTrue(cu.hasContent());
      assertEquals(name, cu.getUrl());
      assertInputStreamMatchesString(cont,
                                   cu.getUnfilteredInputStream());
    }
    CachedUrl cu = au.makeCachedUrl(names[2]);
    assertEquals(2, cu.getVersion());
  }

  // Test the non-def params that make the AU browseable
  public void testBrowseable() throws  Exception {
    Configuration auConfig =
      ConfigurationUtil.fromArgs("handle", "foo");
    ArchivalUnit au = PluginTestUtil.createAu(plug, auConfig);
    assertTrue(au.isNamedArchivalUnit());
    assertEquals(auConfig, au.getConfiguration());
    assertEquals("Named AU: foo", au.getName());
    assertEmpty(au.getStartUrls());
    assertEmpty(au.getUrlStems());
    TypedEntryMap tem = au.getProperties();
    assertEquals(2, tem.size());
    assertEquals("Named AU: foo", tem.getString("au_title", null));
    assertEquals("foo", tem.getString("handle", null));

    // Reconfigure AU, ensure everything changes correctly
    Configuration auConfig2 =
      ConfigurationUtil.fromArgs("handle", "foo",
                                 "start_urls", "http://foo.com/one;http://foo.com/two",
                                 "url_stems", "http://other.host/");

    au.setConfiguration(auConfig2);
    assertEquals(ListUtil.list("http://foo.com/one", "http://foo.com/two"),
                 au.getStartUrls());
    assertEquals(SetUtil.set("http://foo.com/", "http://other.host/"),
                 au.getUrlStems());

    TypedEntryMap tem2 = au.getProperties();
    assertEquals(4, tem2.size());
    assertEquals("Named AU: foo", tem2.getString("au_title", null));
    assertEquals("http://foo.com/one;http://foo.com/two",
                 tem2.getString("start_urls", null));
    assertEquals("http://other.host/",
                 tem2.getString("url_stems", null));
  }

  // Test unicode handle
  public void testUnicodeNamedAu() throws  Exception {
    String uniHandle = "\u092A\u0936\u0941\u092A\u0924\u093F\u0930\u092A\u093F \u0924\u093E\u0928\u094D\u092F\u0939\u093E\u0928\u093F \u0915\u0943\u091A\u094D\u091B\u094D\u0930\u093E\u0926\u094D"; // पशुपतिरपि तान्यहानि कृच्छ्राद् (from the Sanskrit poem Kumāra-saṃbhava)
    Configuration auConfig = ConfigurationUtil.fromArgs("handle", uniHandle);
    ArchivalUnit au = PluginTestUtil.createAu(plug, auConfig);
    assertTrue(au.isNamedArchivalUnit());
    assertEquals(ConfigurationUtil.fromArgs("handle", uniHandle),
                 au.getConfiguration());
    assertEquals("Named AU: " + uniHandle, au.getName());

    String[] names = { "http://foo.bar/baz", "nonURL.1",
                       "nonURL.2", "worse URL", "nonURL.2" };
    for (String name : names) {
      // Ensure unique content so 2nd version gets created for
      // repeated name
      String cont = "Content of " + name + "\n" + randomString(8);
      UrlData ud = new UrlData(new StringInputStream(cont),
                               new CIProperties(), name);
      UrlCacher uc = new DefaultUrlCacher(au, ud);
      uc.storeContent();
      CachedUrl cu = au.makeCachedUrl(name);
      assertTrue(cu.hasContent());
      assertEquals(name, cu.getUrl());
      assertInputStreamMatchesString(cont,
                                   cu.getUnfilteredInputStream());
    }
    CachedUrl cu = au.makeCachedUrl(names[2]);
    assertEquals(2, cu.getVersion());

    assertIsomorphic(ListUtil.list(v2Coll),
                     IterableUtils.toList(v2Repo.getNamespaces()));
    assertIsomorphic(ListUtil.list(PluginManager.generateAuId(plug, auConfig)),
                     IterableUtils.toList(v2Repo.getAuIds(v2Coll)));
  }

}
