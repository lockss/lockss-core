/*

Copyright (c) 2000-2022 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.plugin;

import java.util.*;

import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.test.*;
import org.lockss.plugin.base.*;
import org.lockss.util.*;
import org.lockss.state.*;

/**
 * Test class for NamedArchivalUnit
 */
public class TestNamedArchivalUnit extends LockssTestCase {

  private static final Logger log = Logger.getLogger();
  private static final String plugName = "org.lockss.plugin.NamedPlugin";

  private Plugin plugin;
  private MockLockssDaemon daemon;
  private PluginManager mgr;
  private Plugin plug;

  public void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();
    // make and init a real Pluginmgr
    mgr = daemon.getPluginManager();
    daemon.setDaemonInited(true);
    String plugKey = PluginManager.pluginKeyFromName(plugName);
    mgr.ensurePluginLoaded(plugKey);
    plug = mgr.getPlugin(plugKey);
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  String randomString(int len) {
    return org.apache.commons.lang3.RandomStringUtils.randomAlphabetic(len);
  }

  public void testNamedAu() throws  Exception {
    Configuration auConfig = ConfigurationUtil.fromArgs("handle", "foo");
    ArchivalUnit au =
      mgr.createAu(plug, auConfig, AuEvent.model(AuEvent.Type.Create));
    assertTrue(au.isNamedArchivalUnit());
//     assertTrue(au.getPlugin().isNamedPlugin());
    assertEquals(ConfigurationUtil.fromArgs("handle", "foo"),
                 au.getConfiguration());
    assertEquals("Named AU: foo", au.getName());
    assertEmpty(au.getStartUrls());
    AuState aus = AuUtil.getAuState(au);
    assertFalse(au.shouldCrawlForNewContent(aus));

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

}
