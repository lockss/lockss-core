/*

Copyright (c) 2000-2017 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.config;

import java.io.*;
import org.junit.Before;
import org.junit.Test;
import org.lockss.config.ConfigCache;
import org.lockss.util.test.FileTestUtil;
import org.lockss.test.*;
import org.lockss.util.*;

/**
 * Test class for <code>org.lockss.config.ConfigCache</code>
 */

public class TestConfigCache extends LockssTestCase4 {
  static Logger log = Logger.getLogger();

  private static final String config1 =
    "prop.1=foo\n" +
    "prop.2=bar\n" +
    "prop.3=baz";

  private static final String config2 =
    "prop.4=foo\n" +
    "prop.5=bar\n" +
    "prop.6=baz";

  private static final String config3 =
    "<lockss-config>\n" +
    "  <property name=\"prop.7\" value=\"foo\" />\n" +
    "  <property name=\"prop.8\" value=\"bar\" />\n" +
    "  <property name=\"prop.9\" value=\"baz\" />\n" +
    "</lockss-config>";

  ConfigCache cache;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    cache = new ConfigCache(new ConfigManager(null, "http://localhost:1234",
	null, null));
  }

  /*
   * Test methods.
   */
  @Test
  public void testFind() throws IOException {
    String url = null;

    url = FileTestUtil.urlOfString(config1);
    ConfigFile cf = cache.find(url);
    assertNotNull("ConfigFile should not be null", cf);
    assertSame(cf, cache.find(url));
    assertSame(cf, cache.get(url));

    url = "http://localhost:1234/rcf1";
    cf = cache.find(url);
    assertNotNull("ConfigFile should not be null", cf);
    assertTrue(cf instanceof RestConfigFile);
    assertSame(cf, cache.find(url));
    assertSame(cf, cache.get(url));
  }

  @Test
  public void testFindResource() throws IOException {
    ConfigFile cf = cache.find("resource:foo");
    assertClass(ResourceConfigFile.class, cf);
    assertEquals("resource:foo", cf.getFileUrl());
  }

  @Test
  public void testSize() throws IOException {
    assertEquals(0, cache.size());

    try {
      String url1 = FileTestUtil.urlOfString(config1);
      String url2 = FileTestUtil.urlOfString(config2);
      String url3 = FileTestUtil.urlOfString(config3, ".xml");
      cache.find(url1);
      cache.find(url2);
      cache.find(url3);
      cache.find("http://localhost:1234/rcf1");
    } catch (IOException ex) {
      fail("Unable to load config file: " + ex);
    }

    assertEquals(4, cache.size());
  }
}
