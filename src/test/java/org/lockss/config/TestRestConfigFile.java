/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

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
package org.lockss.config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lockss.config.TestConfigManager.MyConfigManager;
import org.lockss.test.LockssTestCase4;
import org.springframework.web.util.UriUtils;

/**
 * Test class for <code>org.lockss.config.RestConfigFile</code>
 */
public class TestRestConfigFile extends LockssTestCase4 {

  ConfigManager mgr;
  String restLocationUrl = "http://rest";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    mgr = MyConfigManager.makeConfigManager();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testIsRestConfigUrl() throws Exception {
    String parent = "http://parent";
    ConfigFile cf = new FileConfigFile(parent, mgr);
    mgr.parentConfigFile.put("http://notRest", cf);
    mgr.parentConfigFile.put("http://isRest", cf);
    ConfigFile restCf = new RestConfigFile(restLocationUrl, mgr);
    mgr.parentConfigFile.put("http://isRestChild", restCf);

    assertFalse(RestConfigFile.isRestConfigUrl(null, mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://abc", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://notRest", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl(restLocationUrl, mgr));
    assertFalse(RestConfigFile.isRestConfigUrl(parent, mgr));
    assertFalse(RestConfigFile.isRestConfigUrl(parent + "/def", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://isRest", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://isRest/xyz", mgr));
    assertTrue(RestConfigFile.isRestConfigUrl("http://isRestChild", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://isRestChild/123", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl(restLocationUrl + "/a1", mgr));

    mgr.restConfigClient = new RestConfigClient(restLocationUrl);

    try {
      assertFalse(RestConfigFile.isRestConfigUrl(null, mgr));
      fail("RestConfigFile.isRestConfigUrl() should throw for a null URL");
    } catch (NullPointerException npe) {
    }

    assertFalse(RestConfigFile.isRestConfigUrl("", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://abc", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://notRest", mgr));
    assertTrue(RestConfigFile.isRestConfigUrl(restLocationUrl, mgr));
    assertFalse(RestConfigFile.isRestConfigUrl(parent, mgr));
    assertFalse(RestConfigFile.isRestConfigUrl(parent + "/def", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://isRest", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://isRest/xyz", mgr));
    assertTrue(RestConfigFile.isRestConfigUrl("http://isRestChild", mgr));
    assertFalse(RestConfigFile.isRestConfigUrl("http://isRestChild/123", mgr));
    assertTrue(RestConfigFile.isRestConfigUrl(restLocationUrl + "/a1", mgr));
  }

  @Test
  public void testRestConfigFile() throws Exception {
    RestConfigFile rcf = null;

    try {
      rcf = new RestConfigFile(null, mgr);
      fail("RestConfigFile() should throw for a null URL");
    } catch (NullPointerException npe) {}

    try {
      rcf = new RestConfigFile("", mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf = new RestConfigFile("a", mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf = new RestConfigFile("/a", mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    String url = "http://xyz.org/xpath/x.xml";

    try {
      rcf = new RestConfigFile(url, null);
      fail("RestConfigFile() should throw for a null ConfigManager");
    } catch (RuntimeException re) {}

    rcf = new RestConfigFile(url, mgr);
    assertEquals(url, rcf.getRequestUrl());

    mgr.restConfigClient = new RestConfigClient(restLocationUrl);

    try {
      rcf = new RestConfigFile(null, mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (NullPointerException npe) {}

    try {
      rcf = new RestConfigFile("", mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf = new RestConfigFile("a", mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf = new RestConfigFile("/a", mgr);
      fail("RestConfigFile() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    rcf = new RestConfigFile(url, mgr);
    assertEquals(restLocationUrl + "/config/url?url="
	+ UriUtils.encodePathSegment(url, "UTF-8"), rcf.getRequestUrl());
  }

  @Test
  public void testGetRequestUrl() throws Exception {
    RestConfigFile rcf = new RestConfigFile(restLocationUrl, mgr);
    assertEquals(restLocationUrl, rcf.getRequestUrl());
  }


  @Test
  public void testResolveConfigUrl() {
    String fbase = "./config/common.xml";
    String stem = "http://props.lockss.org/";
    String hbase = stem +"path/lockss.xml";
    RestConfigFile rcf = new RestConfigFile("http://localhost:54420/config/url?url=http%3A%2F%2Fprops.lockss.org%2Flockss%2Flockss.xml", mgr);
    assertEquals("http://localhost:54420/config/url?url=http://props.lockss.org/lockss/tdb.xml",
		 rcf.resolveConfigUrl("tdb.xml"));

    rcf = new RestConfigFile("http://localhost:54420/config/file/cluster", mgr);
    assertEquals("http://localhost:54420/config/url?url=config/common.xml",
		 rcf.resolveConfigUrl("config/common.xml"));
  }

  @Test
  public void testRedirectAbsoluteUrl() throws Exception {
    RestConfigFile rcf = new RestConfigFile(restLocationUrl, mgr);

    try {
      rcf.redirectAbsoluteUrl(null);
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf.redirectAbsoluteUrl("");
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf.redirectAbsoluteUrl("a");
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf.redirectAbsoluteUrl("/a");
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    String url = "http://xyz.org/xpath/x.xml";
    assertEquals(url, rcf.redirectAbsoluteUrl(url));

    mgr.restConfigClient = new RestConfigClient(restLocationUrl);
    rcf = new RestConfigFile(restLocationUrl, mgr);

    try {
      rcf.redirectAbsoluteUrl(null);
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf.redirectAbsoluteUrl("");
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf.redirectAbsoluteUrl("a");
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    try {
      rcf.redirectAbsoluteUrl("/a");
      fail("redirectAbsoluteUrl() should throw for a not-absolute URL");
    } catch (IllegalArgumentException iae) {}

    assertEquals("http://rest/config/url?url=http:%2F%2Fxyz.org%2Fxpath%2Fx.xml",
		 rcf.redirectAbsoluteUrl(url));
  }
}
