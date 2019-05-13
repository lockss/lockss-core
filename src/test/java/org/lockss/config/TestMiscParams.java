/*

 Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

import java.util.*;
import org.junit.*;
import org.lockss.plugin.*;
import org.lockss.daemon.*;
import org.lockss.test.*;
import org.lockss.util.*;

public class TestMiscParams extends LockssTestCase4 {

  MiscParams misc;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    misc = getMockLockssDaemon().getManagerByType(MiscParams.class);
  }

  @Test
  public void testIsGloballyExcludedUrl() {
    assertFalse(misc.isGloballyExcludedUrl(null, null));
    assertFalse(misc.isGloballyExcludedUrl(null, "http://random.string/"));
    assertFalse(misc.isGloballyExcludedUrl(null, "http://http://http://"));

    ConfigurationUtil.addFromArgs(MiscParams.PARAM_EXCLUDE_URL_PATTERN,
				  "(http:.*){3,}");
    assertTrue(misc.isGloballyExcludedUrl(null, "http://http://http://"));
    assertFalse(misc.isGloballyExcludedUrl(null, "http://http://https://"));
    assertFalse(misc.isGloballyExcludedUrl(null, "www.x.www.y.www.z"));
    assertFalse(misc.isGloballyExcludedUrl(null, "www.x.www.y.www.z.www."));

    ConfigurationUtil.addFromArgs(MiscParams.PARAM_EXCLUDE_URL_PATTERN,
				  "((http:.*){3,})|((\\bwww\\..*){4,})");
    assertTrue(misc.isGloballyExcludedUrl(null, "http://http://http://"));
    assertFalse(misc.isGloballyExcludedUrl(null, "http://http://https://"));
    assertFalse(misc.isGloballyExcludedUrl(null, "www.x.www.y.www.z"));
    assertTrue(misc.isGloballyExcludedUrl(null, "www.x.www.y.www.z.www."));

    ConfigurationUtil.addFromArgs(MiscParams.PARAM_EXCLUDE_URL_PATTERN,
				  "http:/[^/]");
    assertTrue(misc.isGloballyExcludedUrl(null,
					  "http://example.com/foo/(http:/www.foo/bar.html"));
    assertTrue(misc.isGloballyExcludedUrl(null,
					  "http://example.com/foo/http:/www.foo/bar.html"));
    assertFalse(misc.isGloballyExcludedUrl(null, "http://http://https://"));
    assertFalse(misc.isGloballyExcludedUrl(null, "www.x.www.y.www.z"));

    ConfigurationUtil.addFromArgs(MiscParams.PARAM_EXCLUDE_URL_PATTERN,
				  "(http:/[^/])|((reports/most-read/){2,})");
    assertTrue(misc.isGloballyExcludedUrl(null,
					  "http://example.com/foo/(http:/www.foo/bar.html"));
    assertTrue(misc.isGloballyExcludedUrl(null,
					  "http://example.com/foo/http:/www.foo/bar.html"));
    assertFalse(misc.isGloballyExcludedUrl(null, "http://http://https://"));
    assertFalse(misc.isGloballyExcludedUrl(null, "www.x.www.y.www.z"));

    assertTrue(misc.isGloballyExcludedUrl(null, "http://bjo.bmj.com/content/93/2/176.abstract/reports/most-read/reports/most-read/reports/most-read/reports/most-read/reply"));
  }

}
