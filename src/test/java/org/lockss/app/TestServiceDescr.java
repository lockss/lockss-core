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

package org.lockss.app;

import org.lockss.test.*;
import org.lockss.util.*;

public class TestServiceDescr extends LockssTestCase {
  private final static Logger log = Logger.getLogger();

  public void testIll() {
    try {
      new ServiceDescr(null, "abbrev");
      fail("Shouldn't be able to create ServiceDescr with null name");
    } catch (IllegalArgumentException e) {
    }
    try {
      new ServiceDescr("name", null);
      fail("Shouldn't be able to create ServiceDescr with null abbrev");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testEqualsHash() {
    ServiceDescr sd1 = new ServiceDescr("name", "abbrev");
    ServiceDescr sd2 = new ServiceDescr("name2", "abbrev");
    ServiceDescr sd3 = new ServiceDescr("name", "abbrev2");
    assertEquals(new ServiceDescr("name", "abbrev"), sd1);
    assertEquals(new ServiceDescr("name", "abbrev").hashCode(), sd1.hashCode());

    assertNotEquals(new ServiceDescr("name2", "abbrev"), sd1);
    assertNotEquals(new ServiceDescr("name", "abbrev2"), sd1);
  }

  public void testBuiltin() {
    assertEquals(new ServiceDescr("Config Service", "cfg"),
		 ServiceDescr.SVC_CONFIG);
    assertEquals(new ServiceDescr("Metadata Extraction Service", "mdx"),
		 ServiceDescr.SVC_MDX);
    assertEquals(new ServiceDescr("Metadata Query Service", "mdq"),
		 ServiceDescr.SVC_MDQ);
    assertEquals(new ServiceDescr("Poller Service", "poller"),
		 ServiceDescr.SVC_POLLER);
    assertEquals(new ServiceDescr("Crawler Service", "crawler"),
		 ServiceDescr.SVC_CRAWLER);
    assertEquals(new ServiceDescr("Repository Service", "repo"),
		 ServiceDescr.SVC_REPO);

    assertEquals(new ServiceDescr("Config Service", "cfg"),
		 ServiceDescr.fromAbbrev("cfg"));
    assertEquals(new ServiceDescr("Metadata Extraction Service", "mdx"),
		 ServiceDescr.fromAbbrev("mdx"));
    assertEquals(new ServiceDescr("Metadata Query Service", "mdq"),
		 ServiceDescr.fromAbbrev("mdq"));
    assertEquals(new ServiceDescr("Poller Service", "poller"),
		 ServiceDescr.fromAbbrev("poller"));
    assertEquals(new ServiceDescr("Crawler Service", "crawler"),
		 ServiceDescr.fromAbbrev("crawler"));
    assertEquals(new ServiceDescr("Repository Service", "repo"),
		 ServiceDescr.fromAbbrev("repo"));

    try {
      ServiceDescr.register(new ServiceDescr("Not Repository Service", "repo"));
      fail("Shouldn't be able to register a conflicting ServiceDescr");
    } catch (IllegalStateException e) {
    }

  }
}

