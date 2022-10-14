/*

Copyright (c) 2000-2021 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.repository;

import org.junit.*;
import org.lockss.log.*;
import org.lockss.test.*;

public class TestRepoSpec extends LockssTestCase4 {
  static L4JLogger log = L4JLogger.getLogger();

  @Test
  public void testParse() {
    RepoSpec rs1 = RepoSpec.fromSpec("volatile:ns1");
    log.info("rs1: {}", rs1);
    assertEquals("volatile", rs1.getType());
    assertEquals("ns1", rs1.getNamespace());

    RepoSpec rs2 = RepoSpec.fromSpec("local:ns_2:/path/to/it");
    log.info("rs2: {}", rs2);
    assertEquals("local", rs2.getType());
    assertEquals("ns_2", rs2.getNamespace());
    assertEquals("/path/to/it", rs2.getPath());

    RepoSpec rs3 = RepoSpec.fromSpec("rest:lockss:http://lockss-repository-service:24610");
    log.info("rs3: {}", rs3);
    assertEquals("rest", rs3.getType());
    assertEquals("lockss", rs3.getNamespace());
    assertEquals("http://lockss-repository-service:24610", rs3.getUrl());
  }

  @Test
  public void testIll() {
    try {
      RepoSpec.fromSpec("noparse");
    } catch (IllegalArgumentException e) {
      assertMatchesRE("parse", e.getMessage());
    }

    try {
      RepoSpec.fromSpec("notype:ns");
    } catch (IllegalArgumentException e) {
      assertMatchesRE("unknown type", e.getMessage());
    }

    try {
      RepoSpec.fromSpec("rest:nopath");
    } catch (IllegalArgumentException e) {
      assertMatchesRE("no URL", e.getMessage());
    }
  }


}
