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
package org.lockss.protocol;

import java.util.*;
import org.junit.*;

import org.lockss.state.*;
import org.lockss.log.*;
import org.lockss.util.*;

public class TestDatedPeerIdSetImpl extends StateTestCase {
  L4JLogger log = L4JLogger.getLogger();

  @Override
  protected StateManager makeStateManager() {
    return new InMemoryStateManager();
  }

  @Test
  public void testBasicOps() throws Exception {
    DatedPeerIdSet dpis1 = DatedPeerIdSetImpl.make(AUID1, idMgr);
    DatedPeerIdSet dpis2 = DatedPeerIdSetImpl.make(AUID1, idMgr);
    assertEquals(-1, dpis1.getDate());
    assertTrue(dpis1.isEmpty());
    assertFalse(dpis1.contains(pid0));
    assertEquals(dpis1, dpis2);

    dpis1.add(pid0);
    assertEquals(-1, dpis1.getDate());
    assertFalse(dpis1.isEmpty());
    assertTrue(dpis1.contains(pid0));

    assertNotEquals(dpis1, dpis2);
    dpis2.add(pid0);
    assertEquals(dpis1, dpis2);

    dpis1.setDate(222333444);
    assertEquals(222333444, dpis1.getDate());
    assertFalse(dpis1.isEmpty());
    assertTrue(dpis1.contains(pid0));
    assertNotEquals(dpis1, dpis2);
    dpis2.setDate(222333444);
    assertEquals(dpis1, dpis2);
  }

  @Test
  public void testToJson() throws Exception {
    DatedPeerIdSet dpis1 = DatedPeerIdSetImpl.make(AUID1, idMgr);
    DatedPeerIdSet dpis2 = DatedPeerIdSetImpl.make(AUID1, idMgr);
    dpis1.add(pid0);
    dpis1.add(pid1);
    dpis1.setDate(9876);
    String json = dpis1.toJson();
    log.debug2("json: " + json);
    assertMatchesRE("\"auid\":\"" + RegexpUtil.quotemeta(AUID1), json);
    assertMatchesRE("\"rawSet\":", json);
    assertMatchesRE("\"" + RegexpUtil.quotemeta(pid0.getIdString()) + "\"",
		    json);
    assertMatchesRE("\"" + RegexpUtil.quotemeta(pid1.getIdString()) + "\"",
		    json);
    assertMatchesRE("\"date\":9876", json);

    assertNotEquals(dpis2, dpis1);
    dpis2.updateFromJson(json, getMockLockssDaemon());
    assertEquals(dpis2, dpis1);
    dpis1.remove(pid0);
    assertNotEquals(dpis2, dpis1);
    dpis2.updateFromJson(dpis1.toJson(), getMockLockssDaemon());
    assertEquals(dpis2, dpis1);
  }
}
