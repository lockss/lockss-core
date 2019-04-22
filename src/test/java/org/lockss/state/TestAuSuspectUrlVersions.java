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
package org.lockss.state;

import java.util.*;
import org.junit.*;

import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

public class TestAuSuspectUrlVersions extends StateTestCase {
  L4JLogger log = L4JLogger.getLogger();

  @Override
  protected StateManager makeStateManager() {
    return new InMemoryStateManager();
  }

  @Test
  public void testAll() throws Exception {
    AuSuspectUrlVersions asuv1 = AuSuspectUrlVersions.make(AUID1);
    AuSuspectUrlVersions asuv2 = AuSuspectUrlVersions.make(AUID1);
    assertTrue(asuv1.isEmpty());
    assertEquals(asuv1, asuv2);
    assertFalse(asuv1.isSuspect(URL1, 2));

    TimeBase.setSimulated(123);
    long now1 = TimeBase.nowMs();
    asuv1.markAsSuspect(URL1, 1, null, null);
    TimeBase.step(100);
    long now2 = TimeBase.nowMs();
    assertFalse(asuv1.isEmpty());
    assertTrue(asuv1.isSuspect(URL1, 1));
    assertFalse(asuv1.isSuspect(URL1, 2));

    assertNotEquals(asuv1, asuv2);
    asuv2.markAsSuspect(URL1, 1, null, null);
    assertEquals(asuv1, asuv2);

    SuspectUrlVersion suv1 = asuv1.getSuspectUrlVersion(URL1, 1);
    assertNotNull(suv1);
    assertEquals(URL1, suv1.getUrl());
    assertEquals(1, suv1.getVersion());
    assertEquals(now1, suv1.getCreated());
    assertEquals(null, suv1.getStoredHash());
    assertEquals(null, suv1.getComputedHash());

    asuv1.markAsSuspect(URL2, 2, HASH1, HASH2);
    assertNull(asuv1.getSuspectUrlVersion(URL2, 1));
    SuspectUrlVersion suv2 = asuv1.getSuspectUrlVersion(URL2, 2);
    assertNotNull(suv2);
    assertEquals(URL2, suv2.getUrl());
    assertEquals(2, suv2.getVersion());
    assertEquals(now2, suv2.getCreated());
    assertEquals(HASH1, suv2.getComputedHash());
    assertEquals(HASH2, suv2.getStoredHash());

    assertNotEquals(asuv1, asuv2);
    asuv2.markAsSuspect(URL2, 2, HASH2, HASH1);
    // equals() compares only the url and version
    assertEquals(asuv1, asuv2);

    asuv2.markAsSuspect(URL2, 4, HASH1, HASH2);
    assertNotEquals(asuv1, asuv2);
    asuv2.unmarkAsSuspect(URL2, 4);
    assertEquals(asuv1, asuv2);

    String json = asuv1.toJson();
    log.debug2("json: " + json);
    assertMatchesRE("\"auid\":\"" + RegexpUtil.quotemeta(AUID1), json);
    assertMatchesRE("\"url\":\"" + RegexpUtil.quotemeta(URL1), json);
    assertMatchesRE("\"url\":\"" + RegexpUtil.quotemeta(URL2), json);
    assertMatchesRE("\"version\":1", json);
    assertMatchesRE("\"version\":2", json);

    AuSuspectUrlVersions asuv3 = AuSuspectUrlVersions.make(AUID1);
    assertNotEquals(asuv3, asuv1);
    asuv3.updateFromJson(json, getMockLockssDaemon());
    assertEquals(asuv3, asuv1);

    SuspectUrlVersion suv3 = asuv1.getSuspectUrlVersion(URL2, 2);
    assertNotNull(suv3);
    assertEquals(URL2, suv3.getUrl());
    assertEquals(2, suv3.getVersion());
    assertEquals(now2, suv3.getCreated());
    assertEquals(HASH1, suv3.getComputedHash());
    assertEquals(HASH2, suv3.getStoredHash());

  }

}
