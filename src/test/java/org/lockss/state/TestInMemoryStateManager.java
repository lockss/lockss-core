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

import org.junit.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import static org.lockss.protocol.AgreementType.*;
import org.lockss.test.*;

public class TestInMemoryStateManager extends StateTestCase {

  InMemoryStateManager myStateMgr;

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected StateManager makeStateManager() {
    myStateMgr = new InMemoryStateManager();
    return myStateMgr;
  }

  @Test
  public void testAuState() {
    AuState aus1 = stateMgr.getAuState(mau1);
    AuState aus2 = stateMgr.getAuState(mau2);
    assertNotSame(aus1, aus2);
    assertSame(aus1, stateMgr.getAuState(mau1));
    assertSame(aus2, stateMgr.getAuState(mau2));
    assertEquals(-1, aus1.getLastMetadataIndex());
    aus1.setLastMetadataIndex(123);
    aus2.setLastMetadataIndex(321);
    assertEquals(123, aus1.getLastMetadataIndex());
    assertEquals(321, aus2.getLastMetadataIndex());
    assertSame(aus1, stateMgr.getAuState(mau1));

    // after deactivate event, getAuState() should return a new instance
    // with the same values as the old instance
    auEvent(mau1, AuEvent.Type.Deactivate);
    AuState aus1b = stateMgr.getAuState(mau1);
    assertNotSame(aus1, aus1b);
    assertEquals(123, aus1b.getLastMetadataIndex());
  }

  @Test
  public void testAuAgreements() {
    AuAgreements aua1 = stateMgr.getAuAgreements(AUID1);
    AuAgreements aua2 = stateMgr.getAuAgreements(AUID2);
    assertNotSame(aua1, aua2);
    assertSame(aua1, stateMgr.getAuAgreements(AUID1));
    assertSame(aua2, stateMgr.getAuAgreements(AUID2));
    assertAgreeTime(-1.0f, 0, aua1.findPeerAgreement(pid1, POP));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid1, POP));
    aua1.signalPartialAgreement(pid1, POP, .6f, 400);
    assertAgreeTime(0.6f, 400, aua1.findPeerAgreement(pid1, POP));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid1, POP));
    assertSame(aua1, stateMgr.getAuAgreements(AUID1));

    // after deactivate event, getAuAgreements() should return a new instance
    // with the same values as the old instance
    auEvent(mau1, AuEvent.Type.Deactivate);
    AuAgreements aua1b = stateMgr.getAuAgreements(AUID1);
    assertNotSame(aua1, aua1b);
    assertAgreeTime(0.6f, 400, aua1.findPeerAgreement(pid1, POP));
  }

  void auEvent(ArchivalUnit au, AuEvent.Type type) {
    pluginMgr.signalAuEvent(au, AuEvent.forAu(au, type));
  }
}
