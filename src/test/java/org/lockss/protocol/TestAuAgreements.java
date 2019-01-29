/*

Copyright (c) 2013-2019 Board of Trustees of Leland Stanford Jr. University,
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


import java.io.*;
import java.util.*;
import org.apache.commons.io.FileUtils;

import org.lockss.util.*;
import org.lockss.util.test.FileTestUtil;
import org.lockss.plugin.*;
import org.lockss.test.*;


public class TestAuAgreements extends LockssTestCase {

  List<PeerIdentity> peerIdentityList;
  IdentityManager idMgr;
  PeerIdentity pid0, pid1;
  String AUID = "Plugin&auauau";

  public void setUp() throws Exception {
    super.setUp();
    MockLockssDaemon daemon = getMockLockssDaemon();
    idMgr = daemon.getIdentityManager();
    peerIdentityList = ListUtil.list(daemon.findPeerIdentity("127.0.0.0"),
				     daemon.findPeerIdentity("127.0.0.1"),
				     daemon.findPeerIdentity("127.0.0.2"));
    pid0 = peerIdentityList.get(0);
    pid1 = peerIdentityList.get(1);
  }

  AuAgreements makeAua() {
    return AuAgreements.make(AUID, idMgr);
  }

  public void testEquals() throws Exception {
    AuAgreements aua1 = makeAua();
    signalPartialAgreements(aua1, AgreementType.POR,
			    50.0f, 100);
    signalPartialAgreements(aua1, AgreementType.POR,
			    40.0f, 200);

    AuAgreements aua2 = makeAua();
    assertNotEquals(aua1, aua2);
    signalPartialAgreements(aua2, AgreementType.POR,
			    50.0f, 100);
    assertNotEquals(aua1, aua2);
    signalPartialAgreements(aua2, AgreementType.POR,
			    40.0f, 200);
    assertEquals(aua1, aua2);
    assertEquals(aua1.hashCode(), aua2.hashCode());
    signalPartialAgreements(aua2, AgreementType.POR,
			    40.0f, 300);
    assertNotEquals(aua1, aua2);
  }


  public void testToJson() throws Exception {
    // Create an AuAgreements object and put some agreements in it.
    AuAgreements aua1 = makeAua();
    signalPartialAgreements(aua1, AgreementType.POR,
			    50.0f, 100);
    signalPartialAgreements(aua1, AgreementType.POR,
			    40.0f, 200);

    checkPercentAgreements(aua1, AgreementType.POR,
			   40.0f, 200);
    checkHighestPercentAgreements(aua1, AgreementType.POR,
				  50.0f, 100);
    String json = aua1.toJson();
    log.debug2("json: " + json);
    assertMatchesRE("\"id\":\"127.0.0.0\"", json);
    assertMatchesRE("\"id\":\"127.0.0.1\"", json);
    assertMatchesRE("\"id\":\"127.0.0.2\"", json);

    // write only pid1 data
    String jsonp1 = aua1.toJson(pid1);
    assertNotMatchesRE("\"id\":\"127.0.0.0\"", jsonp1);
    assertMatchesRE("\"id\":\"127.0.0.1\"", jsonp1);
    assertNotMatchesRE("\"id\":\"127.0.0.2\"", jsonp1);
    
    AuAgreements aua2 = makeAua();
    assertNotEquals(aua1, aua2);
    aua2.updateFromJson(json, getMockLockssDaemon());
    log.debug2("loaded AuAgreements: " + aua2);
    assertEquals(aua1, aua2);

    AuAgreements aua3 = makeAua();
    signalPartialAgreements(aua3, AgreementType.POR,
			    80.0f, 400);
    String json3 = aua3.toJson(pid1);
    aua1.updateFromJson(json3, getMockLockssDaemon());
    assertNotEquals(aua1, aua2);

    aua2.signalPartialAgreement(pid1, AgreementType.POR, (80.0f+1)/100, 400+1);

    assertEquals(aua1, aua2);

  }


  public void testMakeWithAuAgreements() throws Exception {
    // Create an AuAgreements object and put some agreements in it.
    AuAgreements auAgreements = makeAua();
    signalPartialAgreements(auAgreements, AgreementType.POR,
			    50.0f, 100);
    signalPartialAgreements(auAgreements, AgreementType.POR,
			    40.0f, 200);

    checkPercentAgreements(auAgreements, AgreementType.POR,
			   40.0f, 200);
    checkHighestPercentAgreements(auAgreements, AgreementType.POR,
				  50.0f, 100);

    String json = auAgreements.toJson();

    // Load the saved AuAgreements into a new instance.
    AuAgreements auAgreementsLoad = makeAua();
    assertNotSame(auAgreements, auAgreementsLoad);
    auAgreements.updateFromJson(json, getMockLockssDaemon());

    assertTrue(auAgreements.haveAgreements());
    // Check that they are as expected.
    checkPercentAgreements(auAgreements, AgreementType.POR,
			   40.0f, 200);
    checkHighestPercentAgreements(auAgreements, AgreementType.POR,
				  50.0f, 100);
  }

  public void testHaveAgreements() {
    AuAgreements auAgreements = makeAua();

    assertFalse(auAgreements.haveAgreements());

    PeerIdentity pid = peerIdentityList.get(0);
    auAgreements.signalPartialAgreement(pid, AgreementType.POR_HINT, 0.5f, 100);

    assertTrue(auAgreements.haveAgreements());
  }

  public void testSignalPartialAgreement() {
    AuAgreements auAgreements;
    PeerIdentity pid = peerIdentityList.get(0);
    auAgreements = makeAua();

    int i = 0;
    for (AgreementType type: AgreementType.values()) {
      auAgreements.signalPartialAgreement(pid, type, (50.0f+i)/100, 100+i);
      auAgreements.signalPartialAgreement(pid, type, (40.0f+i)/100, 200+i);
      i++;
    }

    i = 0;
    for (AgreementType type: AgreementType.values()) {
      PeerAgreement agreement = auAgreements.findPeerAgreement(pid, type);
      assertEquals((40.0f+i)/100, agreement.getPercentAgreement());
      assertEquals(200+i, agreement.getPercentAgreementTime());
      assertEquals((50.0f+i)/100, agreement.getHighestPercentAgreement());
      assertEquals(100+i, agreement.getHighestPercentAgreementTime());
      i++;
    }
  }

  public void testFindPeerAgreement() {
    AuAgreements auAgreements = makeAua();
    PeerIdentity pid = peerIdentityList.get(0);

    PeerAgreement peerAgreement = 
      auAgreements.findPeerAgreement(pid, AgreementType.POR_HINT);
    assertEquals(PeerAgreement.NO_AGREEMENT, peerAgreement);

    auAgreements.signalPartialAgreement(pid, AgreementType.POR_HINT, 0.5f, 100);

    peerAgreement = auAgreements.findPeerAgreement(pid, AgreementType.POR_HINT);
    assertEquals(100, peerAgreement.getPercentAgreementTime());
  }

  public void testHasAgreed() {
    // Create AuAgreements object empty.
    AuAgreements auAgreements = makeAua();
    PeerIdentity pid = peerIdentityList.get(0);
    assertFalse(auAgreements.hasAgreed(pid, 0.0f));
    // in fact, its value is -1.0f
    assertFalse(auAgreements.hasAgreed(pid, -0.5f));

    // signal 0.0, and anything at or under is true, above false.
    auAgreements.signalPartialAgreement(pid, AgreementType.POR,
					  0.0f, 10);
    assertTrue(auAgreements.hasAgreed(pid, 0.0f));
    assertFalse(auAgreements.hasAgreed(pid, 0.00001f));

    // signal 0.5, and anything at or under is true, above false.
    auAgreements.signalPartialAgreement(pid, AgreementType.POR,
					  0.5f, 10);
    assertTrue(auAgreements.hasAgreed(pid, 0.3f));
    assertTrue(auAgreements.hasAgreed(pid, 0.5f));
    assertFalse(auAgreements.hasAgreed(pid, 0.500001f));
  }

  public void testGetAgreements() {
    AuAgreements auAgreements = makeAua();
    auAgreements.signalPartialAgreement(peerIdentityList.get(1),
					AgreementType.POR_HINT, 0.1f, 101);
    auAgreements.signalPartialAgreement(peerIdentityList.get(0),
					AgreementType.POR_HINT, 0.0f, 100);
    auAgreements.signalPartialAgreement(peerIdentityList.get(2),
					AgreementType.POR_HINT, 0.2f, 102);

    auAgreements.signalPartialAgreement(peerIdentityList.get(1),
					AgreementType.POR, 0.1f, 201);
    auAgreements.signalPartialAgreement(peerIdentityList.get(0),
					AgreementType.POR, 0.0f, 200);

    Map<PeerIdentity, PeerAgreement> map;
    map = auAgreements.getAgreements(AgreementType.POR_HINT);
    assertEquals(3, map.size());
    for (int i = 0; i < 3; i++) {
      PeerIdentity pid = peerIdentityList.get(i);
      PeerAgreement peerAgreement = map.get(pid);
      assertNotNull(peerAgreement);
      assertEquals(100+i, peerAgreement.getPercentAgreementTime());
    }
    assertEquals(3, auAgreements.countAgreements(AgreementType.POR_HINT, 0.0f));
    assertEquals(2, auAgreements.countAgreements(AgreementType.POR_HINT, 0.1f));
    assertEquals(1, auAgreements.countAgreements(AgreementType.POR_HINT, 0.2f));
    assertEquals(0, auAgreements.countAgreements(AgreementType.POR_HINT, 0.3f));

    map = auAgreements.getAgreements(AgreementType.POR);
    assertEquals(2, map.size());
    for (int i = 0; i < 2; i++) {
      PeerIdentity pid = peerIdentityList.get(i);
      PeerAgreement peerAgreement = map.get(pid);
      assertNotNull(peerAgreement);
      assertEquals(200+i, peerAgreement.getPercentAgreementTime());
    }
    assertEquals(2, auAgreements.countAgreements(AgreementType.POR, 0.0f));
    assertEquals(1, auAgreements.countAgreements(AgreementType.POR, 0.1f));
    assertEquals(0, auAgreements.countAgreements(AgreementType.POR, 0.2f));

    map = auAgreements.getAgreements(AgreementType.SYMMETRIC_POR);
    assertTrue(map.isEmpty());
  }

  // Signal a sterotyped set of partial agreements
  private void signalPartialAgreements(AuAgreements auAgreements,
				       AgreementType type,
				       float percent, long time) {

    for (int i = 0; i < peerIdentityList.size(); i++) {
      PeerIdentity pid = peerIdentityList.get(i);

      auAgreements.
	signalPartialAgreement(pid, type, (percent+i)/100, time+i);
    }
  }
  
  // Check the sterotyped set of percent agreements
  private void checkPercentAgreements(AuAgreements auAgreements,
				      AgreementType type,
				      float percent, long time) {
    for (int i = 0; i < peerIdentityList.size(); i++) {
      PeerIdentity pid = peerIdentityList.get(i);
      PeerAgreement peerAgreement = 
	auAgreements.findPeerAgreement(pid, type);
      assertEquals((percent+i)/100, peerAgreement.getPercentAgreement());
      assertEquals(time+i, peerAgreement.getPercentAgreementTime());
    }
  }
  
  // Check the sterotyped set of highest percent agreements
  private void checkHighestPercentAgreements(AuAgreements auAgreements,
					     AgreementType type,
					     float percent, long time) {
    for (int i = 0; i < peerIdentityList.size(); i++) {
      PeerIdentity pid = peerIdentityList.get(i);
      PeerAgreement peerAgreement = 
	auAgreements.findPeerAgreement(pid, type);
      assertEquals((percent+i)/100, peerAgreement.getHighestPercentAgreement());
      assertEquals(time+i, peerAgreement.getHighestPercentAgreementTime());
    }
  }

  // Check that there are no agreements of the given type
  private void checkAgreementsMissing(AuAgreements auAgreements,
				      AgreementType type) {
    for (PeerIdentity pid : peerIdentityList) {
      PeerAgreement peerAgreement = 
	auAgreements.findPeerAgreement(pid, type);
      assertEquals(PeerAgreement.NO_AGREEMENT, peerAgreement);
    }
  }
}
