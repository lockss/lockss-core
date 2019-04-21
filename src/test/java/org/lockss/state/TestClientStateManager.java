/*

Copyright (c) 2000-2018 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.*;
import java.util.*;
import org.junit.*;
import org.apache.activemq.broker.BrokerService;

import org.lockss.app.*;
import org.lockss.daemon.Crawler;
import org.lockss.plugin.*;
import org.lockss.poller.v3.V3Poller;
import org.lockss.poller.v3.V3Poller.PollVariant;
import org.lockss.protocol.*;
import static org.lockss.protocol.AgreementType.*;
import org.lockss.test.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.jms.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.time.TimerUtil;
import org.lockss.state.AuSuspectUrlVersions.SuspectUrlVersion;

public class TestClientStateManager extends StateTestCase {
  L4JLogger log = L4JLogger.getLogger();

  static BrokerService broker;

  MyClientStateManager myStateMgr;

  MockPlugin mplug;
  Producer prod;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    broker = JMSManager.createBroker(JMSManager.DEFAULT_BROKER_URI);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (broker != null) {
      TimerUtil.sleep(1000);
      broker.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    daemon = getMockLockssDaemon();
    pluginMgr = daemon.getPluginManager();
//     pluginMgr.startService();

    mplug = new MockPlugin(daemon);

    prod = Producer.createTopicProducer(null, BaseStateManager.DEFAULT_JMS_NOTIFICATION_TOPIC);
  }

  @Override
  protected StateManager makeStateManager() {
    myStateMgr = new MyClientStateManager();
    return myStateMgr;
  }

  // Construct a JMS message map for an AuState update
  Map<String,Object> auStateUpdateMap(AuState aus, Map<String,Object> map)
      throws IOException {
    return MapUtil.map("name", "AuState",
		       "auid", aus.getArchivalUnit().getAuId(),
		       "json", AuUtil.mapToJson(map));
  }

  // Construct a JMS message map for an AuAgreements update
  Map<String,Object> auAgreementsUpdateMap(String auid, String json)
      throws IOException {
    return MapUtil.map("name", "AuAgreements",
		       "auid", auid,
		       "json", json);
  }

  // Construct a JMS message map for an AuSuspectUrlVersions update
  Map<String,Object> auSuspectUrlVersionsUpdateMap(String auid, String json)
      throws IOException {
    return MapUtil.map("name", "AuSuspectUrlVersions",
		       "auid", auid,
		       "json", json);
  }

  @Test
  public void testReceiveAuStateNotification() throws Exception {
    SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
    myStateMgr.setRcvSem(sem);

    AuState aus1 = stateMgr.getAuState(mau1);
    AuState aus2 = stateMgr.getAuState(mau2);
    assertNotSame(aus1, aus2);
    assertSame(aus1, stateMgr.getAuState(mau1));
    assertSame(aus2, stateMgr.getAuState(mau2));
    assertEquals(-1, aus1.getLastMetadataIndex());
    aus1.setLastMetadataIndex(123);
    aus2.setLastMetadataIndex(321);
    assertFalse(sem.take(TIMEOUT_SHOULD));

    prod.sendMap(auStateUpdateMap(aus1, MapUtil.map("lastMetadataIndex",
						    1234565)));
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
    assertEquals(1234565, aus1.getLastMetadataIndex());
    assertEquals(321, aus2.getLastMetadataIndex());
    assertSame(aus1, stateMgr.getAuState(mau1));

    // ensure that non-existent field is ignored, doesn't cause error
    prod.sendMap(auStateUpdateMap(aus2, MapUtil.map("lastMetadataIndex", 12,
						    "lastCrawlResultMsg", "crawl fail",
						    "no_field", "val")));
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
    assertEquals(1234565, aus1.getLastMetadataIndex());
    assertEquals(12, aus2.getLastMetadataIndex());
    assertEquals("crawl fail", aus2.getLastCrawlResultMsg());
  }

  @Test
  public void testReceiveAuAgreementsNotification() throws Exception {
    SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
    myStateMgr.setRcvSem(sem);

    AuAgreements aua1 = stateMgr.getAuAgreements(AUID1);
    AuAgreements aua2 = stateMgr.getAuAgreements(AUID2);
    assertNotSame(aua1, aua2);
    assertAgreeTime(-1.0f, 0, aua1.findPeerAgreement(pid1, POR));
    assertSame(aua1, stateMgr.getAuAgreements(AUID1));
    assertSame(aua2, stateMgr.getAuAgreements(AUID2));

    aua1.signalPartialAgreement(pid0, POR, .9f, 800);
    aua1.signalPartialAgreement(pid0, POP, .7f, 800);

    aua1.signalPartialAgreement(pid1, POR, .25f, 900);
    aua1.signalPartialAgreement(pid1, POP, .50f, 900);

    String json = aua1.toJson(SetUtil.set(pid1));

    aua1.signalPartialAgreement(pid0, POR, .10f, 910);
    aua1.signalPartialAgreement(pid0, POP, .20f, 910);

    aua1.signalPartialAgreement(pid1, POR, .30f, 920);
    aua1.signalPartialAgreement(pid1, POP, .40f, 920);

    assertFalse(sem.take(TIMEOUT_SHOULD));

    assertAgreeTime(.10f, 910, aua1.findPeerAgreement(pid0, POR));
    assertAgreeTime(.20f, 910, aua1.findPeerAgreement(pid0, POP));
    assertAgreeTime(.30f, 920, aua1.findPeerAgreement(pid1, POR));
    assertAgreeTime(.40f, 920, aua1.findPeerAgreement(pid1, POP));

    prod.sendMap(auAgreementsUpdateMap(AUID1, json));
    assertTrue(sem.take(TIMEOUT_SHOULDNT));

    assertAgreeTime(.10f, 910, aua1.findPeerAgreement(pid0, POR));
    assertAgreeTime(.20f, 910, aua1.findPeerAgreement(pid0, POP));
    assertAgreeTime(.25f, 900, aua1.findPeerAgreement(pid1, POR));
    assertAgreeTime(.50f, 900, aua1.findPeerAgreement(pid1, POP));

    assertSame(aua1, stateMgr.getAuAgreements(AUID1));

  }

  @Test
  public void testReceiveAuSuspectUrlVersionsNotification() throws Exception {
    SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
    myStateMgr.setRcvSem(sem);

    AuSuspectUrlVersions asuv1 = stateMgr.getAuSuspectUrlVersions(AUID1);
    AuSuspectUrlVersions asuv2 = stateMgr.getAuSuspectUrlVersions(AUID2);
    assertNotSame(asuv1, asuv2);
    assertTrue(asuv1.isEmpty());
    assertFalse(asuv1.isSuspect(URL1, 1));
    assertSame(asuv1, stateMgr.getAuSuspectUrlVersions(AUID1));
    assertSame(asuv2, stateMgr.getAuSuspectUrlVersions(AUID2));

    asuv1.markAsSuspect(URL1, 1, HASH1, HASH2);
    asuv1.markAsSuspect(URL2, 2, HASH2, HASH1);
    String json = asuv1.toJson();

    assertFalse(sem.take(TIMEOUT_SHOULD));

    assertTrue(asuv1.isSuspect(URL1, 1));
    assertTrue(asuv1.isSuspect(URL2, 2));
    assertFalse(asuv1.isSuspect(URL1, 2));
    assertFalse(asuv1.isSuspect(URL2, 1));

    asuv1.unmarkAsSuspect(URL1, 1);
    asuv1.unmarkAsSuspect(URL2, 2);
    assertFalse(asuv1.isSuspect(URL1, 1));
    assertFalse(asuv1.isSuspect(URL2, 2));
    assertTrue(asuv1.isEmpty());

    prod.sendMap(auSuspectUrlVersionsUpdateMap(AUID1, json));
    assertTrue(sem.take(TIMEOUT_SHOULDNT));

    assertTrue(asuv1.isSuspect(URL1, 1));
    assertTrue(asuv1.isSuspect(URL2, 2));
    assertFalse(asuv1.isSuspect(URL1, 2));
    assertFalse(asuv1.isSuspect(URL2, 1));

    assertSame(asuv1, stateMgr.getAuSuspectUrlVersions(AUID1));

  }


  static class MyClientStateManager extends ClientStateManager {
    private SimpleBinarySemaphore rcvSem;

    public void setRcvSem(SimpleBinarySemaphore sem) {
      this.rcvSem = sem;
    }


    // /////////////////////////////////////////////////////////////////
    // AuState
    // /////////////////////////////////////////////////////////////////

    @Override
    protected AuStateBean doLoadAuStateBean(String key) {
      log.debug2("MyClientStateManager.doLoadAuStateBean");
      return null;
    }

    @Override
    protected void doStoreAuStateBean(String key, AuStateBean ausb,
				      Set<String> fields) {
    }

    @Override
    public synchronized void doReceiveAuStateChanged(String auid, String json,
						     String cookie) {
      super.doReceiveAuStateChanged(auid, json, cookie);
      if (rcvSem != null) rcvSem.give();
    }

    // /////////////////////////////////////////////////////////////////
    // AuAgreements
    // /////////////////////////////////////////////////////////////////

    @Override
    protected AuAgreements doLoadAuAgreements(String key) {
      log.debug2("MyClientStateManager.doLoadAuAgreements");
      return null;
    }

    @Override
    protected void doStoreAuAgreementsUpdate(String key, AuAgreements aus,
					     Set<PeerIdentity> peers) {
    }

    @Override
    public synchronized void doReceiveAuAgreementsChanged(String auid,
							  String json,
							  String cookie) {
      super.doReceiveAuAgreementsChanged(auid, json, cookie);
      if (rcvSem != null) rcvSem.give();
    }

    // /////////////////////////////////////////////////////////////////
    // AuSuspectUrlVersions
    // /////////////////////////////////////////////////////////////////

    @Override
    protected AuSuspectUrlVersions doLoadAuSuspectUrlVersions(String key) {
      log.debug2("MyClientStateManager.doLoadAuSuspectUrlVersions");
      return null;
    }

    @Override
    protected void doStoreAuSuspectUrlVersionsUpdate(String key,
						     AuSuspectUrlVersions aus,
						     Set<SuspectUrlVersion> versions) {
    }

    @Override
    public synchronized void doReceiveAuSuspectUrlVersionsChanged(String auid,
								  String json,
								  String cookie) {
      super.doReceiveAuSuspectUrlVersionsChanged(auid, json, cookie);
      if (rcvSem != null) rcvSem.give();
    }

  }

}
