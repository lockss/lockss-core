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
import org.lockss.repository.AuSuspectUrlVersions;
import org.lockss.test.*;
import org.lockss.log.*;
import org.lockss.util.*;
import org.lockss.jms.*;
import org.lockss.util.io.LockssSerializable;
import org.lockss.util.time.TimerUtil;

public class TestClientStateManager extends LockssTestCase4 {
  L4JLogger log = L4JLogger.getLogger();

  static BrokerService broker;

  MockLockssDaemon daemon;
  PluginManager pluginMgr;
  MyClientStateManager stateMgr;
  MockPlugin mplug;
  MockArchivalUnit mau1;
  MockArchivalUnit mau2;
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

    stateMgr = daemon.setUpStateManager(new MyClientStateManager());
    mplug = new MockPlugin(daemon);
    mau1 = new MockArchivalUnit(mplug, "aaa1");
    mau2 = new MockArchivalUnit(mplug, "aaa2");

    prod = Producer.createTopicProducer(null, BaseStateManager.DEFAULT_JMS_NOTIFICATION_TOPIC);
  }

  // Construct a JMS message map for an AuState update
  Map<String,Object> auStateUpdateMap(AuState aus, Map<String,Object> map)
      throws IOException {
    return MapUtil.map("name", "AuState",
		       "auid", aus.getArchivalUnit().getAuId(),
		       "json", AuUtil.mapToJson(map));
  }

  @Test
  public void testReceiveNotification() throws Exception {
    SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
    stateMgr.setRcvSem(sem);

    AuState aus1 = AuUtil.getAuState(mau1);
    AuState aus2 = AuUtil.getAuState(mau2);
    assertNotSame(aus1, aus2);
    assertSame(aus1, AuUtil.getAuState(mau1));
    assertSame(aus2, AuUtil.getAuState(mau2));
    assertEquals(-1, aus1.getLastMetadataIndex());
    aus1.setLastMetadataIndex(123);
    aus2.setLastMetadataIndex(321);
    assertFalse(sem.take(TIMEOUT_SHOULD));

    prod.sendMap(auStateUpdateMap(aus1, MapUtil.map("lastMetadataIndex",
						    1234565)));
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
    assertEquals(1234565, aus1.getLastMetadataIndex());
    assertEquals(321, aus2.getLastMetadataIndex());
    assertSame(aus1, AuUtil.getAuState(mau1));

    // ensure that non-existent field is ignored, doesn't cause error
    prod.sendMap(auStateUpdateMap(aus2, MapUtil.map("lastMetadataIndex", 12,
						    "lastCrawlResultMsg", "crawl fail",
						    "no_field", "val")));
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
    assertEquals(1234565, aus1.getLastMetadataIndex());
    assertEquals(12, aus2.getLastMetadataIndex());
    assertEquals("crawl fail", aus2.getLastCrawlResultMsg());
  }


  static class MyClientStateManager extends ClientStateManager {
    private SimpleBinarySemaphore rcvSem;

    protected AuState doLoadAuState(ArchivalUnit au) {
      log.debug2("MyClientStateManager.doLoadAuState");
      return null;
    }

    protected void doStoreAuStateUpdate(String key, AuState aus,
					String json, Map<String,Object> map) {
    }

    public synchronized void doReceiveAuStateChanged(String auid, String json) {
      super.doReceiveAuStateChanged(auid, json);
      if (rcvSem != null) rcvSem.give();
    }

    public void setRcvSem(SimpleBinarySemaphore sem) {
      this.rcvSem = sem;
    }
  }

}
