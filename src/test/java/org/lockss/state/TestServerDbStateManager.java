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

import java.io.IOException;
import java.util.Map;

import org.apache.activemq.broker.BrokerService;
import org.junit.*;
import org.lockss.jms.*;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.*;
import org.lockss.test.*;
import org.lockss.util.MapUtil;
import org.lockss.util.time.*;

public class TestServerDbStateManager extends LockssTestCase4 {
  L4JLogger log = L4JLogger.getLogger();

  static BrokerService broker;

  MockLockssDaemon daemon;
  PluginManager pluginMgr;
  MyServerDbStateManager stateMgr;
  MockPlugin mplug;
  MockArchivalUnit mau1;
  MockArchivalUnit mau2;
  Consumer cons;

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
    setUpDiskSpace();
    daemon = getMockLockssDaemon();
    pluginMgr = daemon.getPluginManager();
//     pluginMgr.startService();
    daemon.getManagerByType(org.lockss.config.db.ConfigDbManager.class)
      .startService();

    stateMgr = daemon.setUpStateManager(new MyServerDbStateManager());
    mplug = new MockPlugin(daemon);

    mau1 = new MockArchivalUnit(mplug, "plug&aaa1");
    mau2 = new MockArchivalUnit(mplug, "plug&aaa2");
    
    cons = Consumer.createTopicConsumer(null, BaseStateManager.DEFAULT_JMS_NOTIFICATION_TOPIC);
  }

  // Construct a JMS message map for an AuState update
  Map<String,Object> auStateUpdateMap(AuState aus, Map<String,Object> map)
      throws IOException {
    return auStateUpdateMap(aus.getArchivalUnit().getAuId(), map);
  }

  // Construct a JMS message map for an AuState update
  Map<String,Object> auStateUpdateMap(String key, Map<String,Object> map)
      throws IOException {
    return MapUtil.map("name", "AuState",
		       "auid", key,
		       "json", AuUtil.mapToJson(map));
  }

  @Test
  // Test that local updates of AuState objects cause notifications to be
  // sent, and that updates from service cause local AuState to be updated
  public void testAuState() throws Exception {
    AuState aus1 = AuUtil.getAuState(mau1);
    AuState aus2 = AuUtil.getAuState(mau2);
    assertNotSame(aus1, aus2);
    assertSame(aus1, AuUtil.getAuState(mau1));
    assertSame(aus2, AuUtil.getAuState(mau2));
    assertEquals(-1, aus1.getLastMetadataIndex());
    TimeBase.setSimulated(333444);
    aus1.setLastMetadataIndex(123);
    aus2.newCrawlFinished(3, "Outcome", 124);
    assertEquals(123, aus1.getLastMetadataIndex());
    assertEquals("Outcome", aus2.getLastCrawlResultMsg());
    assertEquals(333444, aus2.getLastCrawlTime());
    
    assertEquals(auStateUpdateMap(aus1, MapUtil.map("lastMetadataIndex", 123)),
		 cons.receiveMap(TIMEOUT_SHOULDNT));
    assertEquals(auStateUpdateMap(aus2, MapUtil.map("lastCrawlTime", 333444,
						    "lastCrawlAttempt", -1,
						    "lastCrawlResultMsg", "Outcome",
						    "lastCrawlResult", 3)),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    Map in1Map = MapUtil.map("lastCrawlTime", 666,
			     "lastCrawlAttempt", 555,
			     "lastCrawlResultMsg", "Success",
			     "lastCrawlResult", 1);
    stateMgr.updateAuStateFromService(stateMgr.auKey(mau2),
				      AuUtil.mapToJson(in1Map));

    assertEquals("Success", aus2.getLastCrawlResultMsg());
    assertEquals(666, aus2.getLastCrawlTime());
  }

  @Test
  // Test that service updates when only AuStateBean exists cause
  // notifications to be sent
  public void testAuStateBeanUpdate() throws Exception {

    String key = "plug&aukey1";
    AuStateBean ausb;

    Map in1Map = MapUtil.map("lastCrawlTime", 666,
			     "lastCrawlAttempt", 555,
			     "lastCrawlResultMsg", "Success",
			     "lastCrawlResult", 1);
    stateMgr.updateAuStateFromService(key,
				      AuUtil.mapToJson(in1Map));

    assertEquals(auStateUpdateMap(key, in1Map),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    assertTrue(stateMgr.isInAuStateBeanMap(key));

    // Create an AU with that auid, ensure resulting AuState has the values
    // that were set on the AuStateBean created by the update

    MockArchivalUnit mau = new MockArchivalUnit(mplug, key);
    AuState aus = AuUtil.getAuState(mau);
    assertEquals("Success", aus.getLastCrawlResultMsg());
    assertEquals(1, aus.getLastCrawlResult());
    assertEquals(666, aus.getLastCrawlTime());
    assertEquals(555, aus.getLastCrawlAttempt());

    assertTrue(stateMgr.isInAuStateMap(key));
  }


  static class MyServerDbStateManager extends ServerDbStateManager {
    private SimpleBinarySemaphore rcvSem;

    // Suppress DB load
    @Override
    protected AuStateBean doLoadAuStateBean(String key) {
      log.debug2("MyServerDbStateManager.doLoadAuState({})", key);
      return null;
    }

    boolean isInAuStateMap(String key) {
      if (!auStates.containsKey(key)) {
	return false;
      }
      if (auStateBeans.containsKey(key)) {
	throw new IllegalStateException("Key found in both auStates and auStateBeans: " + key);
      }
      return true;
    }

    boolean isInAuStateBeanMap(String key) {
      if (!auStateBeans.containsKey(key)) {
	return false;
      }
      if (auStates.containsKey(key)) {
	throw new IllegalStateException("Key found in both auStates and auStateBeans: " + key);
      }
      return true;
    }
  }
}
