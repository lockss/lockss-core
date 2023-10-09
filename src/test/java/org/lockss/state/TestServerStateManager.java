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
import java.util.*;

import org.apache.activemq.broker.BrokerService;
import org.junit.*;
import org.lockss.account.BasicUserAccount;
import org.lockss.account.UserAccount;
import org.lockss.jms.*;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import static org.lockss.protocol.AgreementType.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.jms.*;
import org.lockss.util.time.TimerUtil;

public class TestServerStateManager extends StateTestCase {
  L4JLogger log = L4JLogger.getLogger();

  static BrokerService broker;

  MyServerStateManager myStateMgr;
  MockPlugin mplug;
  JmsConsumer cons;

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
    daemon.getManagerByType(org.lockss.config.db.ConfigDbManager.class)
      .startService();

    mplug = new MockPlugin(daemon);

    mau1 = new MockArchivalUnit(mplug, "plug&aaa1");
    mau2 = new MockArchivalUnit(mplug, "plug&aaa2");
    
    cons = JMSManager.getJmsFactoryStatic().createTopicConsumer(null, BaseStateManager.DEFAULT_JMS_NOTIFICATION_TOPIC);
  }

  @Override
  protected StateManager makeStateManager() {
    myStateMgr = new MyServerStateManager();
    return myStateMgr;
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

  Map<String,Object> userAccountUpdateMap(String username,
                                          String json,
                                          String op,
                                          String cookie) throws IOException {

    Map<String,Object> map = MapUtil.map("name", "UserAccount",
        "username", username,
        "userAccountChange", op,
        "json", json);

    putNotNull(map, "cookie", cookie);

    return map;
  }

  protected void putNotNull(Map map, String key, String val) {
    if (val != null) {
      map.put(key, val);
    }
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
    stateMgr.updateAuStateFromJson(stateMgr.auKey(mau2),
				   AuUtil.mapToJson(in1Map), null);

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
    stateMgr.updateAuStateFromJson(key, AuUtil.mapToJson(in1Map), "Bath Oliver");

    Map outmap = auStateUpdateMap(key, in1Map);
    outmap.put("cookie", "Bath Oliver");
    assertEquals(outmap, cons.receiveMap(TIMEOUT_SHOULDNT));

    assertTrue(myStateMgr.isInAuStateBeanMap(key));

    // Create an AU with that auid, ensure resulting AuState has the values
    // that were set on the AuStateBean created by the update

    MockArchivalUnit mau = new MockArchivalUnit(mplug, key);
    AuState aus = AuUtil.getAuState(mau);
    assertEquals("Success", aus.getLastCrawlResultMsg());
    assertEquals(1, aus.getLastCrawlResult());
    assertEquals(666, aus.getLastCrawlTime());
    assertEquals(555, aus.getLastCrawlAttempt());

    assertTrue(myStateMgr.isInAuStateMap(key));
  }

  @Test
  // Test that local updates of NoAuPeerSet objects cause notifications to be
  // sent, and that updates from service cause local NoAuPeerSet to be updated
  public void testNoAuPeerSet() throws Exception {
    String auid1 = mau1.getAuId();
    DatedPeerIdSetImpl naps1 = (DatedPeerIdSetImpl)AuUtil.getNoAuPeerSet(mau1);
    DatedPeerIdSetImpl naps2 = (DatedPeerIdSetImpl)AuUtil.getNoAuPeerSet(mau2);
    assertNotSame(naps1, naps2);
    assertSame(naps1, AuUtil.getNoAuPeerSet(mau1));
    assertSame(naps2, AuUtil.getNoAuPeerSet(mau2));
    assertTrue(naps1.isEmpty());
    naps1.add(pid1);
    naps1.add(pid2);
    assertTrue(naps1.contains(pid2));
    TimeBase.setSimulated(333444);
    stateMgr.updateNoAuPeerSetFromJson(mau1.getAuId(), naps1.toJson(null),
                                       "mint milano");
    Map m = cons.receiveMap(TIMEOUT_SHOULDNT);
    String json = (String)m.remove("json");
    assertEquals(MapUtil.map("auid", "plug&aaa1",
                             "cookie", "mint milano",
                             "name", "NoAuPeerSet"),
                 m);
    Map jmap = AuUtil.jsonToMap(json);
    assertEquals(auid1, jmap.get("auid"));
    assertEquals(-1, jmap.get("date"));
    assertSameElements(ListUtil.list("TCP:[127.0.0.2]:1231",
                                     "TCP:[127.0.0.1]:1231"),
                       (List)jmap.get("rawSet"));

  }

  @Test
  // Test that local updates of AuAgreements objects cause notifications to be
  // sent, and that updates from service cause local AuAgreements to be updated
  public void testAuAgreements() throws Exception {
    AuAgreements aua1 = stateMgr.getAuAgreements(mau1);
    AuAgreements aua2 = stateMgr.getAuAgreements(mau2);
    assertNotSame(aua1, aua2);
    assertSame(aua1, stateMgr.getAuAgreements(mau1));
    assertSame(aua2, stateMgr.getAuAgreements(mau2));
    assertAgreeTime(-1.0f, 0, aua1.findPeerAgreement(pid1, POR));

    aua1.signalPartialAgreement(pid0, POR, .10f, 910);
    aua1.signalPartialAgreement(pid0, POP, .20f, 910);

    aua1.signalPartialAgreement(pid1, POR, .30f, 920);
    aua1.signalPartialAgreement(pid1, POP, .40f, 920);

    assertAgreeTime(.10f, 910, aua1.findPeerAgreement(pid0, POR));
    assertAgreeTime(.20f, 910, aua1.findPeerAgreement(pid0, POP));
    assertAgreeTime(.30f, 920, aua1.findPeerAgreement(pid1, POR));
    assertAgreeTime(.40f, 920, aua1.findPeerAgreement(pid1, POP));

    String json = aua1.toJson(SetUtil.set(pid0, pid1));
    storeAuAgreements(aua1, pid0, pid1);

    assertEquals(auAgreementsUpdateMap(aua1.getAuid(), json),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    aua2.signalPartialAgreement(pid0, POR, .50f, 1910);
    aua2.signalPartialAgreement(pid0, POP, .60f, 1910);

    String json2 = aua2.toJson(SetUtil.set(pid0));
    stateMgr.updateAuAgreementsFromJson(stateMgr.auKey(mau1), json2, null);

    assertAgreeTime(.50f, 1910, aua1.findPeerAgreement(pid0, POR));
    assertAgreeTime(.60f, 1910, aua1.findPeerAgreement(pid0, POP));
  }


  @Test
  // Test that local updates of AuSuspectUrlVersions objects cause
  // notifications to be sent, and that updates from service cause local
  // AuSuspectUrlVersions to be updated
  public void testAuSuspectUrlVersions() throws Exception {
    AuSuspectUrlVersions asuv1 = stateMgr.getAuSuspectUrlVersions(mau1);
    AuSuspectUrlVersions asuv2 = stateMgr.getAuSuspectUrlVersions(mau2);
    assertNotSame(asuv1, asuv2);
    assertSame(asuv1, stateMgr.getAuSuspectUrlVersions(mau1));
    assertSame(asuv2, stateMgr.getAuSuspectUrlVersions(mau2));
    assertTrue(asuv1.isEmpty());
    assertFalse(asuv1.isSuspect(URL1, 1));

    asuv1.markAsSuspect(URL1, 1, HASH1, HASH2);
    asuv1.markAsSuspect(URL2, 2, HASH2, HASH1);
    assertTrue(asuv1.isSuspect(URL1, 1));
    assertTrue(asuv1.isSuspect(URL2, 2));
    assertFalse(asuv1.isSuspect(URL1, 2));
    assertFalse(asuv1.isSuspect(URL2, 1));

    String json = asuv1.toJson();
    asuv1.storeAuSuspectUrlVersions();
    assertEquals(auSuspectUrlVersionsUpdateMap(mau1.getAuId(), json),
		 cons.receiveMap(TIMEOUT_SHOULDNT));

    asuv1.markAsSuspect(URL1, 33, HASH2, HASH1);
    asuv1.markAsSuspect(URL2, 222, HASH1, HASH2);
    String json2 = asuv1.toJson();
    assertNotEquals(json, json2);
    stateMgr.updateAuSuspectUrlVersionsFromJson(mau1.getAuId(), json2, "toll");

    assertEquals(auSuspectUrlVersionsUpdateMap(mau1.getAuId(), json2),
		 cons.receiveMap(TIMEOUT_SHOULDNT));
  }

  UserAccount makeUser(String name) {
    UserAccount acct = new BasicUserAccount.Factory().newUser(name, null);
    return acct;
  }

  @Test
  public void testUserAccount() throws Exception {
    UserAccount acct1 = makeUser("User1");
    UserAccount acct2 = makeUser("User2");

    assertNull(stateMgr.getUserAccount(acct1.getName()));
    assertNull(stateMgr.getUserAccount(acct2.getName()));

    // Test JMS message on store user account
    stateMgr.storeUserAccount(acct1);
    assertNotNull(stateMgr.getUserAccount(acct1.getName()));

    assertEquals(
        userAccountUpdateMap(acct1.getName(), acct1.toJson(), "ADD", null),
        cons.receiveMap(TIMEOUT_SHOULDNT));

    String json1 = "{\"lastLogin\":\"123\"}";
    String json2 = "{\"lastLogin\":\"456\"}";
    Set<String> fields = SetUtil.set("lastLogin");

    // Test partial user account update without a cookie
    stateMgr.updateUserAccountFromJson(acct1.getName(), json1, null);
    assertEquals(123, acct1.getLastLogin());

    assertEquals(
        userAccountUpdateMap(acct1.getName(), UserAccount.jsonFromUserAccount(acct1, fields), "UPDATE", null),
        cons.receiveMap(TIMEOUT_SHOULDNT));

    // Test partial user account update with a cookie
    stateMgr.updateUserAccountFromJson(acct1.getName(), json2, "xyzzy");
    assertEquals(456, acct1.getLastLogin());

    assertEquals(
        userAccountUpdateMap(acct1.getName(), UserAccount.jsonFromUserAccount(acct1, fields), "UPDATE", "xyzzy"),
        cons.receiveMap(TIMEOUT_SHOULDNT));

    // Test update call with fields set null or empty (results in a store)
    stateMgr.updateUserAccount(acct2, null);
    assertEquals(
        userAccountUpdateMap(acct2.getName(), acct2.toJson(), "ADD", null),
        cons.receiveMap(TIMEOUT_SHOULDNT));

    // Test JMS message on delete user account
    stateMgr.removeUserAccount(acct1);
    assertNull(stateMgr.getUserAccount(acct1.getName()));
    assertEquals(
        userAccountUpdateMap(acct1.getName(), null, "DELETE", null),
        cons.receiveMap(TIMEOUT_SHOULDNT));
  }

  static class MyServerStateManager extends ServerStateManager {
    private SimpleBinarySemaphore rcvSem;

    // Suppress DB load
    @Override
    protected AuStateBean doLoadAuStateBean(String key) {
      log.debug2("MyServerStateManager.doLoadAuState({})", key);
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

    // Suppress DB load
    @Override
    protected AuAgreements doLoadAuAgreements(String key) {
      log.debug2("MyServerStateManager.doLoadAuAgreements({})", key);
      return null;
    }

    // Suppress DB load
    @Override
    protected AuSuspectUrlVersions doLoadAuSuspectUrlVersions(String key) {
      log.debug2("MyServerStateManager.doLoadAuSuspectUrlVersions({})", key);
      return null;
    }
  }
}
