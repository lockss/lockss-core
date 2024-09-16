/*

Copyright (c) 2018-2024 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.state;

import java.io.*;
import java.util.*;
import org.mockito.Mockito;
import org.junit.Before;
import org.junit.Test;
import org.lockss.app.StoreException;
import org.lockss.config.ConfigManager;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.log.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import static org.lockss.protocol.AgreementType.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;

/**
 * Test class for org.lockss.state.PersistentStateManager.
 */
public class TestPersistentStateManager extends StateTestCase {
  static L4JLogger log = L4JLogger.getLogger();

  MyPersistentStateManager myStateMgr;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("derby.stream.error.file",
	new File(tmpdir, "derby.log").getAbsolutePath());

    // Create and start the database manager.
    ConfigDbManager dbManager = new ConfigDbManager();
    daemon.setManagerByType(ConfigDbManager.class, dbManager);
    dbManager.initService(daemon);
    dbManager.startService();
  }

  @Override
  protected StateManager makeStateManager() {
    myStateMgr = new MyPersistentStateManager();
    return myStateMgr;
  }


  @Test
  public void testStoreAndLoadAuState() throws Exception {
    // First AU with default state properties.
    TimeBase.setSimulated(100L);

    String key1 = AUID1;
    AuStateBean ausb1 = stateMgr.getAuStateBean(key1);
    assertEquals(-1, ausb1.getAuCreationTime());
    String json1 = ausb1.toJsonExcept("auCreationTime");

    stateMgr.doStoreAuStateBean(key1, ausb1, null); // gets new creation time

    AuStateBean ausb1b = stateMgr.doLoadAuStateBean(key1);
    assertEquals(100L, ausb1b.getAuCreationTime());
    String json1b = ausb1b.toJsonExcept("auCreationTime");
    assertEquals(AuUtil.jsonToMap(json1), AuUtil.jsonToMap(json1b));

    // Second AU with modified state properties.
    TimeBase.setSimulated(200L);
    
    String key2 = stateMgr.auKey(mau2);
    AuStateBean ausb2 = stateMgr.getAuStateBean(key2);
    ausb2.setAuCreationTime(123454);
    ausb2.setCdnStems(ListUtil.list("http://abc.com", "https://xyz.org"));
    ausb2.setMetadataExtractionEnabled(false);
    String json2 = ausb2.toJson();
    stateMgr.doStoreAuStateBean(key2, ausb2, null); // has existing creation time

    AuStateBean ausb2b = stateMgr.doLoadAuStateBean(key2);
    String json2b = ausb2b.toJson();
    assertEquals(AuUtil.jsonToMap(json2), AuUtil.jsonToMap(json2b));
    assertEquals(123454, ausb2b.getAuCreationTime());

    // make some changes
    ausb2.setAverageHashDuration(1234L);
    List<String> cdn = new ArrayList<>();
    for (int x = 1; x <= 15; x++) {
      cdn.add("http://abcabc" + x + ".com");
    }
    ausb2.setCdnStems(cdn);
    // This should not be stored bacause the AU already has a creation time
    ausb2.setAuCreationTime(8888888);
    json2 = ausb2.toJson();
    log.debug("Large AuState json len: {}", json2.length());
    assertTrue(json2.length() >= DbStateManagerSql.JSON_COMPRESSION_THRESHOLD);
    stateMgr.doStoreAuStateBean(key2, ausb2,
	SetUtil.set("averageHashDuration"));
    AuStateBean ausb2c = stateMgr.doLoadAuStateBean(key2);
    // should have original creation time
    assertEquals(123454, ausb2c.getAuCreationTime());
    // Remove known difference for following assert
    ausb2c.setAuCreationTime(8888888);
    String json2c = ausb2c.toJson();
    assertEquals(AuUtil.jsonToMap(json2), AuUtil.jsonToMap(json2c));
  }

  @Test
  public void testStoreAndLoadLargeAuState() throws Exception {
    // First AU with default state properties.
    TimeBase.setSimulated(100L);

    String key1 = AUID1;
    AuStateBean ausb1 = stateMgr.getAuStateBean(key1);
    assertEquals(-1, ausb1.getAuCreationTime());
    String json1 = ausb1.toJsonExcept("auCreationTime");

    stateMgr.doStoreAuStateBean(key1, ausb1, null); // gets new creation time

    AuStateBean ausb1b = stateMgr.doLoadAuStateBean(key1);
    assertEquals(100L, ausb1b.getAuCreationTime());
    String json1b = ausb1b.toJsonExcept("auCreationTime");
    assertEquals(AuUtil.jsonToMap(json1), AuUtil.jsonToMap(json1b));

    // Second AU with modified state properties.
    TimeBase.setSimulated(200L);

    String key2 = stateMgr.auKey(mau2);
    AuStateBean ausb2 = stateMgr.getAuStateBean(key2);
    ausb2.setAuCreationTime(123454);
//     ausb2.setCdnStems(ListUtil.list("http://abc.com", "https://xyz.org"));
    ausb2.setCdnStems(ListUtil.list("http://abc.com", "https://xyz.org" +
                                    RandomUtil.randomAlphabetic(20000)));
    ausb2.setMetadataExtractionEnabled(false);
    String json2 = ausb2.toJson();
    log.debug("ausb2 json len: {}", json2.length());
    stateMgr.doStoreAuStateBean(key2, ausb2, null); // has existing creation time

    AuStateBean ausb2b = stateMgr.doLoadAuStateBean(key2);
    String json2b = ausb2b.toJson();
    assertEquals(AuUtil.jsonToMap(json2), AuUtil.jsonToMap(json2b));
    assertEquals(123454, ausb2b.getAuCreationTime());

    // make some changes
    ausb2.setAverageHashDuration(1234L);
    List<String> cdn = new ArrayList<>();
    for (int x = 1; x <= 15; x++) {
      cdn.add("http://abcabc" + x + ".com");
    }
    ausb2.setCdnStems(cdn);
    // This should not be stored bacause the AU already has a creation time
    ausb2.setAuCreationTime(8888888);
    json2 = ausb2.toJson();
    log.debug("Large AuState json len: {}", json2.length());
    assertTrue(json2.length() >= DbStateManagerSql.JSON_COMPRESSION_THRESHOLD);
    stateMgr.doStoreAuStateBean(key2, ausb2,
	SetUtil.set("averageHashDuration"));
    AuStateBean ausb2c = stateMgr.doLoadAuStateBean(key2);
    // should have original creation time
    assertEquals(123454, ausb2c.getAuCreationTime());
    // Remove known difference for following assert
    ausb2c.setAuCreationTime(8888888);
    String json2c = ausb2c.toJson();
    assertEquals(AuUtil.jsonToMap(json2), AuUtil.jsonToMap(json2c));
  }

  @Test
  public void testFuncAuStateBean() throws Exception {
    // Store a bean in the db
    AuStateBean b1 = stateMgr.newDefaultAuStateBean(AUID1);
    assertTrue(b1.isMetadataExtractionEnabled());
    b1.setLastCrawlAttempt(7777);
    b1.setCdnStems(CDN_STEMS);
    b1.setMetadataExtractionEnabled(false);
    String json1 = b1.toJson();

    MyStateStore sstore = new MyStateStore();
    myStateMgr.setStateStore(sstore);
    sstore.setStoredAuState(AUID1, json1);

    AuStateBean ausb1 = stateMgr.getAuStateBean(AUID1);
    assertEquals(7777, ausb1.getLastCrawlAttempt());
    assertEquals(CDN_STEMS, ausb1.getCdnStems());
    assertFalse(ausb1.isMetadataExtractionEnabled());

    AuStateBean ausb2 = stateMgr.getAuStateBean(AUID2);
    assertEquals(-1, ausb2.getLastCrawlAttempt());
    assertNull(ausb2.getCdnStems());
    assertTrue(ausb2.isMetadataExtractionEnabled());

    ausb1.setLastCrawlTime(32323);
    stateMgr.updateAuStateBean(AUID1, ausb1, SetUtil.set("lastCrawlTime"));
    String storedjson = sstore.getStoredAuState(AUID1);
    assertMatchesRE("\"lastCrawlTime\":32323", storedjson);
    assertMatchesRE("\"lastCrawlAttempt\":7777", storedjson);
    assertMatchesRE("\"isMetadataExtractionEnabled\":false", storedjson);
  }

  @Test
  public void testFuncAuState() throws Exception {
    // Pre-store an AuState in the db
    AuStateBean ausb1 = stateMgr.newDefaultAuStateBean(AUID1);
    assertTrue(ausb1.isMetadataExtractionEnabled());
    ausb1.setLastCrawlAttempt(7777);
    ausb1.setCdnStems(CDN_STEMS);
    ausb1.setMetadataExtractionEnabled(false);
    String json = ausb1.toJson();

    // Set up fake DB SQL layer
    MyStateStore sstore = new MyStateStore();
    myStateMgr.setStateStore(sstore);
    sstore.setStoredAuState(AUID1, json);

    // Fetch the AuState that's in the DB
    AuState aus1 = stateMgr.getAuState(mau1);
    assertEquals(7777, aus1.getLastCrawlAttempt());
    assertEquals(CDN_STEMS, aus1.getCdnStems());
    assertFalse(ausb1.isMetadataExtractionEnabled());

    // Fetch one with no data
    AuState aus2 = stateMgr.getAuState(mau2);
    assertEquals(-1, aus2.getLastCrawlAttempt());
    assertEmpty(aus2.getCdnStems());
    assertTrue(aus2.isMetadataExtractionEnabled());

    // Perform a json-only update from the service, ensure DB and
    // existing AuState instance get updated.
    AuStateBean ausb2 = stateMgr.newDefaultAuStateBean(AUID1);
    ausb2.setLastCrawlAttempt(7778);
    ausb2.setLastCrawlTime(7779);
    ausb2.setMetadataExtractionEnabled(false);
    String json2 = ausb2.toJson(SetUtil.set("lastCrawlTime",
					    "lastCrawlAttempt",
					    "isMetadataExtractionEnabled"));
    assertEquals(7777, aus1.getLastCrawlAttempt());
    assertEquals(-1, aus1.getLastCrawlTime());
    assertFalse(aus1.isMetadataExtractionEnabled());
    stateMgr.updateAuStateFromJson(AUID1, json2, null);
    assertEquals(7778, aus1.getLastCrawlAttempt());
    assertEquals(7779, aus1.getLastCrawlTime());
    assertFalse(aus1.isMetadataExtractionEnabled());

    String storedjson = sstore.getStoredAuState(AUID1);
    assertMatchesRE("\"lastCrawlAttempt\":7778", storedjson);
    assertMatchesRE("\"lastCrawlTime\":7779", storedjson);
    assertMatchesRE("\"isMetadataExtractionEnabled\":false", storedjson);
  }

  @Test
  public void testAuCreationDate() throws Exception {
    // Store a bean in the db
    AuStateBean b1 = stateMgr.newDefaultAuStateBean(AUID1);
    assertTrue(b1.isMetadataExtractionEnabled());
    b1.setLastCrawlAttempt(7777);
    b1.setCdnStems(CDN_STEMS);
    b1.setMetadataExtractionEnabled(false);
    String json1 = b1.toJson();

    MyStateStore sstore = new MyStateStore();
    myStateMgr.setStateStore(sstore);
    sstore.setStoredAuState(AUID1, json1);

    AuStateBean ausb1 = stateMgr.getAuStateBean(AUID1);
    assertEquals(7777, ausb1.getLastCrawlAttempt());
    assertEquals(CDN_STEMS, ausb1.getCdnStems());
    assertFalse(ausb1.isMetadataExtractionEnabled());

    AuStateBean ausb2 = stateMgr.getAuStateBean(AUID2);
    assertEquals(-1, ausb2.getLastCrawlAttempt());
    assertNull(ausb2.getCdnStems());
    assertTrue(ausb2.isMetadataExtractionEnabled());

    ausb1.setLastCrawlTime(32323);
    stateMgr.updateAuStateBean(AUID1, ausb1, SetUtil.set("lastCrawlTime"));
    String storedjson = sstore.getStoredAuState(AUID1);
    assertMatchesRE("\"lastCrawlTime\":32323", storedjson);
    assertMatchesRE("\"lastCrawlAttempt\":7777", storedjson);
    assertMatchesRE("\"isMetadataExtractionEnabled\":false", storedjson);
  }

  MockPeerIdentity randomPid() {
    MockPeerIdentity pid =
      new MockPeerIdentity("tcp:[" +
                           randomInt(0,127) + "." + randomInt(0,127) + "." +
                           randomInt(0,127) + "." + randomInt(0,127) +
                           "]:1231");
    idMgr.addPeerIdentity(pid.getIdString(), pid);
    return pid;
  }

  int randomInt(int min, int max) {
    return min + (int)(Math.random() * ((max - min) + 1));
  }

  @Test
  public void testStoreAndLoadAuAgreements() throws Exception {
    // Test roundtrip of AuAgreements that doesn't get compressed
    AuAgreements aua0out = stateMgr.newDefaultAuAgreements(AUID1);
    aua0out.signalPartialAgreement(pid1, POR, .8f, 400);
    aua0out.signalPartialAgreement(pid1, POP, .6f, 400);
    assertTrue(aua0out.toJson().length() < DbStateManagerSql.JSON_COMPRESSION_THRESHOLD);
    stateMgr.doStoreAuAgreementsUpdate(AUID1, aua0out, null);
    AuAgreements aua0in = stateMgr.doLoadAuAgreements(AUID1);
    assertEquals(aua0out, aua0in);

    // Test roundtrip of AuAgreements that does get compressed
    AuAgreements aua1out = stateMgr.newDefaultAuAgreements(AUID2);
    aua1out.signalPartialAgreement(pid1, POR, .8f, 400);
    aua1out.signalPartialAgreement(pid1, POP, .6f, 400);
    for (int i=1; i<50; i++) {
      aua1out.signalPartialAgreement(randomPid(), POR, (float)Math.random(),
                                  (int)(Math.random() * ((1000000000))));
      aua1out.signalPartialAgreement(randomPid(), W_POR, (float)Math.random(),
                                  (int)(Math.random() * ((1000000000))));
      aua1out.signalPartialAgreement(randomPid(), POR_HINT, (float)Math.random(),
                                  (int)(Math.random() * ((1000000000))));
    }
    assertTrue(aua1out.toJson().length() >= DbStateManagerSql.JSON_COMPRESSION_THRESHOLD);
    stateMgr.doStoreAuAgreementsUpdate(AUID2, aua1out, null);

    AuAgreements aua1in = stateMgr.doLoadAuAgreements(AUID2);
    assertEquals(aua1out, aua1in);
  }

  @Test
  public void testFuncAuAgreements() throws Exception {
    // Pre-store an AuAgreements in the db
    AuAgreements aua0 = stateMgr.newDefaultAuAgreements(AUID1);
    aua0.signalPartialAgreement(pid1, POR, .8f, 400);
    aua0.signalPartialAgreement(pid1, POP, .6f, 400);

    String json = aua0.toJson();

    // Set up fake store
    MyStateStore sstore = new MyStateStore();
    myStateMgr.setStateStore(sstore);
    sstore.setStoredAuAgreements(AUID1, json);

    // Fetch the AuAgreements that's in the DB
    AuAgreements aua1 = stateMgr.getAuAgreements(AUID1);
    assertAgreeTime(.8f, 400, aua1.findPeerAgreement(pid1, POR));
    assertAgreeTime(.6f, 400, aua1.findPeerAgreement(pid1, POP));
    assertAgreeTime(-1.0f, 0, aua1.findPeerAgreement(pid0, POP));
    assertSame(aua1, stateMgr.getAuAgreements(AUID1));

    // Fetch one with no data
    AuAgreements aua2 = stateMgr.getAuAgreements(AUID2);
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid0, POP));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid1, POP));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid0, POR));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid1, POR));

    // Perform a json-only update from the service, ensure DB and
    // existing AuAgreements instance get updated.
    AuAgreements aua3 = stateMgr.newDefaultAuAgreements(AUID1);
    aua3.signalPartialAgreement(pid0, POR, .9f, 800);
    aua3.signalPartialAgreement(pid0, POP, .7f, 800);
    String json3 = aua3.toJson(SetUtil.set(pid0));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid0, POP));
    assertAgreeTime(-1.0f, 0, aua2.findPeerAgreement(pid0, POR));
    assertAgreeTime(.8f, 400, aua1.findPeerAgreement(pid1, POR));
    assertAgreeTime(.6f, 400, aua1.findPeerAgreement(pid1, POP));
    stateMgr.updateAuAgreementsFromJson(AUID1, json3, null);
    assertAgreeTime(.9f, 800, aua1.findPeerAgreement(pid0, POR));
    assertAgreeTime(.7f, 800, aua1.findPeerAgreement(pid0, POP));
    assertAgreeTime(.8f, 400, aua1.findPeerAgreement(pid1, POR));
    assertAgreeTime(.6f, 400, aua1.findPeerAgreement(pid1, POP));

    String storedjson = sstore.getStoredAuAgreements(AUID1);
    assertMatchesRE("\"percentAgreement\":0.9", storedjson);
    assertMatchesRE("\"percentAgreementTime\":800", storedjson);
  }

  @Test
  public void testFuncAuSuspectUrlVersions() throws Exception {
    // Pre-store an AuSuspectUrlVersions in the db
    AuSuspectUrlVersions asuv0 = stateMgr.newDefaultAuSuspectUrlVersions(AUID1);
    assertTrue(asuv0.isEmpty());
    asuv0.markAsSuspect(URL1, 1, null, null);
    assertFalse(asuv0.isEmpty());
    assertTrue(asuv0.isSuspect(URL1, 1));
    assertFalse(asuv0.isSuspect(URL1, 2));

    String json = asuv0.toJson();

    // Set up fake store
    MyStateStore sstore = new MyStateStore();
    myStateMgr.setStateStore(sstore);
    sstore.setStoredAuSuspectUrlVersions(AUID1, json);

    // Fetch the AuSuspectUrlVersions that's in the DB
    AuSuspectUrlVersions asuv1 = stateMgr.getAuSuspectUrlVersions(AUID1);
    assertFalse(asuv1.isEmpty());
    assertTrue(asuv1.isSuspect(URL1, 1));
    assertFalse(asuv1.isSuspect(URL1, 2));
    assertEquals(1, asuv1.getSuspectList().size());
    assertSame(asuv1, stateMgr.getAuSuspectUrlVersions(AUID1));

    // Fetch one with no data
    AuSuspectUrlVersions asuv2 = stateMgr.getAuSuspectUrlVersions(AUID2);
    assertTrue(asuv2.isEmpty());
    assertFalse(asuv2.isSuspect(URL1, 1));

    // Perform a json-only update from the service, ensure DB and
    // existing AuSuspectUrlVersions instance get updated.
    AuSuspectUrlVersions asuv3 = stateMgr.newDefaultAuSuspectUrlVersions(AUID1);
    asuv3.markAsSuspect(URL1, 3, null, null);

    String json3 = asuv3.toJson();
    assertTrue(asuv3.isSuspect(URL1, 3));
    stateMgr.updateAuSuspectUrlVersionsFromJson(AUID1, json3, null);
    assertTrue(asuv1.isSuspect(URL1, 3));

    String storedjson = sstore.getStoredAuSuspectUrlVersions(AUID1);
    assertMatchesRE("\"auid\":\"" + RegexpUtil.quotemeta(AUID1), storedjson);
    assertMatchesRE("\"url\":\"" + RegexpUtil.quotemeta(URL1), storedjson);
    assertMatchesRE("\"version\":3", storedjson);
    assertNotMatchesRE("\"version\":1", storedjson);
  }


  @Test
  public void testFuncNoAuPeerSet() throws Exception {
    // Pre-store an NoAuPeerSet in the db
    DatedPeerIdSet naps0 = stateMgr.newDefaultNoAuPeerSet(AUID1);
    assertTrue(naps0.isEmpty());
    assertEquals(-1, naps0.getDate());
    naps0.add(pid0);
    naps0.add(pid2);
    naps0.setDate(123);
    assertFalse(naps0.isEmpty());
    assertTrue(naps0.contains(pid0));
    assertFalse(naps0.contains(pid1));
    assertTrue(naps0.contains(pid2));
    assertEquals(123, naps0.getDate());

    String json = naps0.toJson();

    // Set up fake store
    MyStateStore sstore = new MyStateStore();
    myStateMgr.setStateStore(sstore);
    sstore.setStoredNoAuPeerSet(AUID1, json);

    // Fetch the NoAuPeerSet that's in the DB
    DatedPeerIdSet naps1 = stateMgr.getNoAuPeerSet(AUID1);
    // shouldn't get naps0 back as it didn't come from StateManager
    assertNotSame(naps1, naps0);

    assertFalse(naps1.isEmpty());
    assertTrue(naps1.contains(pid0));
    assertFalse(naps1.contains(pid1));
    assertTrue(naps1.contains(pid2));
    assertEquals(123, naps1.getDate());
    assertSame(naps1, stateMgr.getNoAuPeerSet(AUID1));

    // Fetch one with no data
    DatedPeerIdSet naps2 = stateMgr.getNoAuPeerSet(AUID2);
    assertTrue(naps2.isEmpty());
    assertFalse(naps2.contains(pid1));

    // Perform a json-only update from the service, ensure DB and
    // existing NoAuPeerSet instance get updated.
    DatedPeerIdSet naps3 = stateMgr.newDefaultNoAuPeerSet(AUID1);
    naps3.add(pid1);
    naps3.setDate(333);

    assertTrue(naps3.contains(pid1));
    assertFalse(naps1.contains(pid1));
    String json3 = naps3.toJson();
    stateMgr.updateNoAuPeerSetFromJson(AUID1, json3, null);
    assertTrue(naps1.contains(pid1));

    String storedjson = sstore.getStoredNoAuPeerSet(AUID1);
    assertMatchesRE("\"auid\":\"" + RegexpUtil.quotemeta(AUID1), storedjson);
    assertMatchesRE("\"rawSet\":\\[\"" + RegexpUtil.quotemeta(pid1.getIdString()),
		    storedjson);
    assertMatchesRE("\"date\":333", storedjson);
  }

}
