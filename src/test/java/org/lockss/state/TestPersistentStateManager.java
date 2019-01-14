/*

Copyright (c) 2018-2019 Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.config.ConfigManager;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.db.DbException;
import org.lockss.log.*;
import org.lockss.plugin.*;
import org.lockss.protocol.*;
import org.lockss.test.*;
import org.lockss.util.ListUtil;
import org.lockss.util.SetUtil;
import org.lockss.util.time.TimeBase;

/**
 * Test class for org.lockss.state.PersistentStateManager.
 */
public class TestPersistentStateManager extends LockssTestCase4 {
  static L4JLogger log = L4JLogger.getLogger();

  MockLockssDaemon daemon;
  PluginManager pluginMgr;
  MyPersistentStateManager stateMgr;
  MockArchivalUnit mau1;
  MockArchivalUnit mau2;

  static String AUID1 = MockPlugin.KEY + "&base_url~aaa1";
  static String AUID2 = MockPlugin.KEY + "&base_url~aaa2";
  static List CDN_STEMS = ListUtil.list("http://abc.com", "https://xyz.org");

  @Before
  public void setUp() throws Exception {
    super.setUp();

    
    String tmpdir = setUpDiskSpace();
    System.setProperty("derby.stream.error.file",
	new File(tmpdir, "derby.log").getAbsolutePath());

    daemon = getMockLockssDaemon();

    // Create and start the database manager.
    ConfigDbManager dbManager = new ConfigDbManager();
    daemon.setManagerByType(ConfigDbManager.class, dbManager);
    dbManager.initService(daemon);
    dbManager.startService();

    stateMgr = daemon.setUpStateManager(new MyPersistentStateManager());

    pluginMgr = daemon.getPluginManager();

    MockPlugin plugin = new MockPlugin(daemon);
    mau1 = new MockArchivalUnit(AUID1);
    mau2 = new MockArchivalUnit(AUID2);

  }

  @Test
  public void testStoreAndLoadAuState() throws Exception {
    // First AU with default state properties.
    TimeBase.setSimulated(100L);

    String key1 = stateMgr.auKey(mau1);
    AuStateBean ausb1 = stateMgr.newDefaultAuStateBean(key1);
    String json1 = ausb1.toJsonExcept("auCreationTime");

    stateMgr.doStoreAuStateBeanNew(key1, ausb1); // gets new creation time

    AuStateBean ausb1b = stateMgr.doLoadAuStateBean(key1);
    assertEquals(100L, ausb1b.getAuCreationTime());
    String json1b = ausb1b.toJsonExcept("auCreationTime");
    assertEquals(AuUtil.jsonToMap(json1), AuUtil.jsonToMap(json1b));

    // Do it again.
    try {
      stateMgr.doStoreAuStateBeanNew(key1, ausb1);
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ise) {
      // Expected.
    }

    // Second AU with modified state properties.
    TimeBase.setSimulated(200L);
    
    String key2 = stateMgr.auKey(mau2);
    AuStateBean ausb2 = stateMgr.newDefaultAuStateBean(key2);
    ausb2.setAuCreationTime(TimeBase.nowMs());
    ausb2.setCdnStems(ListUtil.list("http://abc.com", "https://xyz.org"));
    String json2 = ausb2.toJson();

    stateMgr.doStoreAuStateBeanNew(key2, ausb2); // has existing creation time

    AuStateBean ausb2b = stateMgr.doLoadAuStateBean(key2);
    String json2b = ausb2b.toJson();
    assertEquals(AuUtil.jsonToMap(json2), AuUtil.jsonToMap(json2b));

    // Update a record
    ausb2.setAverageHashDuration(1234L);
    json2 = ausb2.toJson();
    stateMgr.doStoreAuStateBeanUpdate(key2, ausb2,
	SetUtil.set("averageHashDuration"));
    AuStateBean ausb2c = stateMgr.doLoadAuStateBean(key2);
    String json2c = ausb2c.toJson();
    assertEquals(AuUtil.jsonToMap(json2), AuUtil.jsonToMap(json2c));
  }

  @Test
  public void testFuncAuStateBean() throws Exception {
    // Store a bean in the db
    AuStateBean b1 = stateMgr.newDefaultAuStateBean(AUID1);
    b1.setLastCrawlAttempt(7777);
    b1.setCdnStems(CDN_STEMS);
    String json1 = b1.toJson();

    MyStateStore sstore = new MyStateStore();
    stateMgr.setStateStore(sstore);
    sstore.setStoredAuState(AUID1, json1);

    AuStateBean ausb1 = stateMgr.getAuStateBean(AUID1);
    assertEquals(7777, ausb1.getLastCrawlAttempt());
    assertEquals(CDN_STEMS, ausb1.getCdnStems());

    AuStateBean ausb2 = stateMgr.getAuStateBean(AUID2);
    assertEquals(-1, ausb2.getLastCrawlAttempt());
    assertNull(ausb2.getCdnStems());

    ausb1.setLastCrawlTime(32323);
    stateMgr.updateAuStateBean(AUID1, ausb1, SetUtil.set("lastCrawlTime"));
    String storedjson = sstore.getStoredAuState(AUID1);
    assertMatchesRE("\"lastCrawlTime\":32323", storedjson);
    assertMatchesRE("\"lastCrawlAttempt\":7777", storedjson);
  }

  @Test
  public void testFuncAuState() throws Exception {
    // Pre-store an AuState in the db
    AuStateBean ausb1 = stateMgr.newDefaultAuStateBean(AUID1);
    ausb1.setLastCrawlAttempt(7777);
    ausb1.setCdnStems(CDN_STEMS);
    String json = ausb1.toJson();

    // Set up fake DB SQL layer
    MyStateStore sstore = new MyStateStore();
    stateMgr.setStateStore(sstore);
    sstore.setStoredAuState(AUID1, json);

    // Fetch the AuState that's in the DB
    AuState aus1 = stateMgr.getAuState(mau1);
    assertEquals(7777, aus1.getLastCrawlAttempt());
    assertEquals(CDN_STEMS, aus1.getCdnStems());

    // Fetch one with no data
    AuState aus2 = stateMgr.getAuState(mau2);
    assertEquals(-1, aus2.getLastCrawlAttempt());
    assertEmpty(aus2.getCdnStems());
    
    // Perform a json-only update from the service, ensure DB and
    // existing AuState instance get updated.
    AuStateBean ausb2 = stateMgr.newDefaultAuStateBean(AUID1);
    ausb2.setLastCrawlAttempt(7778);
    ausb2.setLastCrawlTime(7779);
    String json2 = ausb2.toJson(SetUtil.set("lastCrawlTime",
					    "lastCrawlAttempt"));
    assertEquals(7777, aus1.getLastCrawlAttempt());
    assertEquals(-1, aus1.getLastCrawlTime());
    stateMgr.updateAuStateFromJson(AUID1, json2);
    assertEquals(7778, aus1.getLastCrawlAttempt());
    assertEquals(7779, aus1.getLastCrawlTime());

    String storedjson = sstore.getStoredAuState(AUID1);
    assertMatchesRE("\"lastCrawlAttempt\":7778", storedjson);
    assertMatchesRE("\"lastCrawlTime\":7779", storedjson);
  }

  @Test
  public void testStoreAuStateBean() throws Exception {
    // Store a bean in the db
    AuStateBean b1 = stateMgr.newDefaultAuStateBean(AUID1);
    b1.setLastCrawlAttempt(7777);
    b1.setCdnStems(CDN_STEMS);
    String json1 = b1.toJson();

    stateMgr.storeAuStateFromJson(AUID1, json1);
  }

  static class MyPersistentStateManager extends PersistentStateManager {
    StateStore sstore;

    @Override
    protected StateStore getStateStore() throws DbException {
      if (sstore != null) return sstore;
      return super.getStateStore();
    }

    void setStateStore(StateStore val) {
      sstore = val;
    }

  }


  class MyStateStore implements StateStore {
  
    Map<String,String> austates = new HashMap<>();
    Map<String,String> auagmnts = new HashMap<>();

    @Override
    public AuStateBean findArchivalUnitState(String auId)
	throws DbException, IOException {
      log.debug("findArchivalUnitState("+auId+") = "
		+austates.get(auId));
      return getAuState(auId);
    }

    @Override
    public Long addArchivalUnitState(String auId, AuStateBean ausb)
	throws DbException {
      if (austates.containsKey(auId)) {
	throw new IllegalStateException("Attempt to add but already exists: "
					+ auId);
      }
      putAuState(auId, ausb);
      return 1L;
    }

    void putAuState(String auId, AuStateBean ausb) {
      try {
	austates.put(auId, ausb.toJson());
      } catch (IOException e) {
	throw new RuntimeException(e);
      }
    }

    AuStateBean getAuState(String auId) throws IOException {
      String json = austates.get(auId);
      if (json != null) {
	return AuStateBean.fromJson(auId, json, daemon);
      }
      return null;
    }



    @Override
    public Long updateArchivalUnitState(String auId, AuStateBean ausb,
					Set<String> fields)
	throws DbException {
      if (!austates.containsKey(auId)) {
	throw new IllegalStateException("Attempt to update but doesn't exist: "
					+ auId);
      }
      putAuState(auId, ausb);
      return 1L;
    }

    MyStateStore setStoredAuState(String auId, String json) {
      austates.put(auId, json);
      log.debug("setStoredAuState("+auId+", "+json+")");
      return this;
    }

    String getStoredAuState(String auId) {
      return austates.get(auId);
    }

    @Override
    public AuAgreements findAuAgreements(String key)
	throws IOException {
      return getAuAgreeements(key);
    }

    @Override
    public Long updateAuAgreements(String key, AuAgreements aua,
				   Set<PeerIdentity> peers) {
      if (!auagmnts.containsKey(key)) {
	throw new IllegalStateException("Attempt to update but doesn't exist: "
					+ key);
      }
      putAuAgreements(key, aua);
      return 1L;
    }

    void putAuAgreements(String key, AuAgreements aua) {
      try {
	auagmnts.put(key, aua.toJson());
      } catch (IOException e) {
	throw new RuntimeException(e);
      }
    }

    AuAgreements getAuAgreeements(String auId) throws IOException {
      String json = auagmnts.get(auId);
      if (json != null) {
	return AuAgreements.fromJson(auId, json, daemon);
      }
      return null;
    }

  }
}
