/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.File;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.lockss.config.ConfigManager;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.log.L4JLogger;
import org.lockss.plugin.AuUtil;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockArchivalUnit;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.MockPlugin;
import org.lockss.util.ListUtil;
import org.lockss.util.time.TimeBase;

/**
 * Test class for org.lockss.state.DbStateManager.
 */
public class TestDbStateManager extends LockssTestCase4 {
  L4JLogger log = L4JLogger.getLogger();
  DbStateManager stateMgr;
  MockArchivalUnit mau1;
  MockArchivalUnit mau2;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    String tmpdir = getTempDir().toString();
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tmpdir);

    System.setProperty("derby.stream.error.file",
	new File(tmpdir, "derby.log").getAbsolutePath());

    MockLockssDaemon daemon = getMockLockssDaemon();

    // Create and start the database manager.
    ConfigDbManager dbManager = new ConfigDbManager();
    daemon.setManagerByType(ConfigDbManager.class, dbManager);
    dbManager.initService(daemon);
    dbManager.startService();

    stateMgr = daemon.setUpStateManager(new DbStateManager());

    MockPlugin plugin = new MockPlugin(daemon);
    mau1 = new MockArchivalUnit(plugin, MockPlugin.KEY + "&base_url~aaa1");
    mau2 = new MockArchivalUnit(plugin, MockPlugin.KEY + "&base_url~aaa2");
  }

  @Test
  public void testStoreAndLoadAuState() throws Exception {
    // First AU with default state properties.
    String key = stateMgr.auKey(mau1);
    AuState auState = stateMgr.newDefaultAuState(mau1);
    String json = auState.toJson();

    stateMgr.doStoreAuStateNew(key, auState, json, AuUtil.jsonToMap(json));

    AuState newAuState = stateMgr.doLoadAuState(mau1);
    String newJson = AuUtil.jsonFromAuState(newAuState, null);
    assertEquals(AuUtil.jsonToMap(json), AuUtil.jsonToMap(newJson));

    // Do it again.
    try {
      stateMgr.doStoreAuStateNew(key, auState, json, AuUtil.jsonToMap(json));
      fail("Should have thrown IllegalStateException");
    } catch (IllegalStateException ise) {
      // Expected.
    }

    // Second AU with modified state properties.
    key = stateMgr.auKey(mau2);
    auState = stateMgr.newDefaultAuState(mau2);
    json = auState.toJson();
    Map<String, Object> auStateMap = AuUtil.jsonToMap(json);
    auStateMap.put("auCreationTime", TimeBase.nowMs());
    auStateMap.put("cdnStems",
	ListUtil.list("http://abc.com", "https://xyz.org"));
    json = AuUtil.mapToJson(auStateMap);

    stateMgr.doStoreAuStateNew(key, auState, json, auStateMap);

    newAuState = stateMgr.doLoadAuState(mau2);
    newJson = AuUtil.jsonFromAuState(newAuState, null);
    assertEquals(auStateMap, AuUtil.jsonToMap(newJson));
  }
}
