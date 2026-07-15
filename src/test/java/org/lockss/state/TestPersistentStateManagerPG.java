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

import java.util.*;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.lockss.config.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.lockss.config.db.ConfigDbManager;
import org.lockss.log.*;
import org.lockss.plugin.*;
import org.lockss.rs.io.index.db.SQLArtifactIndexDbManager;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;

/**
 * Test (parts of) PersistentStateManager against (embedded) PostgreSQL.
 */
public class TestPersistentStateManagerPG extends StateTestCase {
  static L4JLogger log = L4JLogger.getLogger();

  MyPersistentStateManager myStateMgr;
  ConfigDbManager dbManager;

  /** Shared embedded PostgreSQL instance for all tests in this class */
  private static EmbeddedPostgres embeddedPg;

  @BeforeClass
  public static void setUpClass() throws Exception {
    embeddedPg = startEmbeddedPostgres();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    stopEmbeddedPostgre();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Create a unique database for this test
    String dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

    // Configure DbManager settings
    ConfigurationUtil.addFromArgs(
        ConfigDbManager.PARAM_DATASOURCE_CLASSNAME, "org.postgresql.ds.PGSimpleDataSource",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_DATABASENAME, dbName,
        ConfigDbManager.PARAM_DATASOURCE_SERVERNAME, "localhost",
        ConfigDbManager.PARAM_DATASOURCE_PORTNUMBER, String.valueOf(embeddedPg.getPort()));

    ConfigurationUtil.addFromArgs(
        ConfigDbManager.PARAM_DATASOURCE_USER, "postgres",
        ConfigDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    ConfigurationUtil.addFromArgs(
        ConfigDbManager.PARAM_MAX_RETRY_COUNT, "0",
        ConfigDbManager.PARAM_RETRY_DELAY, "0");

    // Create and start the database manager.
    dbManager = new ConfigDbManager();
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
  public void testStoreAndLoadLargeAuStatePostgress() throws Exception {
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
}
