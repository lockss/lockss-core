/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

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
package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import org.apache.commons.collections4.IterableUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;

/**
 * Tests for counting the AUIDs in a namespace, and for the population of the
 * {@code auids} table the count relies on.
 * <p>
 * The issue behind this (#733) suspected that the AUIDs table might only be
 * written during an AU size calculation, which would make any count taken from
 * it wrong rather than merely slow. These tests pin down that it is in fact
 * written as a side effect of indexing an artifact, and that the count agrees
 * with enumerating the AUIDs the slow way.
 */
public class TestSQLArtifactIndexAuidCount extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private MockLockssDaemon theDaemon;
  private SQLArtifactIndexDbManager idxDbManager;

  private static EmbeddedPostgres embeddedPg;

  @BeforeClass
  public static void setUpClass() throws Exception {
    embeddedPg = startEmbeddedPostgres();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    stopEmbeddedPostgre();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    setUpDiskSpace();
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    String dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME,
        "org.postgresql.ds.PGSimpleDataSource",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_DATABASENAME, dbName,
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_SERVERNAME, "localhost",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PORTNUMBER,
        String.valueOf(embeddedPg.getPort()));

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(7);
    idxDbManager.startService();
    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  @Override
  public void tearDown() throws Exception {
    if (idxDbManager != null) {
      idxDbManager.stopService();
    }
    theDaemon.stopDaemon();
    super.tearDown();
  }

  /**
   * The premise the count rests on: indexing an artifact is what creates the
   * AUID row, so an AU is countable from its first artifact onwards and does
   * not have to wait for a size calculation.
   */
  @Test
  public void testFirstArtifactOfAnAuCreatesItsAuidRow() throws Exception {
    SQLArtifactIndexManagerSql idxdb =
        new SQLArtifactIndexManagerSql(idxDbManager);

    assertEquals("no AU has been indexed yet", 0, countAuidRows());

    idxdb.addArtifact(
        makeArtifactSpec("ns1", "auid1", "http://example.com/one", 1)
            .getArtifact());

    assertEquals("indexing the first artifact of an AU must create its AUID row",
        1, countAuidRows());

    // A second artifact in the same AU reuses the row rather than adding one.
    idxdb.addArtifact(
        makeArtifactSpec("ns1", "auid1", "http://example.com/two", 1)
            .getArtifact());

    assertEquals("a second artifact in the same AU must not add an AUID row",
        1, countAuidRows());

    // A different AU gets its own.
    idxdb.addArtifact(
        makeArtifactSpec("ns1", "auid2", "http://example.com/three", 1)
            .getArtifact());

    assertEquals("a second AU must get its own AUID row", 2, countAuidRows());
  }

  /**
   * The count itself: it must agree with enumerating the AUIDs, must be scoped
   * to the namespace, and must not be inflated by extra URLs or versions within
   * an AU.
   */
  @Test
  public void testCountAuidsMatchesEnumeration() throws Exception {
    SQLArtifactIndexManagerSql idxdb =
        new SQLArtifactIndexManagerSql(idxDbManager);

    assertEquals("an empty index counts zero", 0, idxdb.countAuids("ns1"));

    // ns1: three AUs, one of them with several URLs and versions, which must
    // not make it count more than once.
    idxdb.addArtifact(makeArtifactSpec("ns1", "auid1", "http://a/1", 1).getArtifact());
    idxdb.addArtifact(makeArtifactSpec("ns1", "auid1", "http://a/1", 2).getArtifact());
    idxdb.addArtifact(makeArtifactSpec("ns1", "auid1", "http://a/2", 1).getArtifact());
    idxdb.addArtifact(makeArtifactSpec("ns1", "auid2", "http://b/1", 1).getArtifact());
    idxdb.addArtifact(makeArtifactSpec("ns1", "auid3", "http://c/1", 1).getArtifact());

    // ns2: one AU, plus one AUID that also appears in ns1. The same AUID in two
    // namespaces is one row in auids but must count once in each namespace.
    idxdb.addArtifact(makeArtifactSpec("ns2", "auid4", "http://d/1", 1).getArtifact());
    idxdb.addArtifact(makeArtifactSpec("ns2", "auid1", "http://a/1", 1).getArtifact());

    assertEquals("ns1 has three AUs", 3, idxdb.countAuids("ns1"));
    assertEquals("ns2 has two AUs", 2, idxdb.countAuids("ns2"));

    // The whole point is that this is the same answer as the slow path.
    assertEquals("the count must agree with enumerating the AUIDs",
        IterableUtils.size(idxdb.findAuids("ns1")), idxdb.countAuids("ns1"));
    assertEquals("the count must agree with enumerating the AUIDs",
        IterableUtils.size(idxdb.findAuids("ns2")), idxdb.countAuids("ns2"));
  }

  /**
   * A namespace nobody has written to counts zero rather than failing. COUNT
   * always returns a row, but the namespace join matches nothing.
   */
  @Test
  public void testCountAuidsOfUnknownNamespaceIsZero() throws Exception {
    SQLArtifactIndexManagerSql idxdb =
        new SQLArtifactIndexManagerSql(idxDbManager);

    idxdb.addArtifact(makeArtifactSpec("ns1", "auid1", "http://a/1", 1).getArtifact());

    assertEquals(0, idxdb.countAuids("no-such-namespace"));
  }

  /**
   * The index-level accessor the status table will use, over the same data.
   */
  @Test
  public void testSqlArtifactIndexReportsTheSameCount() throws Exception {
    SQLArtifactIndexManagerSql idxdb =
        new SQLArtifactIndexManagerSql(idxDbManager);

    idxdb.addArtifact(makeArtifactSpec("ns1", "auid1", "http://a/1", 1).getArtifact());
    idxdb.addArtifact(makeArtifactSpec("ns1", "auid2", "http://b/1", 1).getArtifact());

    SQLArtifactIndex index = new SQLArtifactIndex();
    index.init();

    assertEquals(2, index.getAuIdCount("ns1"));
    assertEquals(0, index.getAuIdCount("ns2"));
  }

  /** The number of rows in the auids table, read directly. */
  private long countAuidRows() throws Exception {
    try (Connection conn = idxDbManager.getConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT count(*) FROM auids")) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  private static ArtifactSpec makeArtifactSpec(String ns, String auid,
                                               String url, int version) {
    return new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(version)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(TimeBase.nowMs());
  }
}
