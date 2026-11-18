/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.repository;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.lockss.config.db.SqlConstants.*;

/**
 * Tests for the race-free find-or-create of AUIDs in the repository database
 * and for the schema upgrade that merges the duplicates left behind by the
 * racy version.
 * <p>
 * Runs against embedded PostgreSQL: the upsert is PostgreSQL-only.
 */
public class TestRepositoryDbFindOrCreate extends LockssTestCase4 {

  private static final int THREADS = 8;
  private static final String AUID =
      "org|lockss|plugin|RacedPlugin&base_url~http%3A%2F%2Fexample%2Ecom%2F";
  private static final String OTHER_AUID =
      "org|lockss|plugin|RacedPlugin&base_url~http%3A%2F%2Fother%2Ecom%2F";
  private static final String SINGLE_AUID =
      "org|lockss|plugin|RacedPlugin&base_url~http%3A%2F%2Fsingle%2Ecom%2F";

  private static EmbeddedPostgres embeddedPg;

  private MockLockssDaemon theDaemon;
  private String dbName;
  private RepositoryDbManager dbManager;
  private ExecutorService executor;

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

    dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

    ConfigurationUtil.addFromArgs(
        RepositoryDbManager.PARAM_DATASOURCE_CLASSNAME,
        "org.postgresql.ds.PGSimpleDataSource",
        RepositoryDbManager.PARAM_DATASOURCE_DATABASENAME, dbName,
        RepositoryDbManager.PARAM_DATASOURCE_SERVERNAME, "localhost",
        RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
        String.valueOf(embeddedPg.getPort()));
    ConfigurationUtil.addFromArgs(
        RepositoryDbManager.PARAM_DATASOURCE_USER, "postgres",
        RepositoryDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");
    ConfigurationUtil.addFromArgs(
        RepositoryDbManager.PARAM_MAX_RETRY_COUNT, "0",
        RepositoryDbManager.PARAM_RETRY_DELAY, "0");

    executor = Executors.newFixedThreadPool(THREADS);
  }

  @Override
  public void tearDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (dbManager != null) {
      dbManager.stopService();
    }
    theDaemon.stopDaemon();
    super.tearDown();
  }

  interface Seeder {
    void seed(Connection conn) throws SQLException;
  }

  /**
   * Creates the database at {@code initialVersion}, lets {@code seeder} add
   * rows over a plain JDBC connection, then starts the manager with
   * {@code targetVersion} as its upgrade target.
   */
  private void startDbManager(int initialVersion, int targetVersion,
      Seeder seeder) throws Exception {
    // Bring a fresh database to the initial version through the normal
    // start-up path, exactly as a daemon of that vintage would have.
    dbManager = new RepositoryDbManager();
    dbManager.initService(theDaemon);
    dbManager.setTargetDatabaseVersion(initialVersion);
    dbManager.startService();
    assertTrue("RepositoryDbManager not ready at version " + initialVersion,
               dbManager.isReady());
    dbManager.stopService();

    if (seeder != null) {
      try (Connection conn = openJdbc()) {
        seeder.seed(conn);
      }
    }

    // Then start a daemon that targets the new version against that
    // database, which is what runs the upgrade.
    dbManager = new RepositoryDbManager();
    dbManager.initService(theDaemon);
    dbManager.setTargetDatabaseVersion(targetVersion);
    dbManager.startService();
    theDaemon.setRepositoryDbManager(dbManager);
    assertTrue("RepositoryDbManager not ready at version " + targetVersion,
               dbManager.isReady());
  }

  private void startDbManager() throws Exception {
    dbManager = new RepositoryDbManager();
    dbManager.initService(theDaemon);
    dbManager.startService();
    theDaemon.setRepositoryDbManager(dbManager);
    assertTrue("RepositoryDbManager not ready", dbManager.isReady());
  }

  /** A plain autocommit JDBC connection, independent of the manager. */
  private Connection openJdbc() throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName,
        "postgres", "postgres");
  }

  private long scalar(Connection conn, String sql, Object... args)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        ps.setObject(i + 1, args[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("No row for: " + sql, rs.next());
        return rs.getLong(1);
      }
    }
  }

  private void exec(Connection conn, String sql, Object... args)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        ps.setObject(i + 1, args[i]);
      }
      ps.executeUpdate();
    }
  }

  private boolean isUniqueIndex(Connection conn, String indexName)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(
        "select i.indisunique from pg_class c"
        + " join pg_index i on i.indexrelid = c.oid"
        + " where c.relname = ?")) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("Index " + indexName + " does not exist", rs.next());
        return rs.getBoolean(1);
      }
    }
  }

  private long insertAuid(Connection conn, String auid) throws SQLException {
    exec(conn, "insert into " + AUID_TABLE + "(" + AUID_COLUMN
               + ") values (?)", auid);
    return scalar(conn, "select max(" + AUID_SEQ_COLUMN + ") from "
                        + AUID_TABLE);
  }

  private void insertSize(Connection conn, long auidSeq, long marker)
      throws SQLException {
    exec(conn, "insert into " + ARCHIVAL_UNIT_SIZE_TABLE + "("
               + AUID_SEQ_COLUMN + "," + AU_LATEST_VERSIONS_SIZE_COLUMN + ","
               + AU_ALL_VERSIONS_SIZE_COLUMN + "," + AU_DISK_SIZE_COLUMN + ","
               + LAST_UPDATE_TIME_COLUMN + ") values (?,?,?,?,?)",
         auidSeq, marker, marker, marker, marker);
  }

  @Test
  public void testFreshInstallHasUniqueIndex() throws Exception {
    startDbManager();

    try (Connection conn = openJdbc()) {
      assertTrue(isUniqueIndex(conn, "idx2_" + AUID_TABLE));
    }
  }

  @Test
  public void testFindOrCreateAuidSeqRace() throws Exception {
    startDbManager();
    RepositoryManagerSql sql = new RepositoryManagerSql(dbManager);

    CyclicBarrier barrier = new CyclicBarrier(THREADS);
    List<Future<Long>> futures = new ArrayList<>();

    for (int i = 0; i < THREADS; i++) {
      futures.add(executor.submit((Callable<Long>) () -> {
        Connection conn = dbManager.getConnection();
        try {
          barrier.await(30, TimeUnit.SECONDS);
          Long seq = sql.findOrCreateAuidSeq(conn, AUID);
          conn.commit();
          return seq;
        } finally {
          RepositoryDbManager.safeRollbackAndClose(conn);
        }
      }));
    }

    Set<Long> seqs = new HashSet<>();
    for (Future<Long> f : futures) {
      Long seq = f.get(60, TimeUnit.SECONDS);
      assertNotNull(seq);
      seqs.add(seq);
    }

    assertEquals("Threads disagree on the AUID seq: " + seqs, 1, seqs.size());

    try (Connection conn = openJdbc()) {
      assertEquals(1, scalar(conn, "select count(*) from " + AUID_TABLE
                                   + " where " + AUID_COLUMN + " = ?", AUID));
      assertEquals(seqs.iterator().next().longValue(),
                   scalar(conn, "select " + AUID_SEQ_COLUMN + " from "
                                + AUID_TABLE + " where " + AUID_COLUMN + " = ?",
                          AUID));
    }
  }

  /**
   * Seeds a version-1 database with duplicate auids rows, runs the 1 -> 2
   * upgrade and checks the merge policy and the new unique index.
   */
  @Test
  public void testUpgrade1To2MergesDuplicateAuids() throws Exception {
    // AUID: three rows; only the oldest has a size row. The newest row
    // survives and the size row is dropped with the duplicates.
    final long[] seqs = new long[3];
    // OTHER_AUID: two rows, both with a size row; both size rows are dropped,
    // the survivor's included.
    final long[] otherSeqs = new long[2];
    // SINGLE_AUID: one row with a size row; not a duplicate, left alone.
    final long[] singleSeq = new long[1];

    startDbManager(1, 2, conn -> {
      for (int i = 0; i < 3; i++) {
        seqs[i] = insertAuid(conn, AUID);
      }
      insertSize(conn, seqs[0], 100L);

      for (int i = 0; i < 2; i++) {
        otherSeqs[i] = insertAuid(conn, OTHER_AUID);
      }
      insertSize(conn, otherSeqs[0], 200L);
      insertSize(conn, otherSeqs[1], 201L);

      singleSeq[0] = insertAuid(conn, SINGLE_AUID);
      insertSize(conn, singleSeq[0], 300L);
    });

    try (Connection conn = openJdbc()) {
      assertEquals(1, scalar(conn, "select count(*) from " + AUID_TABLE
                                   + " where " + AUID_COLUMN + " = ?", AUID));
      assertEquals(seqs[2], scalar(conn, "select " + AUID_SEQ_COLUMN + " from "
                                         + AUID_TABLE + " where " + AUID_COLUMN
                                         + " = ?", AUID));

      assertEquals(1, scalar(conn, "select count(*) from " + AUID_TABLE
                                   + " where " + AUID_COLUMN + " = ?",
                             OTHER_AUID));
      assertEquals(otherSeqs[1],
                   scalar(conn, "select " + AUID_SEQ_COLUMN + " from "
                                + AUID_TABLE + " where " + AUID_COLUMN + " = ?",
                          OTHER_AUID));

      // The size rows of both duplicated AUIDs are gone, the survivors'
      // included.
      assertEquals(0, scalar(conn, "select count(*) from "
                                   + ARCHIVAL_UNIT_SIZE_TABLE + " where "
                                   + AUID_SEQ_COLUMN + " in (?,?,?,?,?)",
                             seqs[0], seqs[1], seqs[2], otherSeqs[0],
                             otherSeqs[1]));

      // The non-duplicated AUID keeps its row and its size row.
      assertEquals(singleSeq[0],
                   scalar(conn, "select " + AUID_SEQ_COLUMN + " from "
                                + AUID_TABLE + " where " + AUID_COLUMN + " = ?",
                          SINGLE_AUID));
      assertEquals(300L, scalar(conn, "select " + AU_DISK_SIZE_COLUMN + " from "
                                      + ARCHIVAL_UNIT_SIZE_TABLE + " where "
                                      + AUID_SEQ_COLUMN + " = ?",
                                singleSeq[0]));

      // Nothing else is in the size table.
      assertEquals(1, scalar(conn, "select count(*) from "
                                   + ARCHIVAL_UNIT_SIZE_TABLE));

      assertTrue(isUniqueIndex(conn, "idx2_" + AUID_TABLE));
    }

    // And find-or-create now lands on the survivor.
    RepositoryManagerSql sql = new RepositoryManagerSql(dbManager);
    Connection conn = dbManager.getConnection();
    try {
      assertEquals(seqs[2], sql.findOrCreateAuidSeq(conn, AUID).longValue());
    } finally {
      RepositoryDbManager.safeRollbackAndClose(conn);
    }
  }
}
