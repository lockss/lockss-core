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
package org.lockss.config.db;

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
import java.sql.Statement;
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
 * Tests for the race-free find-or-create of plugins and Archival Units in the
 * configuration database and for the schema upgrade that merges the
 * duplicates left behind by the racy version.
 * <p>
 * Runs against embedded PostgreSQL: the upsert is PostgreSQL-only.
 */
public class TestConfigDbFindOrCreate extends LockssTestCase4 {

  private static final int THREADS = 8;
  private static final String PLUGIN_ID = "org|lockss|plugin|RacedPlugin";
  private static final String AU_KEY = "base_url~http%3A%2F%2Fexample%2Ecom%2F";

  private static EmbeddedPostgres embeddedPg;

  private MockLockssDaemon theDaemon;
  private String dbName;
  private ConfigDbManager dbManager;
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
        ConfigDbManager.PARAM_DATASOURCE_CLASSNAME,
        "org.postgresql.ds.PGSimpleDataSource",
        ConfigDbManager.PARAM_DATASOURCE_DATABASENAME, dbName,
        ConfigDbManager.PARAM_DATASOURCE_SERVERNAME, "localhost",
        ConfigDbManager.PARAM_DATASOURCE_PORTNUMBER,
        String.valueOf(embeddedPg.getPort()));
    ConfigurationUtil.addFromArgs(
        ConfigDbManager.PARAM_DATASOURCE_USER, "postgres",
        ConfigDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");
    ConfigurationUtil.addFromArgs(
        ConfigDbManager.PARAM_MAX_RETRY_COUNT, "0",
        ConfigDbManager.PARAM_RETRY_DELAY, "0");

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

  /**
   * Creates the database at {@code initialVersion}, lets {@code seeder} add
   * rows over a plain JDBC connection, then starts a manager with
   * {@code targetVersion} as its upgrade target.
   */
  private void startDbManager(int initialVersion, int targetVersion,
      Seeder seeder) throws Exception {
    // Bring a fresh database to the initial version through the normal
    // start-up path, exactly as a daemon of that vintage would have.
    dbManager = new ConfigDbManager();
    dbManager.initService(theDaemon);
    dbManager.setTargetDatabaseVersion(initialVersion);
    dbManager.startService();
    assertTrue("ConfigDbManager not ready at version " + initialVersion,
               dbManager.isReady());
    dbManager.stopService();

    if (seeder != null) {
      try (Connection conn = openJdbc()) {
        seeder.seed(conn);
      }
    }

    // Then start a daemon that targets the new version against that
    // database, which is what runs the upgrade.
    dbManager = new ConfigDbManager();
    dbManager.initService(theDaemon);
    dbManager.setTargetDatabaseVersion(targetVersion);
    dbManager.startService();
    theDaemon.setConfigDbManager(dbManager);
    assertTrue("ConfigDbManager not ready at version " + targetVersion,
               dbManager.isReady());
  }

  private void startDbManager() throws Exception {
    dbManager = new ConfigDbManager();
    dbManager.initService(theDaemon);
    dbManager.startService();
    theDaemon.setConfigDbManager(dbManager);
    assertTrue("ConfigDbManager not ready", dbManager.isReady());
  }

  interface Seeder {
    void seed(Connection conn) throws SQLException;
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

  private List<String> strings(Connection conn, String sql, Object... args)
      throws SQLException {
    List<String> result = new ArrayList<>();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        ps.setObject(i + 1, args[i]);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          result.add(rs.getString(1));
        }
      }
    }
    return result;
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

  /**
   * Runs {@code task} on THREADS threads, each released from a barrier at the
   * same moment, each on its own manager connection which it commits right
   * after the call. Returns the values the threads got.
   */
  private Set<Long> race(RacedCall task) throws Exception {
    CyclicBarrier barrier = new CyclicBarrier(THREADS);
    List<Future<Long>> futures = new ArrayList<>();

    for (int i = 0; i < THREADS; i++) {
      futures.add(executor.submit((Callable<Long>) () -> {
        Connection conn = dbManager.getConnection();
        try {
          barrier.await(30, TimeUnit.SECONDS);
          Long seq = task.call(conn);
          conn.commit();
          return seq;
        } finally {
          ConfigDbManager.safeRollbackAndClose(conn);
        }
      }));
    }

    Set<Long> seqs = new HashSet<>();
    for (Future<Long> f : futures) {
      Long seq = f.get(60, TimeUnit.SECONDS);
      assertNotNull(seq);
      seqs.add(seq);
    }
    return seqs;
  }

  interface RacedCall {
    Long call(Connection conn) throws Exception;
  }

  @Test
  public void testFreshInstallHasUniqueIndexes() throws Exception {
    startDbManager();

    try (Connection conn = openJdbc()) {
      assertTrue(isUniqueIndex(conn, "idx1_" + PLUGIN_TABLE));
      assertTrue(isUniqueIndex(conn, "idx3_" + ARCHIVAL_UNIT_TABLE));
    }
  }

  @Test
  public void testFindOrCreatePluginRace() throws Exception {
    startDbManager();
    ConfigManagerSql sql = new ConfigManagerSql(dbManager);

    Set<Long> seqs = race(conn -> sql.findOrCreatePlugin(conn, PLUGIN_ID));

    assertEquals("Threads disagree on the plugin seq: " + seqs, 1, seqs.size());

    try (Connection conn = openJdbc()) {
      assertEquals(1, scalar(conn, "select count(*) from " + PLUGIN_TABLE
                                   + " where " + PLUGIN_ID_COLUMN + " = ?",
                             PLUGIN_ID));
      assertEquals(seqs.iterator().next().longValue(),
                   scalar(conn, "select " + PLUGIN_SEQ_COLUMN + " from "
                                + PLUGIN_TABLE + " where " + PLUGIN_ID_COLUMN
                                + " = ?", PLUGIN_ID));
    }
  }

  @Test
  public void testFindOrCreateArchivalUnitRace() throws Exception {
    startDbManager();
    ConfigManagerSql sql = new ConfigManagerSql(dbManager);

    long pluginSeq;
    Connection setup = dbManager.getConnection();
    try {
      pluginSeq = sql.findOrCreatePlugin(setup, PLUGIN_ID);
      setup.commit();
    } finally {
      ConfigDbManager.safeRollbackAndClose(setup);
    }

    Set<Long> seqs =
        race(conn -> sql.findOrCreateArchivalUnit(conn, pluginSeq, AU_KEY));

    assertEquals("Threads disagree on the AU seq: " + seqs, 1, seqs.size());

    try (Connection conn = openJdbc()) {
      assertEquals(1, scalar(conn, "select count(*) from " + ARCHIVAL_UNIT_TABLE
                                   + " where " + PLUGIN_SEQ_COLUMN + " = ?"
                                   + " and " + ARCHIVAL_UNIT_KEY_COLUMN + " = ?",
                             pluginSeq, AU_KEY));
    }
  }

  /**
   * The upsert must not disturb an existing row's timestamps, and a second
   * find-or-create must return the existing row.
   */
  @Test
  public void testFindOrCreateArchivalUnitKeepsExistingTimes()
      throws Exception {
    startDbManager();
    ConfigManagerSql sql = new ConfigManagerSql(dbManager);

    Connection conn = dbManager.getConnection();
    try {
      long pluginSeq = sql.findOrCreatePlugin(conn, PLUGIN_ID);
      long first = sql.findOrCreateArchivalUnit(conn, pluginSeq, AU_KEY, 1000L);
      long second = sql.findOrCreateArchivalUnit(conn, pluginSeq, AU_KEY, 2000L);
      conn.commit();
      assertEquals(first, second);
    } finally {
      ConfigDbManager.safeRollbackAndClose(conn);
    }

    assertEquals(1000L,
                 sql.findArchivalUnitCreationTime(PLUGIN_ID, AU_KEY).longValue());
  }

  /**
   * Seeds a version-5 database with three archival_unit rows for one
   * (plugin, key) plus a bystander AU, runs the 5 -> 6 upgrade and checks
   * the merge policy and the new unique index.
   */
  @Test
  public void testUpgrade5To6MergesDuplicateArchivalUnits() throws Exception {
    final long[] seqs = new long[3]; // oldest, middle, newest
    final long[] other = new long[1];

    startDbManager(5, 6, conn -> {
      exec(conn, "insert into " + PLUGIN_TABLE + "(" + PLUGIN_ID_COLUMN
                 + ") values (?)", PLUGIN_ID);
      long pluginSeq = scalar(conn, "select " + PLUGIN_SEQ_COLUMN + " from "
                                    + PLUGIN_TABLE);

      for (int i = 0; i < 3; i++) {
        exec(conn, "insert into " + ARCHIVAL_UNIT_TABLE + "("
                   + PLUGIN_SEQ_COLUMN + "," + ARCHIVAL_UNIT_KEY_COLUMN + ","
                   + CREATION_TIME_COLUMN + "," + LAST_UPDATE_TIME_COLUMN
                   + ") values (?,?,?,?)",
             // The newest row (i == 2) has neither the earliest creation
             // time nor the latest update time.
             pluginSeq, AU_KEY, 1000L + i, 5002L - i);
        seqs[i] = scalar(conn, "select max(" + ARCHIVAL_UNIT_SEQ_COLUMN
                               + ") from " + ARCHIVAL_UNIT_TABLE);
      }
      assertTrue(seqs[0] < seqs[1] && seqs[1] < seqs[2]);

      exec(conn, "insert into " + ARCHIVAL_UNIT_TABLE + "("
                 + PLUGIN_SEQ_COLUMN + "," + ARCHIVAL_UNIT_KEY_COLUMN + ","
                 + CREATION_TIME_COLUMN + "," + LAST_UPDATE_TIME_COLUMN
                 + ") values (?,?,?,?)", pluginSeq, "other_key", 1L, 2L);
      other[0] = scalar(conn, "select max(" + ARCHIVAL_UNIT_SEQ_COLUMN
                              + ") from " + ARCHIVAL_UNIT_TABLE);

      // State: oldest and newest have one; the newest's must win.
      exec(conn, "insert into " + ARCHIVAL_UNIT_STATE_TABLE + " values (?,?)",
           seqs[0], "state-oldest");
      exec(conn, "insert into " + ARCHIVAL_UNIT_STATE_TABLE + " values (?,?)",
           seqs[2], "state-newest");
      exec(conn, "insert into " + ARCHIVAL_UNIT_STATE_TABLE + " values (?,?)",
           other[0], "state-other");

      // Agreements: only the oldest has one; it must move to the survivor.
      exec(conn, "insert into " + ARCHIVAL_UNIT_AGREEMENTS_TABLE
                 + " values (?,?)", seqs[0], "agreements-oldest");

      // Suspect URL versions: oldest and middle have one; the middle's (the
      // highest-numbered older duplicate) must move to the survivor.
      exec(conn, "insert into " + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE
                 + " values (?,?)", seqs[0], "suspect-oldest");
      exec(conn, "insert into " + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE
                 + " values (?,?)", seqs[1], "suspect-middle");

      // No AU peer set: nobody has one.

      // Config: oldest has two keys, newest has one; the newest's set is
      // kept as-is and the oldest's is dropped, not merged.
      exec(conn, "insert into " + ARCHIVAL_UNIT_CONFIG_TABLE
                 + " values (?,?,?)", seqs[0], "k1", "oldest-v1");
      exec(conn, "insert into " + ARCHIVAL_UNIT_CONFIG_TABLE
                 + " values (?,?,?)", seqs[0], "k2", "oldest-v2");
      exec(conn, "insert into " + ARCHIVAL_UNIT_CONFIG_TABLE
                 + " values (?,?,?)", seqs[2], "k1", "newest-v1");
    });

    try (Connection conn = openJdbc()) {
      long survivor = seqs[2];

      assertEquals(1, scalar(conn, "select count(*) from " + ARCHIVAL_UNIT_TABLE
                                   + " where " + ARCHIVAL_UNIT_KEY_COLUMN
                                   + " = ?", AU_KEY));
      assertEquals(survivor,
                   scalar(conn, "select " + ARCHIVAL_UNIT_SEQ_COLUMN + " from "
                                + ARCHIVAL_UNIT_TABLE + " where "
                                + ARCHIVAL_UNIT_KEY_COLUMN + " = ?", AU_KEY));
      // The survivor takes the earliest creation time and the latest update
      // time of the group.
      assertEquals(1000L, scalar(conn, "select " + CREATION_TIME_COLUMN
                                       + " from " + ARCHIVAL_UNIT_TABLE
                                       + " where " + ARCHIVAL_UNIT_SEQ_COLUMN
                                       + " = ?", survivor));
      assertEquals(5002L, scalar(conn, "select " + LAST_UPDATE_TIME_COLUMN
                                       + " from " + ARCHIVAL_UNIT_TABLE
                                       + " where " + ARCHIVAL_UNIT_SEQ_COLUMN
                                       + " = ?", survivor));

      String bySeq = " where " + ARCHIVAL_UNIT_SEQ_COLUMN + " = ?";

      assertEquals(List.of("state-newest"),
                   strings(conn, "select " + STATE_STRING_COLUMN + " from "
                                 + ARCHIVAL_UNIT_STATE_TABLE + bySeq, survivor));
      assertEquals(List.of("agreements-oldest"),
                   strings(conn, "select " + AGREEMENTS_STRING_COLUMN + " from "
                                 + ARCHIVAL_UNIT_AGREEMENTS_TABLE + bySeq,
                           survivor));
      assertEquals(List.of("suspect-middle"),
                   strings(conn, "select " + SUSPECT_URL_VERSIONS_STRING_COLUMN
                                 + " from "
                                 + ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE
                                 + bySeq, survivor));
      assertEquals(List.of(),
                   strings(conn, "select " + NO_AU_PEER_SET_STRING_COLUMN
                                 + " from " + ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE
                                 + bySeq, survivor));
      assertEquals(List.of("k1=newest-v1"),
                   strings(conn, "select " + CONFIG_KEY_COLUMN + " || '=' || "
                                 + CONFIG_VALUE_COLUMN + " from "
                                 + ARCHIVAL_UNIT_CONFIG_TABLE + bySeq
                                 + " order by " + CONFIG_KEY_COLUMN, survivor));

      // Nothing of the older duplicates is left in any child table.
      for (String table : new String[] {ARCHIVAL_UNIT_STATE_TABLE,
          ARCHIVAL_UNIT_AGREEMENTS_TABLE,
          ARCHIVAL_UNIT_SUSPECT_URL_VERSIONS_TABLE,
          ARCHIVAL_UNIT_NO_AU_PEER_SET_TABLE, ARCHIVAL_UNIT_CONFIG_TABLE}) {
        assertEquals(table, 0,
                     scalar(conn, "select count(*) from " + table + " where "
                                  + ARCHIVAL_UNIT_SEQ_COLUMN + " in (?,?)",
                            seqs[0], seqs[1]));
      }

      // The bystander is untouched.
      assertEquals(List.of("state-other"),
                   strings(conn, "select " + STATE_STRING_COLUMN + " from "
                                 + ARCHIVAL_UNIT_STATE_TABLE + bySeq, other[0]));

      assertTrue(isUniqueIndex(conn, "idx3_" + ARCHIVAL_UNIT_TABLE));
    }

    // And find-or-create now lands on the survivor.
    ConfigManagerSql sql = new ConfigManagerSql(dbManager);
    Connection conn = dbManager.getConnection();
    try {
      long pluginSeq = sql.findOrCreatePlugin(conn, PLUGIN_ID);
      assertEquals(seqs[2],
                   sql.findOrCreateArchivalUnit(conn, pluginSeq, AU_KEY)
                       .longValue());
    } finally {
      ConfigDbManager.safeRollbackAndClose(conn);
    }
  }

  /** An upgrade with no duplicates just adds the index. */
  @Test
  public void testUpgrade5To6WithoutDuplicates() throws Exception {
    startDbManager(5, 6, conn -> {
      exec(conn, "insert into " + PLUGIN_TABLE + "(" + PLUGIN_ID_COLUMN
                 + ") values (?)", PLUGIN_ID);
      long pluginSeq = scalar(conn, "select " + PLUGIN_SEQ_COLUMN + " from "
                                    + PLUGIN_TABLE);
      exec(conn, "insert into " + ARCHIVAL_UNIT_TABLE + "("
                 + PLUGIN_SEQ_COLUMN + "," + ARCHIVAL_UNIT_KEY_COLUMN + ","
                 + CREATION_TIME_COLUMN + "," + LAST_UPDATE_TIME_COLUMN
                 + ") values (?,?,?,?)", pluginSeq, AU_KEY, 1L, 2L);
    });

    try (Connection conn = openJdbc()) {
      assertEquals(1, scalar(conn, "select count(*) from "
                                   + ARCHIVAL_UNIT_TABLE));
      assertTrue(isUniqueIndex(conn, "idx3_" + ARCHIVAL_UNIT_TABLE));
    }
  }
}
