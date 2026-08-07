/*

Copyright (c) 2000-2025, Board of Trustees of Leland Stanford Jr. University

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
package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lockss.log.L4JLogger;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.lockss.config.db.SqlConstants.URL_DIGEST_EXPRESSION;
import static org.lockss.config.db.SqlConstants.URL_DIGEST_PARAM_EXPRESSION;

/**
 * Pins the concurrency semantics that {@code findOrCreate{Url,Auid,Namespace}Seq}
 * depend on, against a real PostgreSQL rather than against the documentation.
 *
 * <p>The interesting case is not two committed transactions - it is what the
 * loser does when the <em>winner rolls back</em>. Both upsert forms have to get
 * that right, and they get it right in different ways:
 *
 * <ul>
 *   <li>{@code DO UPDATE} (namespaces, auids) must fall through to a real
 *       INSERT, because there is no surviving row to update. If it instead
 *       raised a unique violation, or returned no row, the caller would NPE on
 *       unboxing - the original defect this whole change set exists to fix.
 *   <li>{@code DO NOTHING} (urls) must likewise insert. Its no-row result -
 *       which sends {@code findOrCreateUrlSeq} to its fallback SELECT - must
 *       occur <em>only</em> when the winner committed. If DO NOTHING could
 *       suppress the insert after an abort, the fallback SELECT would find
 *       nothing and the method would return null.
 * </ul>
 *
 * <p>Each test parks a transaction mid-insert, runs the upsert on a second
 * connection where it necessarily blocks on the first transaction's XID, then
 * resolves the first transaction and asserts what the second one did.
 */
public class TestSQLArtifactIndexUpsertRace extends LockssTestCase4 {
  private static final L4JLogger log = L4JLogger.getLogger();

  private static EmbeddedPostgres embeddedPg;

  private MockLockssDaemon theDaemon;
  private String dbName;
  private SQLArtifactIndexDbManager idxDbManager;
  private ExecutorService executor;

  private static final String NS = "the-namespace";
  private static final String URL = "http://example.com/raced";

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
    idxDbManager.startService();
    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);

    executor = Executors.newSingleThreadExecutor();
  }

  @Override
  public void tearDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
    if (idxDbManager != null) {
      idxDbManager.stopService();
    }
    theDaemon.stopDaemon();
    super.tearDown();
  }

  private Connection openConnection() throws SQLException {
    Connection conn = DriverManager.getConnection(
        "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName,
        "postgres", "postgres");
    conn.setAutoCommit(false);
    return conn;
  }

  private long countRows(String table) throws SQLException {
    try (Connection conn = openConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /**
   * Runs {@code sql} bound to {@code value} on its own connection, returning the
   * first column of the first row, or null if the statement returned no rows.
   */
  private Future<Long> upsertAsync(String sql, String value) {
    Callable<Long> task = () -> {
      try (Connection conn = openConnection();
           PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, value);

        try (ResultSet rs = ps.executeQuery()) {
          Long seq = rs.next() ? rs.getLong(1) : null;
          conn.commit();
          return seq;
        }
      }
    };

    return executor.submit(task);
  }

  /**
   * Waits until {@code future} has plausibly blocked inside the server. There is
   * no callback for "this statement is now waiting on an XID", so the test polls
   * pg_stat_activity for a waiting backend instead of sleeping blindly.
   */
  private void awaitBlocked() throws Exception {
    long deadline = System.currentTimeMillis() + 10_000;

    while (System.currentTimeMillis() < deadline) {
      try (Connection conn = openConnection();
           Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery(
               "SELECT count(*) FROM pg_stat_activity"
               + " WHERE wait_event_type = 'Lock' AND datname = '" + dbName + "'")) {
        rs.next();
        if (rs.getLong(1) > 0) {
          return;
        }
      }
      Thread.sleep(50);
    }

    fail("the second transaction never blocked on the first");
  }

  private static String upsertNamespaceSql() throws Exception {
    return readStaticString("UPSERT_NAMESPACE_QUERY");
  }

  private static String upsertUrlSql() throws Exception {
    return readStaticString("UPSERT_URL_QUERY");
  }

  private static String findUrlSeqSql() throws Exception {
    return readStaticString("FIND_URL_SEQ_QUERY");
  }

  private static String readStaticString(String name) throws Exception {
    java.lang.reflect.Field f =
        SQLArtifactIndexManagerSql.class.getDeclaredField(name);
    f.setAccessible(true);
    return (String) f.get(null);
  }

  /**
   * The question this class exists for: with ON CONFLICT DO UPDATE, a loser
   * whose winner aborted must insert, not fail and not update nothing.
   */
  @Test
  public void testDoUpdateInsertsWhenTheWinnerRollsBack() throws Exception {
    Connection a = openConnection();

    try (PreparedStatement ps =
             a.prepareStatement("INSERT INTO namespaces (namespace) VALUES (?)")) {
      ps.setString(1, NS);
      ps.executeUpdate();
    }
    // A is deliberately left uncommitted.

    Future<Long> b = upsertAsync(upsertNamespaceSql(), NS);
    awaitBlocked();

    a.rollback();
    a.close();

    Long bSeq = b.get(30, TimeUnit.SECONDS);

    assertNotNull("DO UPDATE must always return a row", bSeq);
    assertEquals("A rolled back, so B's row must be the only one", 1L,
        countRows("namespaces"));

    try (Connection conn = openConnection();
         PreparedStatement ps = conn.prepareStatement(
             "SELECT namespace_seq FROM namespaces WHERE namespace = ?")) {
      ps.setString(1, NS);

      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("the namespace must exist", rs.next());
        assertEquals("B must have returned the sequence of the row it inserted",
            bSeq.longValue(), rs.getLong(1));
      }
    }
  }

  /** The committed-winner case, for contrast: B updates rather than inserts. */
  @Test
  public void testDoUpdateReturnsTheWinnersRowWhenItCommits() throws Exception {
    Connection a = openConnection();
    long aSeq;

    try (PreparedStatement ps = a.prepareStatement(
             "INSERT INTO namespaces (namespace) VALUES (?)"
             + " RETURNING namespace_seq")) {
      ps.setString(1, NS);

      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        aSeq = rs.getLong(1);
      }
    }

    Future<Long> b = upsertAsync(upsertNamespaceSql(), NS);
    awaitBlocked();

    a.commit();
    a.close();

    Long bSeq = b.get(30, TimeUnit.SECONDS);

    assertNotNull("DO UPDATE must always return a row", bSeq);
    assertEquals("no second row may be created", 1L, countRows("namespaces"));
    assertEquals("B must have returned the winner's sequence", aSeq,
        bSeq.longValue());
  }

  /**
   * The urls form. DO NOTHING returning no row is what sends
   * findOrCreateUrlSeq to its fallback SELECT, so it must happen only when the
   * winner committed - never after an abort, where the fallback would find
   * nothing and the method would return null.
   */
  @Test
  public void testDoNothingInsertsWhenTheWinnerRollsBack() throws Exception {
    Connection a = openConnection();

    try (PreparedStatement ps =
             a.prepareStatement("INSERT INTO urls (url) VALUES (?)")) {
      ps.setString(1, URL);
      ps.executeUpdate();
    }

    Future<Long> b = upsertAsync(upsertUrlSql(), URL);
    awaitBlocked();

    a.rollback();
    a.close();

    Long bSeq = b.get(30, TimeUnit.SECONDS);

    assertNotNull("after the winner aborted, DO NOTHING must still insert and"
        + " return a row", bSeq);
    assertEquals("A rolled back, so B's row must be the only one", 1L,
        countRows("urls"));
  }

  /** DO NOTHING yields no row only when the winner really committed. */
  @Test
  public void testDoNothingYieldsNoRowWhenTheWinnerCommits() throws Exception {
    Connection a = openConnection();

    try (PreparedStatement ps =
             a.prepareStatement("INSERT INTO urls (url) VALUES (?)")) {
      ps.setString(1, URL);
      ps.executeUpdate();
    }

    Future<Long> b = upsertAsync(upsertUrlSql(), URL);
    awaitBlocked();

    a.commit();
    a.close();

    Long bSeq = b.get(30, TimeUnit.SECONDS);

    assertNull("DO NOTHING must suppress the insert once the winner committed,"
        + " which is what sends findOrCreateUrlSeq to its fallback SELECT",
        bSeq);
    assertEquals("no second row may be created", 1L, countRows("urls"));
  }

  /**
   * Pins what a digest collision would cost, and so what the digest choice is
   * buying.
   *
   * <p>Uniqueness is declared on a digest of the URL rather than on the URL,
   * because a btree index row is bounded and a URL is not. The price of that is
   * this: a digest collision does not produce a wrong row, it produces a URL
   * that cannot be stored at all. The upsert's {@code ON CONFLICT DO NOTHING}
   * suppresses the insert, the fallback SELECT's {@code url = ?} term correctly
   * refuses to match the other URL, and {@code findOrCreateUrlSeq()} turns the
   * empty result into a {@link org.lockss.db.DbException}.
   *
   * <p>Producing a genuine colliding pair would make the fixture opaque, and
   * for SHA-256 it is not possible at all. So the test substitutes a
   * deterministic collision injector - the production digest truncated to zero
   * bytes - which drives precisely the same
   * {@code ON CONFLICT DO NOTHING -> exact fallback SELECT} control flow with
   * every distinct URL colliding.
   *
   * <p>This is why {@code SqlConstants.URL_DIGEST_EXPRESSION} specifies
   * SHA-256: the behavior below is reached whenever two distinct URLs share a
   * digest, and MD5 collisions are constructible in seconds by anyone able to
   * choose two URLs the repository will crawl. The exact-comparison term bounds
   * the damage to a refused URL rather than a wrong one; only the strength of
   * the digest keeps the case out of reach.
   */
  @Test
  public void testDigestCollisionSuppressesDistinctUrlAndExactFallbackFindsNothing()
      throws Exception {
    String firstUrl = "http://example.com/first";
    String secondUrl = "http://example.com/second";

    // Same shape as the production expression, and constant for every input.
    String collisionExpression =
        "substring(" + URL_DIGEST_EXPRESSION + " from 1 for 0)";
    String collisionParamExpression =
        "substring(" + URL_DIGEST_PARAM_EXPRESSION + " from 1 for 0)";

    // Keep the test statements structurally identical to the production
    // statements; only substitute a deterministic collision-producing key.
    String upsertSql =
        upsertUrlSql().replace(URL_DIGEST_EXPRESSION, collisionExpression);
    String findSql = findUrlSeqSql()
        .replace(URL_DIGEST_EXPRESSION, collisionExpression)
        .replace(URL_DIGEST_PARAM_EXPRESSION, collisionParamExpression);

    assertNotEquals("the collision injector must actually replace something,"
            + " or this test silently exercises the production expression",
        upsertUrlSql(), upsertSql);

    try (Connection conn = openConnection();
         Statement st = conn.createStatement()) {
      st.executeUpdate("DROP INDEX idx4_urls");
      st.executeUpdate("CREATE UNIQUE INDEX idx4_urls ON urls ("
          + collisionExpression + ")");

      try (PreparedStatement first = conn.prepareStatement(upsertSql);
           PreparedStatement second = conn.prepareStatement(upsertSql);
           PreparedStatement fallback = conn.prepareStatement(findSql)) {
        first.setString(1, firstUrl);
        try (ResultSet rs = first.executeQuery()) {
          assertTrue("the first URL must insert", rs.next());
        }

        second.setString(1, secondUrl);
        try (ResultSet rs = second.executeQuery()) {
          assertFalse("the conflicting distinct URL is incorrectly suppressed",
              rs.next());
        }

        fallback.setString(1, secondUrl);
        fallback.setString(2, secondUrl);
        try (ResultSet rs = fallback.executeQuery()) {
          assertFalse("the exact fallback must not mistake the first URL for the"
              + " second", rs.next());
        }
      }

      conn.commit();
    }

    assertEquals("the distinct colliding URL was not stored", 1L,
        countRows("urls"));
  }
}
