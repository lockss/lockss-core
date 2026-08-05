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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.lockss.config.db.SqlConstants.LONG_URL_TABLE;
import static org.lockss.config.db.SqlConstants.MAX_AUID_COLUMN;
import static org.lockss.config.db.SqlConstants.URL_PREFIX_INDEX_LENGTH;

/**
 * Tests that schema version 5 declares the C (byte-order) collation on the text
 * columns that are ordered or range-scanned, on both the fresh-create and the
 * version 4 upgrade path, and that {@code idx1_urls} is consequently usable by
 * the URL prefix range predicate.
 *
 * <p>Two distinct facts are asserted, and they are not the same fact:
 *
 * <ul>
 *   <li>the <b>column</b> collation, from
 *       {@code information_schema.columns.collation_name}; and
 *   <li>the <b>index</b> collation, from {@code pg_index.indcollation}.
 * </ul>
 *
 * <p>The index collation is the one the planner cares about. For {@code auids}
 * that means confirming the version 5 {@code ALTER} actually rebuilt the
 * pre-existing {@code idx2_auids} in the new collation rather than leaving it in
 * the database default.
 *
 * <p>{@code idx1_urls} is a different case: version 5 drops and recreates it,
 * because folding {@code long_urls} into {@code urls.url} leaves that column
 * unbounded and a btree index row cannot exceed 2704 bytes. Its replacement is
 * built over {@code left(url, URL_PREFIX_INDEX_LENGTH)}, which is why the prefix
 * predicate below is spelled against that expression.
 *
 * <p>Note that PostgreSQL reports {@code collation_name} as {@code NULL} when a
 * column carries its type's default collation, so the version 4 state is
 * {@code NULL} rather than the literal name of the cluster collation. The
 * upgrade test asserts that before/after transition rather than only the final
 * state, so that it cannot pass vacuously.
 *
 * @see SQLArtifactIndexDbManagerSql#updateDatabaseFrom4To5
 */
public class TestSQLArtifactIndexDbCollation extends LockssTestCase4 {
  private static final L4JLogger log = L4JLogger.getLogger();

  /** Shared embedded PostgreSQL instance for all tests in this class */
  private static EmbeddedPostgres embeddedPg;

  private MockLockssDaemon theDaemon;
  private String dbName;
  /** Where the migration writes its collision report. */
  private java.io.File dataDir;
  private final List<SQLArtifactIndexDbManager> startedManagers = new ArrayList<>();

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
    dataDir = new java.io.File(setUpDiskSpace());
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    // A database name unique to this test, so that a test can bring up a second
    // manager against the same database to exercise the upgrade path.
    dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, "org.postgresql.ds.PGSimpleDataSource",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_DATABASENAME, dbName,
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_SERVERNAME, "localhost",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PORTNUMBER, String.valueOf(embeddedPg.getPort()));

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");
  }

  @Override
  public void tearDown() throws Exception {
    for (int i = startedManagers.size() - 1; i >= 0; i--) {
      try {
        startedManagers.get(i).stopService();
      } catch (RuntimeException e) {
        log.warn("Failed to stop a db manager", e);
      }
    }
    startedManagers.clear();
    theDaemon.stopDaemon();
    super.tearDown();
  }

  /**
   * Brings up a {@link SQLArtifactIndexDbManager} against this test's database,
   * migrating it to {@code targetVersion}. Calling this twice with an increasing
   * target migrates the same database in place.
   */
  private SQLArtifactIndexDbManager startDbManagerAtVersion(int targetVersion) {
    SQLArtifactIndexDbManager mgr = new SQLArtifactIndexDbManager();
    mgr.initService(theDaemon);
    mgr.setTargetDatabaseVersion(targetVersion);
    mgr.startService();
    startedManagers.add(mgr);
    theDaemon.setSQLArtifactIndexDbManager(mgr);
    return mgr;
  }

  /** A direct connection to this test's database, bypassing the DbManager. */
  private Connection openRawConnection() throws SQLException {
    return DriverManager.getConnection(
        "jdbc:postgresql://localhost:" + embeddedPg.getPort() + "/" + dbName,
        "postgres", "postgres");
  }

  /**
   * The declared collation of a column, as PostgreSQL reports it: null when the
   * column carries its type's default collation, otherwise the collation name.
   */
  private String columnCollation(Connection conn, String table, String column)
      throws SQLException {
    String sql = "SELECT collation_name FROM information_schema.columns"
        + " WHERE table_name = ? AND column_name = ?";

    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, table);
      ps.setString(2, column);

      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("no such column: " + table + "." + column, rs.next());
        return rs.getString(1);
      }
    }
  }

  /**
   * The collation of an index's first key column, from {@code pg_index}. This is
   * the collation the planner matches a predicate against; "default" means the
   * database default collation.
   */
  private String indexCollation(Connection conn, String indexName)
      throws SQLException {
    String sql = "SELECT co.collname FROM pg_index i"
        + " JOIN pg_class cl ON cl.oid = i.indexrelid"
        + " LEFT JOIN pg_collation co ON co.oid = i.indcollation[0]"
        + " WHERE cl.relname = ?";

    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);

      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("no such index: " + indexName, rs.next());
        return rs.getString(1);
      }
    }
  }

  /** Asserts the ordered / range-scanned columns and their indexes are C-collated. */
  private void assertVersion5Collations(Connection conn) throws SQLException {
    assertEquals("urls.url must be declared COLLATE \"C\"",
        "C", columnCollation(conn, "urls", "url"));
    assertEquals("auids.auid must be declared COLLATE \"C\"",
        "C", columnCollation(conn, "auids", "auid"));

    // long_urls is not asserted because version 5 drops it: urls.url now holds
    // every URL whole, so there is no tail table to collate.
    assertFalse("version 5 must drop long_urls", tableExists(conn, LONG_URL_TABLE));

    // The column collation alone does not prove the pre-existing index was
    // rebuilt; that is what the planner actually matches against.
    assertEquals("idx2_auids must be a C-collated btree",
        "C", indexCollation(conn, "idx2_auids"));

    // idx1_urls survives in name only. The v4 index was a plain btree over
    // urls.url, which stops being legal once the column is unbounded: a btree
    // index row cannot exceed 2704 bytes. Version 5 drops it and rebuilds it
    // over left(url, N), so what must be asserted is not that it exists but
    // that it is the bounded expression index rather than the old column one -
    // an assertion on existence alone would pass against the very index that
    // makes long URLs unstorable.
    String urlIndexDef = indexDefinition(conn, "idx1_urls");
    assertTrue("idx1_urls must be built over left(url, "
            + URL_PREFIX_INDEX_LENGTH + "), not over the bare column; was:\n"
            + urlIndexDef,
        urlIndexDef.contains("left(" + "url, " + URL_PREFIX_INDEX_LENGTH + ")")
            || urlIndexDef.contains("\"left\"(url, " + URL_PREFIX_INDEX_LENGTH + ")"));

    // The uniqueness that version 4 removed, restored on a key that fits.
    assertTrue("idx4_urls must enforce uniqueness on md5(url)",
        indexExists(conn, "idx4_urls"));

    // ALTER COLUMN ... TYPE preserves NOT NULL, but silently losing it would be
    // a quiet data-integrity regression, so pin it.
    assertEquals("the auids.auid NOT NULL constraint must survive the migration",
        "NO", columnNullable(conn, "auids", "auid"));

    // The declared length of auids.auid must be unchanged; the ALTER respells the
    // type and so could silently resize it.
    assertEquals("auids.auid must keep its declared length",
        Integer.valueOf(MAX_AUID_COLUMN), columnLength(conn, "auids", "auid"));
  }

  /** Whether a table exists in the current database. */
  private boolean tableExists(Connection conn, String table) throws SQLException {
    String sql = "SELECT 1 FROM information_schema.tables WHERE table_name = ?";

    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, table);

      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** Whether an index exists in the current database. */
  private boolean indexExists(Connection conn, String indexName)
      throws SQLException {
    String sql = "SELECT 1 FROM pg_class WHERE relkind = 'i' AND relname = ?";

    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);

      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** The reconstructed {@code CREATE INDEX} statement for an index. */
  private String indexDefinition(Connection conn, String indexName)
      throws SQLException {
    String sql = "SELECT indexdef FROM pg_indexes WHERE indexname = ?";

    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, indexName);

      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("no such index: " + indexName, rs.next());
        return rs.getString(1);
      }
    }
  }

  /** {@code "YES"} or {@code "NO"}, per {@code information_schema}. */
  private String columnNullable(Connection conn, String table, String column)
      throws SQLException {
    return columnAttribute(conn, table, column, "is_nullable");
  }

  /** The declared character length, or null for an unbounded type. */
  private Integer columnLength(Connection conn, String table, String column)
      throws SQLException {
    String v = columnAttribute(conn, table, column, "character_maximum_length");
    return v == null ? null : Integer.valueOf(v);
  }

  private String columnAttribute(Connection conn, String table, String column,
                                 String attribute) throws SQLException {
    // The attribute name is a fixed identifier chosen by this class, never
    // caller input, so interpolating it is safe.
    String sql = "SELECT " + attribute + " FROM information_schema.columns"
        + " WHERE table_name = ? AND column_name = ?";

    try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, table);
      ps.setString(2, column);

      try (ResultSet rs = ps.executeQuery()) {
        assertTrue("no such column: " + table + "." + column, rs.next());
        return rs.getString(1);
      }
    }
  }

  /**
   * A fresh database is built by running the version steps in sequence from 1, so
   * it passes through the version 5 step and picks up the collation there.
   */
  @Test
  public void testFreshCreateIsCCollated() throws Exception {
    startDbManagerAtVersion(5);

    try (Connection conn = openRawConnection()) {
      assertVersion5Collations(conn);
    }
  }

  /**
   * The collision path. Two artifacts that land on the same (namespace, auid,
   * url, version) once duplicate URLs are collapsed must not stop the
   * migration, and must be written to a report file.
   *
   * <p>Has to be constructed deliberately: version 4 has no unique constraint on
   * {@code urls.url} - that is the defect being fixed - so duplicate URL rows
   * can simply be inserted, but no natural workload produces the version
   * collision on top of it. Measured zero on a 108M-row production index. This
   * is the only coverage the reporting path gets, because by construction it
   * never fires otherwise.
   */
  @Test
  public void testVersionCollisionsAreReportedAndMigrationContinues()
      throws Exception {
    startDbManagerAtVersion(4);

    String url = "http://example.com/collide,\"quoted\"";

    try (Connection conn = openRawConnection()) {
      try (Statement st = conn.createStatement()) {
        st.executeUpdate("INSERT INTO namespaces (namespace) VALUES ('ns1')");
        st.executeUpdate("INSERT INTO auids (auid) VALUES ('auid1')");

        // Two rows, same URL: legal at v4, and the whole problem.
        try (java.sql.PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO urls (url) VALUES (?)")) {
          ps.setString(1, url);
          ps.executeUpdate();
          ps.executeUpdate();
        }

        // One artifact per URL row, both claiming version 1. Once the two
        // url_seqs collapse into one, they collide.
        st.executeUpdate(
            "INSERT INTO artifacts (uuid, namespace_seq, auid_seq, url_seq,"
            + " version, committed, storage_url, length, digest, crawl_time)"
            + " SELECT 'uuid-' || u.url_seq, ns.namespace_seq, au.auid_seq,"
            + " u.url_seq, 1, true, 'store://' || u.url_seq, 10, 'sha1:x',"
            + " u.url_seq"
            + " FROM urls u, namespaces ns, auids au");
      }
    }

    // Must not throw: a data ambiguity is not a reason to block a schema change.
    startDbManagerAtVersion(5);

    try (Connection conn = openRawConnection()) {
      // The migration ran to completion despite the collision.
      assertFalse("version 5 must still drop long_urls",
          tableExists(conn, LONG_URL_TABLE));
      assertTrue("version 5 must still create idx4_urls",
          indexExists(conn, "idx4_urls"));

      // ...and the duplicate URL rows really were collapsed.
      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT count(*) FROM urls")) {
        assertTrue(rs.next());
        assertEquals("the duplicate url row must have been deleted", 1,
            rs.getInt(1));
      }
    }

    File report = new File(dataDir,
        SQLArtifactIndexDbManagerSql.COLLISION_REPORT_FILE);

    assertTrue("a collision report must have been written to " + report,
        report.exists());

    List<String> lines = java.nio.file.Files.readAllLines(report.toPath());

    assertEquals("expected a header and one line per colliding artifact, got:\n"
        + String.join("\n", lines), 3, lines.size());
    assertTrue("the header must name the columns; was: " + lines.get(0),
        lines.get(0).startsWith("namespace,auid,url,version"));

    // Both artifacts are listed, and the URL - which contains a comma and
    // quotes - survives CSV quoting intact.
    String body = lines.get(1) + "\n" + lines.get(2);
    assertTrue("both colliding artifacts must be listed; was:\n" + body,
        body.contains("uuid-1") && body.contains("uuid-2"));
    assertTrue("the URL must be CSV-quoted, not corrupted; was:\n" + body,
        body.contains("\"http://example.com/collide,\"\"quoted\"\"\""));
  }

  /**
   * The upgrade path: build the database at version 4, confirm it is in the old
   * (database default) collation, then bring a second manager up against the same
   * database with target version 5 and confirm the migration converted it.
   */
  @Test
  public void testUpgradeFrom4To5AppliesCollation() throws Exception {
    startDbManagerAtVersion(4);

    try (Connection conn = openRawConnection()) {
      // PostgreSQL reports null for a column carrying its type's default
      // collation, so this is the "not yet migrated" state.
      assertNull("urls.url should carry the database default collation at v4",
          columnCollation(conn, "urls", "url"));
      assertNull("auids.auid should carry the database default collation at v4",
          columnCollation(conn, "auids", "auid"));
      assertNull("long_urls.long_url should carry the database default collation at v4",
          columnCollation(conn, "long_urls", "long_url"));
      assertEquals("idx1_urls should be in the database default collation at v4",
          "default", indexCollation(conn, "idx1_urls"));
    }

    // Same database, higher target: only the 4 -> 5 step runs.
    startDbManagerAtVersion(5);

    try (Connection conn = openRawConnection()) {
      assertVersion5Collations(conn);
    }
  }

  /**
   * The point of the migration: the URL prefix range predicate, spelled exactly
   * as {@code SQLArtifactIndexManagerSql} assembles it, must be able to use
   * {@code idx1_urls}.
   *
   * <p>Asserted with {@code enable_seqscan = off}, which asks whether the index
   * is <em>usable</em> for the predicate rather than whether the planner prefers
   * it. That is the property the migration establishes; whether it wins on cost
   * depends on table size and selectivity, and pinning a cost-based choice would
   * make this test brittle. Before the migration this plan is a sequential scan
   * even with {@code enable_seqscan = off}, because a C-collated predicate simply
   * cannot be answered by a default-collated index.
   */
  @Test
  public void testUrlPrefixRangePredicateUsesUrlIndex() throws Exception {
    startDbManagerAtVersion(5);

    try (Connection conn = openRawConnection()) {
      try (Statement st = conn.createStatement()) {
        st.execute("INSERT INTO urls (url) SELECT 'http://example.com/'"
            + " || chr(97 + (i % 26)) || '/' || lpad(i::text, 8, '0')"
            + " FROM generate_series(1, 5000) i");
        st.execute("ANALYZE urls");
        st.execute("SET enable_seqscan = off");
      }

      String plan = explain(conn,
          "SELECT url_seq FROM urls"
          + " WHERE left(url, " + URL_PREFIX_INDEX_LENGTH + ") COLLATE \"C\""
          + " >= 'http://example.com/a'"
          + " AND left(url, " + URL_PREFIX_INDEX_LENGTH + ") COLLATE \"C\""
          + " < 'http://example.com/b'");

      log.debug("plan = {}", plan);

      assertTrue("the C-collated range predicate must be able to use idx1_urls;"
          + " plan was:\n" + plan, plan.contains("idx1_urls"));
      assertFalse("the plan must not fall back to a sequential scan; plan was:\n"
          + plan, plan.contains("Seq Scan"));
    }
  }

  /**
   * Every assembled variant of every templated query must parse and plan on a
   * real server.
   *
   * <p>This exists because the {@code COLLATE "C"} added to the {@code auid}
   * {@code ORDER BY} term is the kind of change a server can reject even though
   * it compiles: under {@code SELECT DISTINCT}, PostgreSQL requires each
   * {@code ORDER BY} expression to appear in the select list, and
   * {@code auid.auid COLLATE "C"} is a different expression from {@code
   * auid.auid}. Four of the eight edited {@code ORDER BY} clauses moreover live
   * in {@code LONG_URL_*} queries, which only run for URLs past the 2500
   * character split threshold and are otherwise thinly covered.
   *
   * <p>{@code PREPARE} is used rather than {@code EXPLAIN} because it lets
   * PostgreSQL infer the parameter types, so the check needs no per-query
   * knowledge of how many parameters each variant binds or of what type. A
   * successful {@code PREPARE} means the statement was parsed, analyzed and
   * planned.
   */
  @Test
  public void testAllAssembledQueryVariantsParse() throws Exception {
    startDbManagerAtVersion(5);

    String keysetAllAuids = readStaticString("KEYSET_WHERE_CLAUSE_ALL_AUIDS");
    String keysetPlain = readStaticString("KEYSET_WHERE_CLAUSE");
    String sortUriExpr = readStaticString("SORT_URI_EXPR");
    String sortAuid = readStaticString("SORT_AUID_EXPR");
    String prefixToken = readStaticString("URL_PREFIX_CONDITION_TOKEN");

    java.lang.reflect.Method prefixCond = SQLArtifactIndexManagerSql.class
        .getDeclaredMethod("urlPrefixCondition", String.class, boolean.class,
            boolean.class, boolean.class);
    prefixCond.setAccessible(true);

    // Every shape urlPrefixCondition can emit, as {idxBounded, recheck,
    // exactBounded}. The recheck arm only appears for a prefix longer than
    // URL_PREFIX_INDEX_LENGTH, and the two upper bounds are computed
    // independently, so a truncated range can be bounded where the exact one is
    // not.
    boolean[][] prefixShapes = {
        {true, false, false},   // within the index bound, upper bound found
        {false, false, false},  // within the bound, range open-ended
        {true, true, true},     // longer than the bound, both bounded
        {true, true, false},    // longer than the bound, exact range open-ended
    };

    int prepared = 0;
    int withAuidOrder = 0;

    try (Connection conn = openRawConnection()) {
      for (java.lang.reflect.Field f
          : SQLArtifactIndexManagerSql.class.getDeclaredFields()) {
        if (f.getType() != String.class
            || !java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
          continue;
        }

        f.setAccessible(true);
        String template = (String) f.get(null);

        // Only the templated, runtime-assembled queries; those are the paged
        // ones, and they are the ones carrying the edited ORDER BY.
        if (template == null
            || !template.startsWith("SELECT ")
            || !template.contains("--KeysetCondition--")) {
          continue;
        }

        // Schema version 6 folded long_urls back into urls.url, so there is a
        // single sortUri expression and the prefix predicate always binds
        // against u.url. Before that, both followed the query's long/short
        // branch and had to be selected per template.
        String prefixColumn = "u.url";

        // The all-AUIDs queries are exactly those whose ORDER BY carries the
        // AUID tiebreaker; they are the ones that take the extended keyset.
        boolean allAuids = template.contains(sortAuid);
        if (allAuids) {
          withAuidOrder++;
        }
        String keyset = (allAuids ? keysetAllAuids : keysetPlain)
            .replace("--SortUriExpr--", sortUriExpr);

        // Cursor present / absent, and (where applicable) a bounded / unbounded
        // prefix range: every combination the production code can emit.
        for (String keysetClause : new String[] {"", keyset}) {
          for (int shape = 0; shape < prefixShapes.length; shape++) {
            String sql = template.replace("--KeysetCondition--", keysetClause);

            // One query injects a MAX-version subquery, which itself carries a
            // committed-status placeholder.
            sql = sql.replace("--MaxVersionAllUrlsWithNamespaceAndAuid--",
                readStaticString("MAX_VERSION_OF_URL_WITH_NAMESPACE_AND_AUID_QUERY"));
            sql = sql.replace("--CommittedStatusCondition--",
                readStaticString("ARTIFACT_COMMITTED_STATUS_CONDITION"));

            if (sql.contains(prefixToken)) {
              boolean[] s = prefixShapes[shape];
              sql = sql.replace(prefixToken, (String) prefixCond.invoke(
                  null, prefixColumn, s[0], s[1], s[2]));
            } else if (shape > 0) {
              // No prefix predicate: the shape axis is degenerate.
              continue;
            }

            sql += " LIMIT ?";

            assertFalse("unsubstituted placeholder in " + f.getName() + ": " + sql,
                sql.contains("--") || sql.contains("@@"));

            // A short statement name: PostgreSQL truncates identifiers at 63
            // characters, and these constant names collide once truncated.
            prepare(conn, "q" + prepared++, f.getName(), sql);
          }
        }
      }
    }

    assertTrue("expected to have exercised many query variants, got " + prepared,
        prepared > 20);
    // A coverage tripwire, not a collation assertion. The floor was eight while
    // each all-AUIDs query had a LONG_URL_* twin; schema version 6 collapsed
    // those pairs, so four remain. If you added an all-AUIDs paged query, this
    // count simply needs bumping - nothing is wrong with the collation.
    assertTrue("expected at least the four known AUID-ordered queries to be"
        + " covered, found " + withAuidOrder + "; if you added an all-AUIDs paged"
        + " query, raise this floor", withAuidOrder >= 4);
  }

  /** Reads a private static String constant from the query class. */
  private String readStaticString(String name) throws Exception {
    java.lang.reflect.Field f =
        SQLArtifactIndexManagerSql.class.getDeclaredField(name);
    f.setAccessible(true);
    return (String) f.get(null);
  }

  /**
   * Asks the server to parse, analyze and plan {@code sql}, letting it infer the
   * parameter types. JDBC {@code ?} markers are renumbered to PostgreSQL
   * {@code $n} markers first.
   */
  private void prepare(Connection conn, String name, String label, String sql)
      throws SQLException {
    StringBuilder sb = new StringBuilder(sql.length() + 16);
    int n = 0;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '?') {
        sb.append('$').append(++n);
      } else {
        sb.append(c);
      }
    }

    try (Statement st = conn.createStatement()) {
      st.execute("PREPARE " + name + " AS " + sb);
    } catch (SQLException e) {
      throw new SQLException("failed to prepare " + label + ": " + sb, e);
    }
  }

  /** Returns the EXPLAIN output for a query as a single newline-joined string. */
  private String explain(Connection conn, String query) throws SQLException {
    StringBuilder sb = new StringBuilder();

    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery("EXPLAIN " + query)) {
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
    }

    return sb.toString();
  }
}
