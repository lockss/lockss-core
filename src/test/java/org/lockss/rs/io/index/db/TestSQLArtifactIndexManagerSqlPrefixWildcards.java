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
import org.apache.commons.collections4.IterableUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.VersionsEnum;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;

import java.net.URI;
import java.util.*;

import static org.lockss.config.db.SqlConstants.URL_PREFIX_INDEX_LENGTH;

/**
 * <b>Regression tests guarding literal URL-prefix matching.</b> These once
 * documented an unfixed defect and failed; the defect is fixed and they now pass.
 * They exist to keep it fixed.
 *
 * <p>The three URL-prefix query methods in {@link SQLArtifactIndexManagerSql}
 * &mdash; {@code fetchArtifactsByPrefixAllAuidsPage} (backing Q5,
 * {@code findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace}),
 * {@code fetchLatestArtifactsWithPrefixPage} (backing Q6,
 * {@code findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid})
 * and {@code fetchAllVersionsArtifactsWithPrefixPage} (backing Q7,
 * {@code findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid})
 * &mdash; used to build a SQL {@code LIKE} pattern by appending {@code "%"} to the
 * caller-supplied prefix <em>without escaping LIKE metacharacters</em>, on
 * predicates that carried no {@code ESCAPE} clause. Binding the pattern with
 * {@code PreparedStatement.setString()} prevents SQL <em>injection</em> but does
 * nothing about LIKE <em>metacharacters</em>, which the {@code LIKE} operator
 * interprets after the parameter is bound. The observable consequences were:
 *
 * <ol>
 *   <li>{@code %} in the prefix acted as a multi-character wildcard &rarr; <b>over-match</b>
 *       (a strict superset of the correct results).</li>
 *   <li>{@code _} in the prefix acted as a single-character wildcard &rarr; <b>over-match</b>.
 *       Underscores are common in real URLs, so this was the higher-frequency trigger.</li>
 *   <li>{@code \} is PostgreSQL's default LIKE escape character when no {@code ESCAPE}
 *       clause is given, so a prefix containing a backslash silently escaped the
 *       following character &rarr; <b>under-match</b>: the intended URL was missed.</li>
 *   <li><b>Length asymmetry</b>: a URL was split at 2500 characters across
 *       {@code urls.url} and {@code long_urls.long_url}, and the prefix was split to
 *       match. Its first 2500 characters were bound to an {@code =} predicate (literal,
 *       and therefore always correct) and only {@code prefix.substring(2500)} reached the
 *       {@code LIKE}. An identical {@code %} was thus a wildcard in a short prefix and a
 *       literal in a long one.</li>
 * </ol>
 *
 * <p>Schema version 5 folded {@code long_urls} back into {@code urls.url}, so that
 * head/tail split no longer exists. The asymmetry it created is nonetheless still the
 * right thing to guard, because the predicate acquired a <em>new</em> length-dependent
 * seam in the same change: the replacement {@code idx1_urls} is built over
 * {@code left(url, }{@link org.lockss.config.db.SqlConstants#URL_PREFIX_INDEX_LENGTH
 * URL_PREFIX_INDEX_LENGTH}{@code )}, so a prefix longer than that bound is matched by a
 * truncated range plus an exact recheck, while a shorter one is matched by the truncated
 * range alone. The threshold moved from 2500 to 600 and the mechanism changed entirely;
 * the invariant did not.
 *
 * <p><b>Contract asserted here.</b> Literal prefix matching, i.e.
 * {@code artifact.getUri().startsWith(prefix)}. That is what the two sibling index
 * implementations do: {@code VolatileArtifactIndex.getArtifactsWithUrlPrefixFromAllAus}
 * filters with {@code filterByURIPrefix} (a {@code startsWith}), and
 * {@code SolrArtifactIndex} uses a Solr {@code {!prefix f=uri}} query. The SQL index
 * was the odd one out.
 *
 * <p><b>How it is now implemented.</b> The prefix {@code LIKE} predicates were replaced
 * by a collation-explicit range,
 * {@code col COLLATE "C" >= prefix AND col COLLATE "C" < prefixUpperBound(prefix)},
 * with the upper bound computed by
 * {@link SQLArtifactIndexManagerSql#prefixUpperBound(String)} (unit-tested in
 * {@link TestSQLArtifactIndexManagerSqlPrefixUpperBound}). No test in this class
 * depends on <em>how</em> the fix is implemented; escaping plus an {@code ESCAPE}
 * clause would satisfy them equally.
 *
 * <p>These tests live in their own class, separate from
 * {@link TestSQLArtifactIndexManagerSqlPaging}, so that prefix-semantics failures are
 * distinguishable at a glance from paging failures. Both classes should stay green.
 *
 * <p><b>Note on page size:</b> unlike the paging test class, this class uses a large
 * page size so that keyset paging is entirely out of the picture. Every data set here is
 * small; a page boundary landing mid-result would produce a failure unrelated to prefix
 * semantics, which would muddy the evidence.
 *
 * <h3>Embedded PostgreSQL Lifecycle</h3>
 * <p>This test class uses the shared embedded PostgreSQL instance from
 * {@link LockssTestCase4}. Each test gets a unique database within that shared instance
 * for isolation. PostgreSQL matters here: consequence (3) above, the backslash
 * under-match, is PostgreSQL-specific. Derby follows the SQL standard, under which
 * {@code LIKE} has no default escape character.
 */
public class TestSQLArtifactIndexManagerSqlPrefixWildcards extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  /**
   * Deliberately large: paging is orthogonal to the defect under test, and every data
   * set in this class is far smaller than this.
   */
  private static final int TEST_PAGE_SIZE = 1000;

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private SQLArtifactIndexDbManager idxDbManager;

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

  @Override
  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    // Create a unique database for this test
    String dbName = "test_" + UUID.randomUUID().toString().replace("-", "");

    // Configure DbManager settings
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

    // Initialize the DbManager with a unique database for this test
    idxDbManager = new SQLArtifactIndexDbManager();
    idxDbManager.initService(theDaemon);
    idxDbManager.setTargetDatabaseVersion(5);
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

  private static ArtifactSpec makeArtifactSpec(String ns, String auid, String url, int version) {
    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(ns)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(version)
        .setStorageUrl(URI.create("storage_url"))
        .setContentLength(1)
        .setContentDigest("digest")
        .setCollectionDate(TimeBase.nowMs());

    log.debug2("spec = " + spec);
    return spec;
  }

  /**
   * Creates a SQLArtifactIndexManagerSql with the test page size configured.
   */
  private SQLArtifactIndexManagerSql createIndexManagerSql() {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);
    idxdb.setPageSize(TEST_PAGE_SIZE);
    return idxdb;
  }

  // ==========================================================================
  // Helpers
  // ==========================================================================

  /** Adds and commits one artifact per given URL, at version 1. */
  private void addCommitted(SQLArtifactIndexManagerSql idxdb, String ns, String auid,
                            String... urls) throws Exception {
    for (String url : urls) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }
  }

  /** Adds and commits versions 1..numVersions of each given URL. */
  private void addCommittedVersions(SQLArtifactIndexManagerSql idxdb, String ns, String auid,
                                    int numVersions, String... urls) throws Exception {
    for (String url : urls) {
      for (int v = 1; v <= numVersions; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }
  }

  /** The set of URIs in the result, sorted so assertion output is readable. */
  private static SortedSet<String> uris(List<Artifact> artifacts) {
    SortedSet<String> s = new TreeSet<>();
    for (Artifact a : artifacts) {
      s.add(a.getUri());
    }
    return s;
  }

  /** The set of "uri@version" keys in the result, sorted for readable output. */
  private static SortedSet<String> urisAndVersions(List<Artifact> artifacts) {
    SortedSet<String> s = new TreeSet<>();
    for (Artifact a : artifacts) {
      s.add(a.getUri() + "@v" + a.getVersion());
    }
    return s;
  }

  private static SortedSet<String> setOf(String... items) {
    return new TreeSet<>(Arrays.asList(items));
  }

  /**
   * Asserts the literal-prefix contract: every returned URI must literally start with
   * the query prefix, and the returned URI set must be exactly {@code expected}.
   */
  private static void assertLiteralPrefixResult(String prefix, SortedSet<String> expected,
                                                List<Artifact> actual) {
    SortedSet<String> actualUris = uris(actual);

    for (Artifact a : actual) {
      assertTrue("Prefix query with prefix [" + prefix + "] returned URI [" + a.getUri()
              + "], which does not literally start with the prefix. LIKE metacharacters in"
              + " the prefix were interpreted as wildcards. Full result set: " + actualUris,
          a.getUri().startsWith(prefix));
    }

    assertEquals("Prefix query with prefix [" + prefix + "] should return exactly the URIs"
            + " that literally start with it (" + expected.size() + " expected), but returned "
            + actualUris.size() + ": " + actualUris,
        expected, actualUris);
  }

  private static String repeat(char c, int n) {
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(c);
    }
    return sb.toString();
  }

  /**
   * Elides the middle of a very long string so that assertion messages about
   * multi-kilobyte URLs stay readable. Long URLs in this class differ only in their
   * head and tail, which is exactly what survives the elision.
   */
  private static String abbrev(String s) {
    if (s == null || s.length() <= 80) {
      return s;
    }
    return s.substring(0, 34) + "...<" + (s.length() - 68) + " chars>..."
        + s.substring(s.length() - 34);
  }

  /** {@link #abbrev} applied to every URI in the result, sorted. */
  private static SortedSet<String> abbrevUris(List<Artifact> artifacts) {
    SortedSet<String> s = new TreeSet<>();
    for (Artifact a : artifacts) {
      s.add(abbrev(a.getUri()));
    }
    return s;
  }

  // ==========================================================================
  // Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
  // ==========================================================================

  /**
   * A literal '%' in the prefix must match only a literal '%' in the stored URL.
   *
   * <p>Regression guard: under the old unescaped {@code LIKE}, the pattern
   * {@code "http://example.com/a%b/%"} made '%' a multi-character wildcard, so both
   * decoys were returned as well.
   */
  @Test
  public void testPercentInPrefixIsLiteral_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String literalMatch = "http://example.com/a%b/file.html";
    String wildcardDecoy = "http://example.com/aXYZb/file.html";
    String emptyMatchDecoy = "http://example.com/ab/file.html";

    addCommitted(idxdb, ns, "auid1", literalMatch, wildcardDecoy, emptyMatchDecoy);

    String prefix = "http://example.com/a%b/";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefix, VersionsEnum.ALL));

    assertLiteralPrefixResult(prefix, setOf(literalMatch), artifacts);
  }

  /**
   * A literal '_' in the prefix must match only a literal '_' in the stored URL.
   * Underscores occur constantly in real URLs, so this is the highest-frequency trigger
   * of the defect.
   *
   * <p>Regression guard: under the old unescaped {@code LIKE}, '_' was a
   * single-character wildcard, so both {@code myXpage} and {@code my-page} were
   * returned as well.
   */
  @Test
  public void testUnderscoreInPrefixIsLiteral_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String literalMatch = "http://example.com/my_page/file.html";
    String letterDecoy = "http://example.com/myXpage/file.html";
    String hyphenDecoy = "http://example.com/my-page/file.html";

    addCommitted(idxdb, ns, "auid1", literalMatch, letterDecoy, hyphenDecoy);

    String prefix = "http://example.com/my_page/";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefix, VersionsEnum.ALL));

    assertLiteralPrefixResult(prefix, setOf(literalMatch), artifacts);
  }

  /**
   * A prefix consisting of a bare "%" is a perfectly ordinary (if useless) literal
   * prefix: no stored URL starts with a percent sign, so the correct answer is the empty
   * set.
   *
   * <p>Regression guard: under the old unescaped {@code LIKE}, the pattern became
   * {@code "%%"}, which matched every row in the namespace. A caller asking for URLs
   * under "%" got the entire index.
   */
  @Test
  public void testBarePercentPrefixMatchesNothing_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    addCommitted(idxdb, ns, "auid1",
        "http://example.com/one.html",
        "http://example.com/two.html",
        "http://example.com/dir/three.html",
        "http://other.org/four.html",
        "http://other.org/dir/five.html");

    String prefix = "%";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefix, VersionsEnum.ALL));

    assertEquals("No stored URL literally starts with [" + prefix + "], so the prefix query"
            + " should return nothing, but it returned " + artifacts.size() + " artifact(s): "
            + uris(artifacts) + ". The bare '%' was interpreted as a match-everything"
            + " wildcard.",
        0, artifacts.size());
  }

  /**
   * The realistic trigger: percent-encoded octets in URLs. Any URL containing an escape
   * such as {@code %20}, {@code %2F} or {@code %7E} turns into a wildcard pattern when
   * used as a prefix.
   *
   * <p>Regression guard: under the old unescaped {@code LIKE}, the pattern
   * {@code ".../path/a%20b/%"} matched any URL of the form
   * {@code .../path/a<anything>20b/<anything>}.
   */
  @Test
  public void testPercentEncodedPrefixIsLiteral_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String literalMatch = "http://example.com/path/a%20b/doc.html";
    String wildcardDecoy = "http://example.com/path/aZZ20b/doc.html";

    addCommitted(idxdb, ns, "auid1", literalMatch, wildcardDecoy);

    String prefix = "http://example.com/path/a%20b/";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefix, VersionsEnum.ALL));

    assertLiteralPrefixResult(prefix, setOf(literalMatch), artifacts);
  }

  /**
   * A backslash in the prefix must be an ordinary character: the URL containing it is
   * returned, and no other.
   *
   * <p>This was the one <b>under-match</b> case. PostgreSQL treats {@code \} as
   * {@code LIKE}'s default escape character when the predicate carries no
   * {@code ESCAPE} clause &mdash; and none of the prefix predicates in
   * {@code SQLArtifactIndexManagerSql} had one. So the prefix
   * {@code http://example.com/a\b/} became the pattern
   * {@code http://example.com/a\b/%}, in which {@code \b} is an escaped (hence plain)
   * {@code b}; the pattern was effectively {@code http://example.com/ab/%}. The URL that
   * actually contains the backslash was never matched, while an unrelated one was.
   *
   * <p>That behaviour was PostgreSQL-specific. Derby follows the SQL standard, under
   * which {@code LIKE} has <em>no</em> default escape character and a backslash is just a
   * backslash. These tests run against embedded PostgreSQL, so the defect reproduced
   * here &mdash; and so does this guard against its return.
   */
  @Test
  public void testBackslashInPrefixIsLiteral_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    // Java literal "\\" is a single backslash character in the URL.
    String backslashUrl = "http://example.com/a\\b/file.html";
    String escapedAwayUrl = "http://example.com/ab/file.html";

    addCommitted(idxdb, ns, "auid1", backslashUrl, escapedAwayUrl);

    String prefix = "http://example.com/a\\b/";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefix, VersionsEnum.ALL));

    SortedSet<String> actualUris = uris(artifacts);

    assertLiteralPrefixResult(prefix, setOf(backslashUrl), artifacts);
  }

  // ==========================================================================
  // Q6: findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
  // ==========================================================================

  /**
   * Same defect through Q6 (latest committed version of each URL under a prefix, scoped
   * to one namespace + AUID). Three versions of each URL are stored; only the latest
   * version of the single literal match should come back.
   *
   * <p>Regression guard: under the old unescaped {@code LIKE}, the latest version of
   * each decoy was returned too.
   */
  @Test
  public void testPercentInPrefixIsLiteral_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int numVersions = 3;

    String literalMatch = "http://example.com/a%b/file.html";
    String wildcardDecoy = "http://example.com/aXYZb/file.html";
    String emptyMatchDecoy = "http://example.com/ab/file.html";

    addCommittedVersions(idxdb, ns, auid, numVersions,
        literalMatch, wildcardDecoy, emptyMatchDecoy);

    String prefix = "http://example.com/a%b/";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
            ns, auid, prefix));

    assertLiteralPrefixResult(prefix, setOf(literalMatch), artifacts);

    assertEquals("Q6 should return only the latest version of each matching URL for prefix ["
            + prefix + "], but returned " + urisAndVersions(artifacts),
        setOf(literalMatch + "@v" + numVersions), urisAndVersions(artifacts));
  }

  // ==========================================================================
  // Q7: findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
  // ==========================================================================

  /**
   * Same defect through Q7 (all committed versions of each URL under a prefix, scoped to
   * one namespace + AUID), using the underscore trigger. All three versions of the single
   * literal match should come back, and nothing from either decoy.
   *
   * <p>Regression guard: under the old unescaped {@code LIKE}, all versions of both
   * decoys were returned too.
   */
  @Test
  public void testUnderscoreInPrefixIsLiteral_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int numVersions = 3;

    String literalMatch = "http://example.com/my_page/file.html";
    String letterDecoy = "http://example.com/myXpage/file.html";
    String hyphenDecoy = "http://example.com/my-page/file.html";

    addCommittedVersions(idxdb, ns, auid, numVersions,
        literalMatch, letterDecoy, hyphenDecoy);

    String prefix = "http://example.com/my_page/";

    List<Artifact> artifacts = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
            ns, auid, prefix));

    assertLiteralPrefixResult(prefix, setOf(literalMatch), artifacts);

    assertEquals("Q7 should return every version of the one URL that literally starts with ["
            + prefix + "] and nothing from the decoy URLs, but returned "
            + urisAndVersions(artifacts),
        setOf(literalMatch + "@v1", literalMatch + "@v2", literalMatch + "@v3"),
        urisAndVersions(artifacts));
  }

  // ==========================================================================
  // Empty prefix (the unbounded, one-parameter form of the range predicate)
  // ==========================================================================

  /**
   * The empty prefix matches everything, and reaches a code path no other input does.
   *
   * <p>A literal prefix has an exclusive upper bound only if some code point in it can be
   * incremented; the empty string has none, so
   * {@link SQLArtifactIndexManagerSql#prefixUpperBound(String)} returns null and the
   * generated predicate degenerates to a lone {@code col COLLATE "C" >= ''}, which is
   * trivially true for every row. That form binds <em>one</em> parameter where the normal
   * form binds two, so it is a distinct query assembly with a distinct placeholder count
   * &mdash; and the empty prefix is the only input that produces it.
   *
   * <p>All three prefix queries are exercised: Q5 across all AUIDs (which is also the
   * only one of the three where the result spans more than one AUID), and Q6/Q7 scoped to
   * a single namespace and AUID.
   *
   * <p>Callers reach this through {@code StringUtil.isNullString}, so a null prefix
   * behaves identically; that path is covered too.
   */
  @Test
  public void testEmptyPrefixMatchesEverything_Q5_Q6_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String otherAuid = "auid2";
    String otherNs = "ns2";

    // Two versions of each URL in (ns1, auid1), so Q6 (latest only) and Q7 (all
    // versions) give different answers and neither can accidentally match the other.
    String url1 = "http://example.com/one.html";
    String url2 = "http://example.com/dir/two.html";
    String url3 = "http://zzz.example.org/three.html";
    addCommittedVersions(idxdb, ns, auid, 2, url1, url2, url3);

    // A second AUID in the same namespace: visible to Q5, invisible to Q6 and Q7.
    String url4 = "http://example.com/other-auid.html";
    addCommitted(idxdb, ns, otherAuid, url4);

    // A different namespace: invisible to all three.
    String url5 = "http://example.com/other-ns.html";
    addCommitted(idxdb, otherNs, auid, url5);

    SortedSet<String> nsUris = setOf(url1, url2, url3, url4);
    SortedSet<String> auUris = setOf(url1, url2, url3);

    // ---- Q5, all versions, every AUID in the namespace.
    List<Artifact> q5all = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, "", VersionsEnum.ALL));

    assertEquals("Q5 with an empty prefix should return every committed version in the"
            + " namespace, but returned " + urisAndVersions(q5all),
        setOf(url1 + "@v1", url1 + "@v2", url2 + "@v1", url2 + "@v2",
              url3 + "@v1", url3 + "@v2", url4 + "@v1"),
        urisAndVersions(q5all));
    assertLiteralPrefixResult("", nsUris, q5all);

    // ---- Q5, latest versions only.
    List<Artifact> q5latest = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, "", VersionsEnum.LATEST));

    assertEquals("Q5/LATEST with an empty prefix should return the latest version of every"
            + " URL in the namespace, but returned " + urisAndVersions(q5latest),
        setOf(url1 + "@v2", url2 + "@v2", url3 + "@v2", url4 + "@v1"),
        urisAndVersions(q5latest));

    // ---- Q5 with a null prefix must behave exactly like the empty string.
    List<Artifact> q5null = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, null, VersionsEnum.ALL));

    assertEquals("A null prefix must be treated as the empty prefix",
        urisAndVersions(q5all), urisAndVersions(q5null));

    // ---- Q6: latest version of each URL in (ns1, auid1).
    List<Artifact> q6 = IterableUtils.toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
            ns, auid, ""));

    assertEquals("Q6 with an empty prefix should return the latest version of every URL in"
            + " the AU, and nothing from another AU or namespace, but returned "
            + urisAndVersions(q6),
        setOf(url1 + "@v2", url2 + "@v2", url3 + "@v2"), urisAndVersions(q6));
    assertLiteralPrefixResult("", auUris, q6);

    // ---- Q7: every version of every URL in (ns1, auid1).
    List<Artifact> q7 = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
            ns, auid, ""));

    assertEquals("Q7 with an empty prefix should return every committed version of every"
            + " URL in the AU, and nothing from another AU or namespace, but returned "
            + urisAndVersions(q7),
        setOf(url1 + "@v1", url1 + "@v2", url2 + "@v1", url2 + "@v2",
              url3 + "@v1", url3 + "@v2"),
        urisAndVersions(q7));
    assertLiteralPrefixResult("", auUris, q7);
  }

  // ==========================================================================
  // Length asymmetry
  // ==========================================================================

  /**
   * Guards against the return of a <b>length asymmetry</b>: whether a '%' in the prefix
   * is treated as a wildcard must not depend on where in the prefix it sits.
   *
   * <p>The seam this straddles moved with schema version 5. It used to be the 2500
   * character head/tail split across {@code urls.url} and {@code long_urls.long_url},
   * where the head went to an {@code =} predicate and only the tail reached the
   * {@code LIKE}. That split is gone. The seam is now
   * {@link org.lockss.config.db.SqlConstants#URL_PREFIX_INDEX_LENGTH}: {@code idx1_urls}
   * is built over {@code left(url, N)}, so {@code urlPrefixCondition} emits a truncated
   * range over that expression, plus &mdash; only for a prefix longer than {@code N}
   * &mdash; an exact recheck against the bare column. A prefix therefore falls into one
   * of three classes, and this test covers all three:
   *
   * <ul>
   *   <li><b>Case 1 &mdash; prefix shorter than {@code N}.</b> The truncated range alone
   *       decides; there is no recheck term at all. {@code left(U,N)} starts with P if
   *       and only if {@code U} does, so the range is exact on its own.</li>
   *   <li><b>Case 2 &mdash; prefix longer than {@code N}, '%' <em>within</em> the first
   *       {@code N} characters.</b> The '%' appears in both the truncated range and the
   *       recheck, and must be literal in both.</li>
   *   <li><b>Case 3 &mdash; prefix longer than {@code N}, '%' <em>past</em> the first
   *       {@code N} characters.</b> The truncated range has collapsed to equality on
   *       {@code left(P,N)} and cannot see the '%' at all; only the exact recheck can
   *       exclude the decoy. This is the direct descendant of the old tail case, and the
   *       one that fails if the recheck term is dropped or mis-emitted.</li>
   * </ul>
   *
   * <p>Every decoy differs from its match by replacing the metacharacter with a run of
   * the same length, so a wildcard reading really would match it. The exclusions are
   * therefore genuine discriminators rather than length mismatches any implementation
   * would reject. Cases 2 and 3 also depend on the explicit {@code COLLATE "C"}: they
   * turn on 'Z' (0x5A) and 'X' (0x58) sorting above '%' (0x25), which a locale collation
   * weighting punctuation differently could reorder.
   *
   * <p>Long URLs are storable because {@code urls.url} is declared
   * {@code --PreferUnboundedTextType--} and, since version 5, holds the whole URL. The
   * btree limit that once forced the split is now handled by indexing
   * {@code left(url, N)} rather than by truncating the column.
   */
  @Test
  public void testLengthAsymmetryOfPercentHandling_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String base = "http://example.com/long/";

    // Case 1 data: a prefix comfortably below the index bound.
    String shortA = base + "a%b/fileA.html";        // literal match
    String shortC = base + "aXb/fileC.html";        // decoy: 'X' where the '%' is

    // Heads of exactly URL_PREFIX_INDEX_LENGTH characters, differing only at the marker
    // just after the base. headA carries a literal "a%b", headC an "aXb". Same LENGTH on
    // purpose, so a wildcard reading of headA really would match headC.
    String headA = base + "a%b" + repeat('z', URL_PREFIX_INDEX_LENGTH - base.length() - 3);
    String headC = base + "aXb" + repeat('z', URL_PREFIX_INDEX_LENGTH - base.length() - 3);

    assertEquals("headA must be exactly URL_PREFIX_INDEX_LENGTH chars",
        URL_PREFIX_INDEX_LENGTH, headA.length());
    assertEquals("headC must be exactly URL_PREFIX_INDEX_LENGTH chars",
        URL_PREFIX_INDEX_LENGTH, headC.length());

    // Case 2 data: tails carry no metacharacters, so only the in-bound '%' is in question.
    String urlA = headA + "plainTail/fileA.html";   // literal match
    String urlC = headC + "plainTail/fileC.html";   // decoy differing inside the bound

    // Case 3 data: identical in-bound heads, differing only past the bound.
    String urlD = headA + "q%r/fileD.html";         // literal match
    String urlE = headA + "qZZr/fileE.html";        // decoy differing past the bound

    addCommitted(idxdb, ns, "auid1", shortA, shortC, urlA, urlC, urlD, urlE);

    // ---- Case 1: prefix below the index bound -> truncated range only, no recheck.
    String shortPrefix = base + "a%b/";
    assertTrue("Case 1 prefix must be shorter than URL_PREFIX_INDEX_LENGTH so that"
            + " urlPrefixCondition emits no recheck term",
        shortPrefix.length() < URL_PREFIX_INDEX_LENGTH);

    List<Artifact> case1 = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, shortPrefix, VersionsEnum.ALL));

    assertEquals("Case 1: a prefix shorter than " + URL_PREFIX_INDEX_LENGTH
            + " is matched by the truncated range alone, which is exact at that length."
            + " Only the URL ending 'fileA.html' should be returned, not the 'aXb' decoy"
            + " ending 'fileC.html'. Returned " + case1.size() + ": " + abbrevUris(case1),
        setOf(abbrev(shortA)), abbrevUris(case1));

    // ---- Case 2: '%' inside the first N chars -> present in both range and recheck.
    String prefixHeadPercent = headA + "plainTail/";
    assertTrue("Case 2 prefix must exceed URL_PREFIX_INDEX_LENGTH so that the recheck"
            + " term is emitted",
        prefixHeadPercent.length() > URL_PREFIX_INDEX_LENGTH);

    List<Artifact> case2 = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefixHeadPercent, VersionsEnum.ALL));

    assertEquals("Case 2: the '%' at offset " + base.length() + " falls within the first "
            + URL_PREFIX_INDEX_LENGTH + " characters, so it appears in the truncated range"
            + " and in the recheck, and must be literal in both. Only the URL ending"
            + " 'fileA.html' should be returned, not the 'aXb' decoy ending 'fileC.html'."
            + " Returned " + case2.size() + ": " + abbrevUris(case2),
        setOf(abbrev(urlA)), abbrevUris(case2));

    // ---- Case 3: '%' past the first N chars -> only the exact recheck can see it.
    String prefixTailPercent = headA + "q%r/";
    assertTrue("Case 3 prefix must exceed URL_PREFIX_INDEX_LENGTH so that the recheck"
            + " term is emitted",
        prefixTailPercent.length() > URL_PREFIX_INDEX_LENGTH);

    // The decoy must be invisible to the truncated range, or this case silently
    // degenerates into case 2 and stops testing the recheck at all: the range is an
    // equality on left(url, N) once the prefix is longer than N, so the decoy can only
    // survive it by agreeing with the match over those first N characters.
    assertEquals("Case 3's decoy must be indistinguishable from its match within the"
            + " first " + URL_PREFIX_INDEX_LENGTH + " characters, so that only the exact"
            + " recheck can separate them",
        urlD.substring(0, URL_PREFIX_INDEX_LENGTH),
        urlE.substring(0, URL_PREFIX_INDEX_LENGTH));
    assertEquals("...and must agree with the prefix over those characters too",
        prefixTailPercent.substring(0, URL_PREFIX_INDEX_LENGTH),
        urlE.substring(0, URL_PREFIX_INDEX_LENGTH));

    List<Artifact> case3 = IterableUtils.toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, prefixTailPercent, VersionsEnum.ALL));

    assertEquals("Case 3: the '%' at offset " + URL_PREFIX_INDEX_LENGTH + "+1 lies past"
            + " the indexed prefix, where the truncated range has collapsed to equality"
            + " and cannot discriminate. Only the exact recheck against the bare column"
            + " can exclude the 'qZZr' decoy ending 'fileE.html', so a failure here means"
            + " the recheck term is missing, mis-emitted, or treating '%' as a wildcard"
            + " -- i.e. whether '%' is literal depends on its offset again. Returned "
            + case3.size() + ": " + abbrevUris(case3),
        setOf(abbrev(urlD)), abbrevUris(case3));
  }
}
