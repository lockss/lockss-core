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
import org.lockss.config.Configuration;
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
import java.util.function.Function;

/**
 * Tests for methods in {@link SQLArtifactIndexManagerSql} that use keyset paging.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Paging correctly iterates through large result sets</li>
 *   <li>No artifacts are skipped or duplicated across page boundaries</li>
 *   <li>Sort order is maintained across pages</li>
 *   <li>Edge cases (exact page size multiples, single item, empty) are handled</li>
 * </ul>
 *
 * <p>Query methods under test (referenced as Q1-Q7 in test method names):
 * <ul>
 *   <li><b>Q1</b>: {@link SQLArtifactIndexManagerSql#findLatestArtifactsOfAllUrlsWithNamespaceAndAuid findLatestArtifactsOfAllUrlsWithNamespaceAndAuid}
 *       - Returns latest version of each URL in a namespace/AUID</li>
 *   <li><b>Q2</b>: {@link SQLArtifactIndexManagerSql#findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid}
 *       - Returns all versions of all URLs in a namespace/AUID</li>
 *   <li><b>Q3</b>: {@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid}
 *       - Returns all committed versions of a specific URL</li>
 *   <li><b>Q4</b>: {@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace}
 *       - Returns all versions of a URL across all AUIDs in a namespace</li>
 *   <li><b>Q5</b>: {@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace}
 *       - Returns all versions of URLs matching a prefix across all AUIDs</li>
 *   <li><b>Q6</b>: {@link SQLArtifactIndexManagerSql#findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid}
 *       - Returns latest version of URLs matching a prefix in a namespace/AUID</li>
 *   <li><b>Q7</b>: {@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid}
 *       - Returns all versions of URLs matching a prefix in a namespace/AUID</li>
 * </ul>
 *
 * <p>Tests use a small page size (10) and smaller data sets to speed up execution
 * while still exercising multi-page scenarios.</p>
 *
 * <h3>Embedded PostgreSQL Lifecycle</h3>
 * <p>This test class uses the shared embedded PostgreSQL instance from
 * {@link LockssTestCase4} for efficiency. Each test gets a unique database
 * within the shared PostgreSQL instance for isolation.
 */
public class TestSQLArtifactIndexManagerSqlPaging extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  /** Small page size for testing - allows testing paging with fewer artifacts */
  private static final int TEST_PAGE_SIZE = 10;

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
    idxDbManager.setTargetDatabaseVersion(4);
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

  // ============================================================================
  // Paging Tests
  // ============================================================================
  // Tests that verify basic paging functionality across multiple pages of results.

  /**
   * Tests that paging works correctly with a large number of URLs,
   * ensuring all artifacts are returned without duplicates or gaps.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testPaging_ManyUrls_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    SortedMap<String, String> sortUris = new TreeMap<>();
    List<String> unorderedUris = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      String uri = String.format("http://example.com/a-aa/%05d", i);
      sortUris.put(uri.replace("/", "\t"), uri);
      unorderedUris.add(uri);
    }

    for (int i = 0; i < 10; i++) {
      String uri = String.format("http://example.com/a/%05d", i);
      sortUris.put(uri.replace("/", "\t"), uri);
      unorderedUris.add(uri);
    }

    for (int i = 0; i < 15; i++) {
      String uri = String.format("http://example.com/aaa-/%05d", i);
      sortUris.put(uri.replace("/", "\t"), uri);
      unorderedUris.add(uri);
    }

    List<String> sortedUris = new ArrayList<>(unorderedUris);
    Collections.sort(sortedUris);
    
    // Assert that the canonical order by Collections.sort() and the sort by TreeMap are different
    assertNotEquals(sortedUris, sortUris.values());

    // Create enough URLs to span multiple pages (test page size is 10)
    int numUrls = 35;
    List<ArtifactSpec> latestSpecs = new ArrayList<>();
    for (int i = 0; i < numUrls; i++) {
      // Create multiple versions for each URL to test that only latest is returned
      ArtifactSpec specV1 = makeArtifactSpec(ns, auid, unorderedUris.get(i), 1);
      ArtifactSpec specV2 = makeArtifactSpec(ns, auid, unorderedUris.get(i), 2);

      idxdb.addArtifact(specV1.getArtifact());
      idxdb.addArtifact(specV2.getArtifact());

      // Commit both versions
      idxdb.commitArtifact(specV1.getArtifactUuid());
      idxdb.commitArtifact(specV2.getArtifactUuid());

      latestSpecs.add(specV2); // Only track latest version
    }

    // Query for latest artifacts which will be returned in an order on sortUri field
    List<Artifact> artifacts = 
        toList(idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    // Verify count matches expected
    assertEquals("Should return exactly one artifact per URL", numUrls, artifacts.size());

    // Verify artifacts are returned in proper sort order
    List<String> orderedSortUris = new ArrayList<>(sortUris.values());

    for (int i = 0; i < numUrls; i++) {
      assertEquals(artifacts.get(i).getUri(), orderedSortUris.get(i));
    }

    assertSorted(orderedSortUris, (uri) -> uri.replace("/", "\t"));
    assertSorted(artifacts, (artifact) -> artifact.getUri().replace("/", "\t"));
  }

  /**
   * Tests paging with a result set that is exactly a multiple of the page size.
   * This is an edge case where the iterator needs to correctly detect the end.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testPaging_ExactPageSizeMultiple_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create exactly 20 URLs (2 pages with test page size of 10)
    int numUrls = 20;
    Set<String> expectedUrls = new HashSet<>();

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/item/%05d", i);
      expectedUrls.add(url);

      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query for latest artifacts
    List<Artifact> artifacts =
        toList(idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return all artifacts", numUrls, artifacts.size());

    // Verify no duplicates
    Set<String> returnedUrls = new HashSet<>();
    for (Artifact a : artifacts) {
      assertTrue("Duplicate URL found: " + a.getUri(), returnedUrls.add(a.getUri()));
    }
  }

  /**
   * Tests paging with URLs that differ only at page boundaries,
   * ensuring the keyset cursor correctly handles the transition.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testPaging_PageBoundaryUrlSimilarity_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create URLs that are very similar and will sort near page boundaries
    // Using URLs that would sort consecutively
    int numUrls = 15; // Just over one page (test page size is 10)

    SortedMap<String, String> sortUriMap = new TreeMap<>();
    List<String> uriList = new ArrayList<>();

    for (int i = 0; i < numUrls; i++) {
      String basePath = i % 2 == 0 ? "http://example.com/aa-/" : "http://example.com/aa/";
      String uri = basePath + String.format("%05d", i);
      sortUriMap.put(uri.replace("/", "\t"), uri);
      uriList.add(uri);

      ArtifactSpec spec = makeArtifactSpec(ns, auid, uri, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<String> sortedUriList = new ArrayList<>(uriList);
    Collections.sort(sortedUriList);
    assertNotEquals(sortedUriList, sortUriMap.values());

    List<Artifact> artifacts =
        toList(idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    // Verify count matches expected
    assertEquals("Should return exactly one artifact per URL", numUrls, artifacts.size());

    // Verify artifacts are returned in proper sort order
    for (int i = 0; i < numUrls; i++) {
      List<String> sortUris = new ArrayList<>(sortUriMap.values());
      assertEquals(artifacts.get(i).getUri(), sortUris.get(i));
    }
  }

  /**
   * Tests that iterating multiple times over the same query returns consistent results.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testPaging_MultipleIterations_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    int numUrls = 25;
    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/path/%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> itr1 =
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();
    Iterator<Artifact> itr2 =
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    while (itr1.hasNext()) {
      Artifact a1 = itr1.next();
      Artifact a2 = itr2.next();
      assertEquals("Multiple iterations should return same results", a1, a2);
    }

    assertFalse(itr2.hasNext());
  }

  /**
   * Tests paging behavior when partial iteration is performed (early break).
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testPaging_PartialIteration_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    int numUrls = 35;
    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/path/%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Partial iteration - only get first 5 (less than one page)
    int partialCount = 5;
    List<Artifact> partial = new ArrayList<>();
    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    for (int i = 0; i < partialCount && iter.hasNext(); i++) {
      partial.add(iter.next());
    }

    assertEquals("Should be able to partially iterate", partialCount, partial.size());

    // Full iteration should still work
    List<Artifact> full = new ArrayList<>();
    for (Artifact a : idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false)) {
      full.add(a);
    }

    assertEquals("Full iteration should return all artifacts", numUrls, full.size());

    // First five from partial should match first five from full
    for (int i = 0; i < partialCount; i++) {
      assertEquals("Partial iteration should match full iteration order",
          partial.get(i).getUri(), full.get(i).getUri());
    }
  }

  /**
   * Tests that paging returns all versions of all URLs correctly.
   * Q2: findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testPaging_ManyUrlsAndVersions_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create URLs with multiple versions each to span multiple pages
    int numUrls = 10;
    int versionsPerUrl = 4; // 10 * 4 = 40 artifacts, spanning 4 pages
    int totalArtifacts = numUrls * versionsPerUrl;

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/path/%05d", i);

      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query for all versions
    Iterable<Artifact> result = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);

    // Collect all results
    List<Artifact> artifacts = new ArrayList<>();
    for (Artifact a : result) {
      artifacts.add(a);
    }

    assertEquals("Should return all artifacts (all versions)", totalArtifacts, artifacts.size());

    // Verify no duplicates (by UUID)
    Set<String> returnedUuids = new HashSet<>();
    for (Artifact a : artifacts) {
      assertTrue("Duplicate artifact UUID found: " + a.getUuid(), returnedUuids.add(a.getUuid()));
    }

    // Verify each URL has all versions
    Map<String, Set<Integer>> urlVersions = new HashMap<>();
    for (Artifact a : artifacts) {
      urlVersions.computeIfAbsent(a.getUri(), k -> new HashSet<>()).add(a.getVersion());
    }

    assertEquals("Should have all URLs", numUrls, urlVersions.size());
    for (Map.Entry<String, Set<Integer>> entry : urlVersions.entrySet()) {
      assertEquals("URL " + entry.getKey() + " should have all versions",
          versionsPerUrl, entry.getValue().size());
    }
  }

  /**
   * Tests paging when fetching all versions of a single URL.
   * Q3: findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid
   */
  @Test
  public void testPaging_ManyVersions_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/many-versions";

    // Create many versions to span multiple pages (test page size is 10)
    int numVersions = 35;

    for (int v = 1; v <= numVersions; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query for all versions of this URL
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url);

    // Collect all results
    List<Artifact> artifacts = new ArrayList<>();
    for (Artifact a : result) {
      artifacts.add(a);
    }

    assertEquals("Should return all versions", numVersions, artifacts.size());

    // Verify all versions are present
    Set<Integer> returnedVersions = new HashSet<>();
    int artifactVersion = numVersions;
    for (Artifact a : artifacts) {
      assertEquals("All artifacts should have the correct URL", url, a.getUri());
      assertTrue("Duplicate version found: " + a.getVersion(), returnedVersions.add(a.getVersion()));
      assertEquals("Wrong version:", (long) artifactVersion--, (long) a.getVersion());
    }
  }

  /**
   * Tests fetching artifacts for a URL across multiple AUIDs with multi-page results.
   * This verifies that the extended keyset pagination (sortUri, auid, version) correctly
   * handles cases where the same URL exists across multiple AUIDs with the same versions.
   * Q4: findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace
   */
  @Test
  public void testPaging_MultipleAuids_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared-url";

    // Create enough artifacts across AUIDs to span multiple pages
    // TEST_PAGE_SIZE is 10, so we need > 10 artifacts to test pagination
    int numAuids = 5;
    int versionsPerAuid = 3; // 5 * 3 = 15 artifacts, spanning 2 pages
    int totalArtifacts = numAuids * versionsPerAuid;

    for (int a = 0; a < numAuids; a++) {
      String auid = String.format("auid%05d", a);
      for (int v = 1; v <= versionsPerAuid; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query for all versions
    List<Artifact> artifacts =
        toList(idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals("Should return all artifacts across all AUIDs", totalArtifacts, artifacts.size());

    // Verify all AUIDs are represented
    Set<String> returnedAuids = new HashSet<>();
    for (Artifact artifact : artifacts) {
      returnedAuids.add(artifact.getAuid());
      assertEquals("All artifacts should have the correct URL", url, artifact.getUri());
    }

    assertEquals("Should have artifacts from all AUIDs", numAuids, returnedAuids.size());
  }
  
  <T> List<T> toList(Iterable<T> itr) {
    List<T> list = new ArrayList<>();
    for (T t : itr) {
      list.add(t);
    }
    return list;
  }

  <T, C extends Comparable<? super C>> void assertSorted(Collection<T> objs, Function<T, C> sortFn) {
    T previous = null;
    for (T obj : objs) {
      if (previous != null) {
        assertTrue("Results should be sorted, but found " + previous + " before " + obj,
            sortFn.apply(previous).compareTo(sortFn.apply(obj)) <= 0);
      }
      previous = obj;
    }
  }

  /**
   * Tests fetching artifacts by URL prefix across all AUIDs.
   * Uses multiple unique URLs to test paging properly (each URL has distinct sortUri).
   * Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
   */
  @Test
  public void testPaging_ManyUrls_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/prefix/";

    // Create many unique URLs across multiple AUIDs to span multiple pages
    // Each URL is unique, so keyset pagination based on sortUri will work
    int numAuids = 5;
    int urlsPerAuid = 8;  // Total: 40 artifacts spanning multiple pages
    int totalArtifacts = numAuids * urlsPerAuid;

    for (int a = 0; a < numAuids; a++) {
      String auid = String.format("auid%05d", a);
      for (int u = 0; u < urlsPerAuid; u++) {
        // Make each URL unique by including both auid and url index
        String url = String.format("%s%s/file%05d.html", prefix, auid, u);
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Also create non-matching URLs
    for (int i = 0; i < 10; i++) {
      String url = String.format("http://other.com/path/%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, "other-auid", url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query by prefix - all versions
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
        ns, prefix, VersionsEnum.ALL);

    int count = 0;
    for (Artifact artifact : result) {
      assertTrue("URL should match prefix: " + artifact.getUri(),
          artifact.getUri().startsWith(prefix));
      count++;
    }

    assertEquals("Should return all matching artifacts", totalArtifacts, count);
  }

  /**
   * Tests paging when fetching latest versions of URLs matching a prefix.
   * Q6: findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testPaging_ManyUrls_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";

    // Create URLs matching the prefix with multiple versions
    int numUrls = 35;
    int versionsPerUrl = 3;

    Set<String> expectedUrls = new TreeSet<>();
    for (int i = 0; i < numUrls; i++) {
      String url = String.format("%sarticle%05d.html", prefix, i);
      expectedUrls.add(url);

      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Also create non-matching URLs
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/other/%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query for latest versions matching prefix
    Iterable<Artifact> result = idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix);

    Set<String> returnedUrls = new HashSet<>();
    for (Artifact artifact : result) {
      assertTrue("URL should match prefix: " + artifact.getUri(),
          artifact.getUri().startsWith(prefix));
      assertEquals("Should return latest version", Integer.valueOf(versionsPerUrl), artifact.getVersion());
      assertTrue("Duplicate URL found: " + artifact.getUri(), returnedUrls.add(artifact.getUri()));
    }

    assertEquals("Should return all matching URLs", numUrls, returnedUrls.size());
    assertEquals("Should return exactly the expected URLs", expectedUrls, returnedUrls);
  }

  /**
   * Tests paging when fetching all versions of URLs matching a prefix.
   * Q7: findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testPaging_ManyUrlsAndVersions_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";

    int numUrls = 10;
    int versionsPerUrl = 4; // 10 * 4 = 40 artifacts spanning multiple pages
    int totalArtifacts = numUrls * versionsPerUrl;

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("%sdoc%05d.pdf", prefix, i);

      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Also create non-matching URLs
    for (int i = 0; i < 10; i++) {
      String url = String.format("http://example.com/images/%05d.png", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query for all versions matching prefix
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix);

    int count = 0;
    Map<String, Set<Integer>> urlVersions = new HashMap<>();
    for (Artifact artifact : result) {
      assertTrue("URL should match prefix: " + artifact.getUri(),
          artifact.getUri().startsWith(prefix));
      urlVersions.computeIfAbsent(artifact.getUri(), k -> new HashSet<>()).add(artifact.getVersion());
      count++;
    }

    assertEquals("Should return all matching artifacts", totalArtifacts, count);
    assertEquals("Should have all matching URLs", numUrls, urlVersions.size());

    // Verify each URL has all versions
    for (Map.Entry<String, Set<Integer>> entry : urlVersions.entrySet()) {
      assertEquals("URL " + entry.getKey() + " should have all versions",
          versionsPerUrl, entry.getValue().size());
    }
  }

  // ============================================================================
  // Sort Order Tests
  // ============================================================================
  // These tests verify that each paging query returns results in the correct
  // sort order. They use URLs where natural string sort differs from sortUri
  // sort (which replaces "/" with "\t") to ensure the tests would fail if
  // the DB wasn't actually sorting by the correct field.

  /**
   * Creates a set of URIs that sort differently by natural string order vs sortUri order.
   * This is essential for testing that the DB query actually sorts by sortUri.
   *
   * @param prefix URL prefix to use
   * @param count total number of URIs to create
   * @return a map from sortUri key to original URI, in sortUri order
   */
  private SortedMap<String, String> createDivergentSortUris(String prefix, int count) {
    SortedMap<String, String> sortUris = new TreeMap<>();
    List<String> unorderedUris = new ArrayList<>();

    // Create three groups with paths that sort differently:
    // Natural sort: /x-yy/ < /x/ < /xxx-/  (because '-' < '/' in ASCII)
    // SortUri sort: /x/ < /x-yy/ < /xxx-/  (because '\t' < '-' in ASCII after replacement)
    int perGroup = count / 3;
    int remainder = count % 3;

    for (int i = 0; i < perGroup; i++) {
      String uri = String.format("%sx-yy/%05d", prefix, i);
      sortUris.put(uri.replace("/", "\t"), uri);
      unorderedUris.add(uri);
    }

    for (int i = 0; i < perGroup; i++) {
      String uri = String.format("%sx/%05d", prefix, i);
      sortUris.put(uri.replace("/", "\t"), uri);
      unorderedUris.add(uri);
    }

    for (int i = 0; i < perGroup + remainder; i++) {
      String uri = String.format("%sxxx-/%05d", prefix, i);
      sortUris.put(uri.replace("/", "\t"), uri);
      unorderedUris.add(uri);
    }

    // Verify the two orderings are actually different
    List<String> naturallySorted = new ArrayList<>(unorderedUris);
    Collections.sort(naturallySorted);
    assertNotEquals("Test setup error: natural and sortUri order should differ",
        naturallySorted, new ArrayList<>(sortUris.values()));

    return sortUris;
  }

  /**
   * Tests that findLatestArtifactsOfAllUrlsWithNamespaceAndAuid returns
   * results sorted by sortUri (ascending), then version (descending).
   * Since this returns only latest versions, the version component is constant per URL.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testSortOrder_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    SortedMap<String, String> sortUris = createDivergentSortUris("http://example.com/", 21);

    for (String uri : sortUris.values()) {
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, uri, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals(sortUris.size(), artifacts.size());

    // Verify sort by sortUri and that only latest version is returned
    List<String> expectedUris = new ArrayList<>(sortUris.values());
    for (int i = 0; i < expectedUris.size(); i++) {
      assertEquals("Wrong URI at position " + i, expectedUris.get(i), artifacts.get(i).getUri());
      assertEquals("Should return latest version", Integer.valueOf(3), artifacts.get(i).getVersion());
    }

    assertSorted(artifacts, a -> a.getUri().replace("/", "\t"));
  }

  /**
   * Tests that findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid returns
   * results sorted by sortUri (ascending), then version (descending).
   * Since this queries a single URL, sortUri is constant, so we verify version order.
   * Q3: findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid
   */
  @Test
  public void testSortOrder_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/test/resource";
    int numVersions = 15;

    // Insert versions out of order to ensure DB is sorting
    int[] insertOrder = {5, 12, 3, 8, 1, 15, 7, 10, 2, 14, 6, 11, 4, 9, 13};
    for (int v : insertOrder) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url));

    assertEquals(numVersions, artifacts.size());

    // Verify versions are returned in descending order
    for (int i = 0; i < numVersions; i++) {
      int expectedVersion = numVersions - i;
      assertEquals("Wrong version at position " + i,
          Integer.valueOf(expectedVersion), artifacts.get(i).getVersion());
      assertEquals("All artifacts should have same URL", url, artifacts.get(i).getUri());
    }
  }

  /**
   * Tests that findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid returns
   * results sorted by sortUri (ascending) then version (descending).
   * Q2: findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testSortOrder_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int versionsPerUrl = 3;

    SortedMap<String, String> sortUris = createDivergentSortUris("http://example.com/", 15);

    for (String uri : sortUris.values()) {
      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, uri, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals(sortUris.size() * versionsPerUrl, artifacts.size());

    // Verify primary sort by sortUri, secondary sort by version (descending)
    List<String> expectedUris = new ArrayList<>(sortUris.values());
    int idx = 0;
    for (String expectedUri : expectedUris) {
      for (int expectedVersion = versionsPerUrl; expectedVersion >= 1; expectedVersion--) {
        assertEquals("Wrong URI at position " + idx, expectedUri, artifacts.get(idx).getUri());
        assertEquals("Wrong version at position " + idx,
            Integer.valueOf(expectedVersion), artifacts.get(idx).getVersion());
        idx++;
      }
    }
  }

  /**
   * Tests that findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace returns
   * results sorted by sortUri (ascending), auid (ascending), then version (descending).
   * Since this test uses a single URL, sortUri is constant, so we verify auid and version order.
   * Q4: findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace
   */
  @Test
  public void testSortOrder_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";
    int versionsPerAuid = 3;

    // Create AUIDs that will test sorting
    String[] auids = {"auid-02", "auid-01", "auid-03"};  // Inserted out of order
    String[] sortedAuids = {"auid-01", "auid-02", "auid-03"};  // Expected order

    for (String auid : auids) {
      for (int v = 1; v <= versionsPerAuid; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals(auids.length * versionsPerAuid, artifacts.size());

    // Verify primary sort by auid, secondary sort by version (descending)
    int idx = 0;
    for (String expectedAuid : sortedAuids) {
      for (int expectedVersion = versionsPerAuid; expectedVersion >= 1; expectedVersion--) {
        assertEquals("Wrong AUID at position " + idx, expectedAuid, artifacts.get(idx).getAuid());
        assertEquals("Wrong version at position " + idx,
            Integer.valueOf(expectedVersion), artifacts.get(idx).getVersion());
        idx++;
      }
    }
  }

  /**
   * Tests that findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace returns
   * results sorted by sortUri (ascending), auid (ascending), then version (descending).
   * Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
   */
  @Test
  public void testSortOrder_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/";
    int versionsPerArtifact = 2;

    SortedMap<String, String> sortUris = createDivergentSortUris(prefix, 9);
    String[] auids = {"auid-b", "auid-a", "auid-c"};  // Inserted out of order
    String[] sortedAuids = {"auid-a", "auid-b", "auid-c"};

    for (String uri : sortUris.values()) {
      for (String auid : auids) {
        for (int v = 1; v <= versionsPerArtifact; v++) {
          ArtifactSpec spec = makeArtifactSpec(ns, auid, uri, v);
          idxdb.addArtifact(spec.getArtifact());
          idxdb.commitArtifact(spec.getArtifactUuid());
        }
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));

    assertEquals(sortUris.size() * auids.length * versionsPerArtifact, artifacts.size());

    // Verify sort order: sortUri ASC, auid ASC, version DESC
    int idx = 0;
    for (String expectedUri : sortUris.values()) {
      for (String expectedAuid : sortedAuids) {
        for (int expectedVersion = versionsPerArtifact; expectedVersion >= 1; expectedVersion--) {
          assertEquals("Wrong URI at position " + idx, expectedUri, artifacts.get(idx).getUri());
          assertEquals("Wrong AUID at position " + idx, expectedAuid, artifacts.get(idx).getAuid());
          assertEquals("Wrong version at position " + idx,
              Integer.valueOf(expectedVersion), artifacts.get(idx).getVersion());
          idx++;
        }
      }
    }
  }

  /**
   * Tests that findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   * returns results sorted by sortUri (ascending), then version (descending).
   * Since this returns only latest versions, the version component is constant per URL.
   * Q6: findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testSortOrder_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/";

    SortedMap<String, String> sortUris = createDivergentSortUris(prefix, 21);

    for (String uri : sortUris.values()) {
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, uri, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals(sortUris.size(), artifacts.size());

    // Verify sort by sortUri and that only latest version is returned
    List<String> expectedUris = new ArrayList<>(sortUris.values());
    for (int i = 0; i < expectedUris.size(); i++) {
      assertEquals("Wrong URI at position " + i, expectedUris.get(i), artifacts.get(i).getUri());
      assertEquals("Should return latest version", Integer.valueOf(3), artifacts.get(i).getVersion());
    }

    assertSorted(artifacts, a -> a.getUri().replace("/", "\t"));
  }

  /**
   * Tests that findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   * returns results sorted by sortUri (ascending) then version (descending).
   * Q7: findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testSortOrder_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/";
    int versionsPerUrl = 3;

    SortedMap<String, String> sortUris = createDivergentSortUris(prefix, 15);

    for (String uri : sortUris.values()) {
      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, uri, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals(sortUris.size() * versionsPerUrl, artifacts.size());

    // Verify primary sort by sortUri, secondary sort by version (descending)
    List<String> expectedUris = new ArrayList<>(sortUris.values());
    int idx = 0;
    for (String expectedUri : expectedUris) {
      for (int expectedVersion = versionsPerUrl; expectedVersion >= 1; expectedVersion--) {
        assertEquals("Wrong URI at position " + idx, expectedUri, artifacts.get(idx).getUri());
        assertEquals("Wrong version at position " + idx,
            Integer.valueOf(expectedVersion), artifacts.get(idx).getVersion());
        idx++;
      }
    }
  }

  // ============================================================================
  // Concurrent Modification Tests
  // ============================================================================

  /**
   * Tests that artifacts added AFTER the current cursor position during iteration
   * are included in subsequent pages.
   */
  @Test
  public void testConcurrentModification_AddAfterCursor_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create initial artifacts: url00000 through url00014 (15 artifacts, spans 2 pages)
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Start iteration
    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page (10 items: url00000 through url00009)
    List<String> firstPageUrls = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      assertTrue("Should have more items in first page", iter.hasNext());
      firstPageUrls.add(iter.next().getUri());
    }

    // Add a new artifact AFTER the cursor (url00020, which sorts after url00014)
    String newUrl = "http://example.com/url00020";
    ArtifactSpec newSpec = makeArtifactSpec(ns, auid, newUrl, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    // Continue iteration - should see remaining original items plus the new one
    List<String> remainingUrls = new ArrayList<>();
    while (iter.hasNext()) {
      remainingUrls.add(iter.next().getUri());
    }

    // Should have original 5 remaining (url00010-url00014) plus new one (url00020)
    assertEquals("Should see remaining items plus newly added one", 6, remainingUrls.size());
    assertTrue("New artifact should be included", remainingUrls.contains(newUrl));
  }

  /**
   * Tests that artifacts added BEFORE the current cursor position during iteration
   * are NOT included (already passed).
   */
  @Test
  public void testConcurrentModification_AddBeforeCursor_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create initial artifacts: url00010 through url00024 (15 artifacts)
    for (int i = 10; i < 25; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Start iteration
    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page (10 items: url00010 through url00019)
    for (int i = 0; i < 10; i++) {
      assertTrue("Should have more items in first page", iter.hasNext());
      iter.next();
    }

    // Add a new artifact BEFORE the cursor (url00005, which sorts before url00010)
    String newUrl = "http://example.com/url00005";
    ArtifactSpec newSpec = makeArtifactSpec(ns, auid, newUrl, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    // Continue iteration - should NOT see the new artifact (already passed that position)
    List<String> remainingUrls = new ArrayList<>();
    while (iter.hasNext()) {
      remainingUrls.add(iter.next().getUri());
    }

    // Should only have original 5 remaining (url00020-url00024)
    assertEquals("Should only see remaining original items", 5, remainingUrls.size());
    assertFalse("New artifact should NOT be included (before cursor)", remainingUrls.contains(newUrl));
  }

  /**
   * Tests that artifacts deleted during iteration are not returned
   * (if not yet fetched).
   */
  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create initial artifacts and track their UUIDs
    Map<String, String> urlToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      urlToUuid.put(url, spec.getArtifactUuid());
    }

    // Start iteration
    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page (10 items)
    for (int i = 0; i < 10; i++) {
      assertTrue("Should have more items in first page", iter.hasNext());
      iter.next();
    }

    // Delete an artifact that hasn't been fetched yet (url00012)
    String deletedUrl = "http://example.com/url00012";
    idxdb.deleteArtifact(urlToUuid.get(deletedUrl));

    // Continue iteration
    List<String> remainingUrls = new ArrayList<>();
    while (iter.hasNext()) {
      remainingUrls.add(iter.next().getUri());
    }

    // Should have 4 remaining (url00010, url00011, url00013, url00014) - deleted one is missing
    assertEquals("Should see remaining items minus deleted one", 4, remainingUrls.size());
    assertFalse("Deleted artifact should NOT be included", remainingUrls.contains(deletedUrl));
  }

  /**
   * Tests that committing an uncommitted artifact during iteration makes it visible
   * in subsequent pages (when querying committed only).
   */
  @Test
  public void testConcurrentModification_CommitDuringIteration_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create committed artifacts: url00000 through url00009
    for (int i = 0; i < 10; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Create an UNCOMMITTED artifact that sorts after first page: url00015
    String uncommittedUrl = "http://example.com/url00015";
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, auid, uncommittedUrl, 1);
    idxdb.addArtifact(uncommittedSpec.getArtifact());
    // NOT committed yet

    // Create more committed artifacts: url00020 through url00024
    for (int i = 20; i < 25; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Start iteration (committed only)
    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page (10 items: url00000-url00009)
    for (int i = 0; i < 10; i++) {
      assertTrue("Should have more items in first page", iter.hasNext());
      iter.next();
    }

    // Now commit the previously uncommitted artifact
    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    // Continue iteration - should now see the newly committed artifact
    List<String> remainingUrls = new ArrayList<>();
    while (iter.hasNext()) {
      remainingUrls.add(iter.next().getUri());
    }

    // Should have 6 items: url00015 (newly committed) + url00020-url00024 (5 items)
    assertEquals("Should see newly committed artifact plus remaining", 6, remainingUrls.size());
    assertTrue("Newly committed artifact should be included", remainingUrls.contains(uncommittedUrl));
  }

  /**
   * Tests that deleting an artifact BEFORE the current cursor position during iteration
   * does not affect the remaining results (already passed that position).
   */
  @Test
  public void testConcurrentModification_DeleteBeforeCursor_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create initial artifacts and track their UUIDs
    Map<String, String> urlToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      urlToUuid.put(url, spec.getArtifactUuid());
    }

    // Start iteration
    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page (10 items: url00000 through url00009)
    for (int i = 0; i < 10; i++) {
      assertTrue("Should have more items in first page", iter.hasNext());
      iter.next();
    }

    // Delete an artifact that was already fetched (url00005)
    String deletedUrl = "http://example.com/url00005";
    idxdb.deleteArtifact(urlToUuid.get(deletedUrl));

    // Continue iteration - deletion before cursor should not affect remaining results
    List<String> remainingUrls = new ArrayList<>();
    while (iter.hasNext()) {
      remainingUrls.add(iter.next().getUri());
    }

    // Should have all 5 remaining (url00010-url00014)
    assertEquals("Should see all remaining items", 5, remainingUrls.size());
  }

  @Test
  public void testConcurrentModification_AddAfterCursor_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create 12 artifacts (spans 2 pages): 4 URLs with 3 versions each
    for (int i = 0; i < 4; i++) {
      String url = String.format("http://example.com/url%05d", i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page (10 items)
    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Add new artifact after cursor position
    String newUrl = "http://example.com/url00010";
    ArtifactSpec newSpec = makeArtifactSpec(ns, auid, newUrl, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    // Continue - should see remaining + new
    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining items plus new one", 3, remaining.size());
    assertTrue("New artifact should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(newUrl)));
  }

  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    Map<String, String> urlVersionToUuid = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String url = String.format("http://example.com/url%05d", i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
        urlVersionToUuid.put(url + ":" + v, spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page
    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Delete artifact not yet fetched
    String deletedKey = "http://example.com/url00004:1";
    idxdb.deleteArtifact(urlVersionToUuid.get(deletedKey));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    // 15 total - 10 fetched - 1 deleted = 4
    assertEquals("Should see remaining minus deleted", 4, remaining.size());
  }

  @Test
  public void testConcurrentModification_AddAfterCursor_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/file";

    // Create 12 versions (spans 2 pages)
    for (int v = 1; v <= 12; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url).iterator();

    // Consume first page (10 items - versions 12 down to 3 since descending)
    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Add new version (will be at beginning due to descending order, so after cursor in iteration)
    ArtifactSpec newSpec = makeArtifactSpec(ns, auid, url, 13);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    // Should only see versions 2 and 1 (new version 13 is before cursor in sort order)
    assertEquals("Should see remaining versions", 2, remaining.size());
  }

  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/file";

    Map<Integer, String> versionToUuid = new HashMap<>();
    for (int v = 1; v <= 12; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      versionToUuid.put(v, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url).iterator();

    // Consume first page (versions 12 down to 3)
    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Delete version 1 (not yet fetched)
    idxdb.deleteArtifact(versionToUuid.get(1));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    // Should only see version 2 (version 1 deleted)
    assertEquals("Should see remaining minus deleted", 1, remaining.size());
    assertEquals("Should be version 2", Integer.valueOf(2), remaining.get(0).getVersion());
  }

  @Test
  public void testConcurrentModification_AddAfterCursor_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    // Create 12 artifacts across AUIDs (spans 2 pages)
    for (int a = 0; a < 4; a++) {
      String auid = String.format("auid%05d", a);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(
        ns, url, VersionsEnum.ALL).iterator();

    // Consume first page
    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Add new artifact in new AUID (sorts after existing)
    String newAuid = "auid00010";
    ArtifactSpec newSpec = makeArtifactSpec(ns, newAuid, url, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining plus new", 3, remaining.size());
    assertTrue("New artifact should be included",
        remaining.stream().anyMatch(a -> a.getAuid().equals(newAuid)));
  }

  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    Map<String, String> auidVersionToUuid = new HashMap<>();
    for (int a = 0; a < 5; a++) {
      String auid = String.format("auid%05d", a);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
        auidVersionToUuid.put(auid + ":" + v, spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(
        ns, url, VersionsEnum.ALL).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Delete artifact not yet fetched
    idxdb.deleteArtifact(auidVersionToUuid.get("auid00004:1"));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    // 15 total - 10 fetched - 1 deleted = 4
    assertEquals("Should see remaining minus deleted", 4, remaining.size());
  }

  @Test
  public void testConcurrentModification_AddAfterCursor_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/data/";

    // Create 12 artifacts (spans 2 pages)
    for (int i = 0; i < 4; i++) {
      String url = String.format("%sfile%05d", prefix, i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
        ns, prefix, VersionsEnum.ALL).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Add new artifact after cursor
    String newUrl = prefix + "file00010";
    ArtifactSpec newSpec = makeArtifactSpec(ns, "auid1", newUrl, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining plus new", 3, remaining.size());
    assertTrue("New artifact should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(newUrl)));
  }

  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/data/";

    Map<String, String> urlVersionToUuid = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String url = String.format("%sfile%05d", prefix, i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
        urlVersionToUuid.put(url + ":" + v, spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
        ns, prefix, VersionsEnum.ALL).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    String deletedKey = String.format("%sfile%05d:%d", prefix, 4, 1);
    idxdb.deleteArtifact(urlVersionToUuid.get(deletedKey));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining minus deleted", 4, remaining.size());
  }

  @Test
  public void testConcurrentModification_AddAfterCursor_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";

    // Create 15 artifacts (spans 2 pages)
    for (int i = 0; i < 15; i++) {
      String url = String.format("%sarticle%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Add new artifact after cursor
    String newUrl = prefix + "article00020";
    ArtifactSpec newSpec = makeArtifactSpec(ns, auid, newUrl, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining plus new", 6, remaining.size());
    assertTrue("New artifact should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(newUrl)));
  }

  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";

    Map<String, String> urlToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("%sarticle%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      urlToUuid.put(url, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    String deletedUrl = prefix + "article00012";
    idxdb.deleteArtifact(urlToUuid.get(deletedUrl));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining minus deleted", 4, remaining.size());
    assertFalse("Deleted should not be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(deletedUrl)));
  }

  @Test
  public void testConcurrentModification_AddAfterCursor_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";

    // Create 12 artifacts (4 URLs x 3 versions, spans 2 pages)
    for (int i = 0; i < 4; i++) {
      String url = String.format("%sdoc%05d", prefix, i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    // Add new artifact after cursor
    String newUrl = prefix + "doc00010";
    ArtifactSpec newSpec = makeArtifactSpec(ns, auid, newUrl, 1);
    idxdb.addArtifact(newSpec.getArtifact());
    idxdb.commitArtifact(newSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining plus new", 3, remaining.size());
    assertTrue("New artifact should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(newUrl)));
  }

  @Test
  public void testConcurrentModification_DeleteAfterCursor_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";

    Map<String, String> urlVersionToUuid = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String url = String.format("%sdoc%05d", prefix, i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
        urlVersionToUuid.put(url + ":" + v, spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    for (int i = 0; i < 10; i++) {
      assertTrue(iter.hasNext());
      iter.next();
    }

    String deletedKey = String.format("%sdoc%05d:%d", prefix, 4, 1);
    idxdb.deleteArtifact(urlVersionToUuid.get(deletedKey));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining minus deleted", 4, remaining.size());
  }

  // ============================================================================
  // Single Item Result Tests
  // ============================================================================
  // Tests that verify correct behavior when query returns exactly 1 item

  @Test
  public void testSingleItemResult_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/only-one";

    ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
    assertEquals("Should return correct URL", url, artifacts.get(0).getUri());
  }

  @Test
  public void testSingleItemResult_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/only-one";

    ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
    assertEquals("Should return correct URL", url, artifacts.get(0).getUri());
  }

  @Test
  public void testSingleItemResult_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/single-version";

    ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
    assertEquals("Should return version 1", Integer.valueOf(1), artifacts.get(0).getVersion());
  }

  @Test
  public void testSingleItemResult_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
  }

  @Test
  public void testSingleItemResult_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/prefix/";
    String url = prefix + "only-file.html";

    ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
    assertTrue("URL should match prefix", artifacts.get(0).getUri().startsWith(prefix));
  }

  @Test
  public void testSingleItemResult_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";
    String url = prefix + "only-article.html";

    ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
    assertTrue("URL should match prefix", artifacts.get(0).getUri().startsWith(prefix));
  }

  @Test
  public void testSingleItemResult_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";
    String url = prefix + "only-doc.pdf";

    ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
    idxdb.addArtifact(spec.getArtifact());
    idxdb.commitArtifact(spec.getArtifactUuid());

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return exactly 1 artifact", 1, artifacts.size());
    assertTrue("URL should match prefix", artifacts.get(0).getUri().startsWith(prefix));
  }

  // ============================================================================
  // Page Size + 1 Tests
  // ============================================================================
  // Tests that verify correct behavior when result set is exactly page size + 1

  @Test
  public void testPageSizePlusOne_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int count = TEST_PAGE_SIZE + 1; // 11 items

    for (int i = 0; i < count; i++) {
      String url = String.format("http://example.com/item%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());

    // Verify no duplicates
    Set<String> urls = new HashSet<>();
    for (Artifact a : artifacts) {
      assertTrue("No duplicates allowed", urls.add(a.getUri()));
    }
  }

  @Test
  public void testPageSizePlusOne_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int count = TEST_PAGE_SIZE + 1;

    for (int i = 0; i < count; i++) {
      String url = String.format("http://example.com/item%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());
  }

  @Test
  public void testPageSizePlusOne_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/many-versions";
    int count = TEST_PAGE_SIZE + 1;

    for (int v = 1; v <= count; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());
  }

  @Test
  public void testPageSizePlusOne_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";
    int count = TEST_PAGE_SIZE + 1;

    for (int i = 0; i < count; i++) {
      String auid = String.format("auid%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());
  }

  @Test
  public void testPageSizePlusOne_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/prefix/";
    int count = TEST_PAGE_SIZE + 1;

    for (int i = 0; i < count; i++) {
      String url = String.format("%sfile%05d.html", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());
  }

  @Test
  public void testPageSizePlusOne_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";
    int count = TEST_PAGE_SIZE + 1;

    for (int i = 0; i < count; i++) {
      String url = String.format("%sarticle%05d.html", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());
  }

  @Test
  public void testPageSizePlusOne_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";
    int count = TEST_PAGE_SIZE + 1;

    for (int i = 0; i < count; i++) {
      String url = String.format("%sdoc%05d.pdf", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return exactly page size + 1", count, artifacts.size());
  }

  // ============================================================================
  // Invalid/Stale Cursor Tests
  // ============================================================================
  // Tests behavior when referenced artifact is deleted mid-iteration

  @Test
  public void testStaleCursor_DeletedAtCursor_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    Map<String, String> urlToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      urlToUuid.put(url, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Get first 10 items
    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    // Delete the last fetched artifact (cursor position)
    idxdb.deleteArtifact(urlToUuid.get(lastFetched.getUri()));

    // Continue iteration - should still work and return remaining items
    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining items", 5, remaining.size());
  }

  @Test
  public void testStaleCursor_DeletedAtCursor_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    Map<String, String> keyToUuid = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String url = String.format("http://example.com/url%05d", i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
        keyToUuid.put(url + ":" + v, spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    idxdb.deleteArtifact(keyToUuid.get(lastFetched.getUri() + ":" + lastFetched.getVersion()));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining items", 5, remaining.size());
  }

  @Test
  public void testStaleCursor_DeletedAtCursor_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/file";

    Map<Integer, String> versionToUuid = new HashMap<>();
    for (int v = 1; v <= 15; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      versionToUuid.put(v, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url).iterator();

    // Consume first 10 (versions 15 down to 6)
    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    // Delete the cursor position artifact
    idxdb.deleteArtifact(versionToUuid.get(lastFetched.getVersion()));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining versions", 5, remaining.size());
  }

  @Test
  public void testStaleCursor_DeletedAtCursor_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    Map<String, String> auidToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String auid = String.format("auid%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      auidToUuid.put(auid, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(
        ns, url, VersionsEnum.ALL).iterator();

    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    idxdb.deleteArtifact(auidToUuid.get(lastFetched.getAuid()));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining items", 5, remaining.size());
  }

  @Test
  public void testStaleCursor_DeletedAtCursor_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/prefix/";

    Map<String, String> urlToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("%sfile%05d.html", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      urlToUuid.put(url, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
        ns, prefix, VersionsEnum.ALL).iterator();

    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    idxdb.deleteArtifact(urlToUuid.get(lastFetched.getUri()));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining items", 5, remaining.size());
  }

  @Test
  public void testStaleCursor_DeletedAtCursor_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";

    Map<String, String> urlToUuid = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("%sarticle%05d.html", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
      urlToUuid.put(url, spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    idxdb.deleteArtifact(urlToUuid.get(lastFetched.getUri()));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining items", 5, remaining.size());
  }

  @Test
  public void testStaleCursor_DeletedAtCursor_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";

    Map<String, String> keyToUuid = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      String url = String.format("%sdoc%05d.pdf", prefix, i);
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
        keyToUuid.put(url + ":" + v, spec.getArtifactUuid());
      }
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    Artifact lastFetched = null;
    for (int i = 0; i < 10; i++) {
      lastFetched = iter.next();
    }

    idxdb.deleteArtifact(keyToUuid.get(lastFetched.getUri() + ":" + lastFetched.getVersion()));

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should return remaining items", 5, remaining.size());
  }

  // ============================================================================
  // Many Pages (10+) Iteration Tests
  // ============================================================================
  // Tests that verify iteration works correctly across many pages

  @Test
  public void testManyPagesIteration_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int numItems = TEST_PAGE_SIZE * 12 + 5; // 125 items, 13 pages

    Set<String> expectedUrls = new TreeSet<>();
    for (int i = 0; i < numItems; i++) {
      String url = String.format("http://example.com/item%05d", i);
      expectedUrls.add(url);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return all items", numItems, artifacts.size());

    // Verify no duplicates and all expected items present
    Set<String> returnedUrls = new HashSet<>();
    for (Artifact a : artifacts) {
      assertTrue("No duplicates", returnedUrls.add(a.getUri()));
    }
    assertEquals("Should have all URLs", expectedUrls, returnedUrls);

    // Verify sort order maintained across all pages
    assertSorted(artifacts, a -> a.getUri().replace("/", "\t"));
  }

  @Test
  public void testManyPagesIteration_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    int numUrls = 42;
    int versionsPerUrl = 3;
    int totalItems = numUrls * versionsPerUrl; // 126 items

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/item%05d", i);
      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return all items", totalItems, artifacts.size());

    // Verify no duplicates by UUID
    Set<String> uuids = new HashSet<>();
    for (Artifact a : artifacts) {
      assertTrue("No duplicate UUIDs", uuids.add(a.getUuid()));
    }
  }

  @Test
  public void testManyPagesIteration_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/many-versions";
    int numVersions = TEST_PAGE_SIZE * 12 + 5; // 125 versions

    for (int v = 1; v <= numVersions; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url));

    assertEquals("Should return all versions", numVersions, artifacts.size());

    // Verify descending version order
    for (int i = 0; i < numVersions; i++) {
      assertEquals("Wrong version at position " + i,
          Integer.valueOf(numVersions - i), artifacts.get(i).getVersion());
    }
  }

  @Test
  public void testManyPagesIteration_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";
    int numAuids = TEST_PAGE_SIZE * 12 + 5; // 125 AUIDs

    for (int i = 0; i < numAuids; i++) {
      String auid = String.format("auid%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals("Should return all items", numAuids, artifacts.size());

    // Verify AUID sort order
    String prevAuid = null;
    for (Artifact a : artifacts) {
      if (prevAuid != null) {
        assertTrue("AUIDs should be sorted", prevAuid.compareTo(a.getAuid()) <= 0);
      }
      prevAuid = a.getAuid();
    }
  }

  @Test
  public void testManyPagesIteration_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/prefix/";
    int numItems = TEST_PAGE_SIZE * 12 + 5; // 125 items

    for (int i = 0; i < numItems; i++) {
      String url = String.format("%sfile%05d.html", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));

    assertEquals("Should return all items", numItems, artifacts.size());

    // Verify all match prefix
    for (Artifact a : artifacts) {
      assertTrue("Should match prefix", a.getUri().startsWith(prefix));
    }
  }

  @Test
  public void testManyPagesIteration_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";
    int numItems = TEST_PAGE_SIZE * 12 + 5; // 125 items

    for (int i = 0; i < numItems; i++) {
      String url = String.format("%sarticle%05d.html", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return all items", numItems, artifacts.size());

    // Verify sort order
    assertSorted(artifacts, a -> a.getUri().replace("/", "\t"));
  }

  @Test
  public void testManyPagesIteration_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";
    int numUrls = 42;
    int versionsPerUrl = 3;
    int totalItems = numUrls * versionsPerUrl; // 126 items

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("%sdoc%05d.pdf", prefix, i);
      for (int v = 1; v <= versionsPerUrl; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return all items", totalItems, artifacts.size());

    // Verify all match prefix
    for (Artifact a : artifacts) {
      assertTrue("Should match prefix", a.getUri().startsWith(prefix));
    }
  }

  @Test
  public void testConcurrentModification_CommitDuringIteration_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create committed artifacts
    for (int i = 0; i < 10; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Create uncommitted artifact that sorts after first page
    String uncommittedUrl = "http://example.com/url00015";
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, auid, uncommittedUrl, 1);
    idxdb.addArtifact(uncommittedSpec.getArtifact());

    // More committed artifacts
    for (int i = 20; i < 25; i++) {
      String url = String.format("http://example.com/url%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Consume first page
    for (int i = 0; i < 10; i++) {
      iter.next();
    }

    // Commit the uncommitted artifact
    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see newly committed plus remaining", 6, remaining.size());
    assertTrue("Newly committed should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(uncommittedUrl)));
  }

  @Test
  public void testConcurrentModification_CommitDuringIteration_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/file";

    // Create committed versions 10-1 (descending order in results)
    for (int v = 1; v <= 10; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Create uncommitted version 11 (will appear at start due to DESC order)
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, auid, url, 11);
    idxdb.addArtifact(uncommittedSpec.getArtifact());

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url).iterator();

    // Consume all 10 committed versions (10 down to 1)
    for (int i = 0; i < 10; i++) {
      iter.next();
    }

    // Commit version 11
    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    // Should have no more since version 11 is before cursor (higher version = earlier in DESC order)
    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("New version is before cursor in sort order, should not appear", 0, remaining.size());
  }

  @Test
  public void testConcurrentModification_CommitDuringIteration_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    // Create committed artifacts
    for (int i = 0; i < 10; i++) {
      String auid = String.format("auid%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Create uncommitted artifact in AUID that sorts after first page
    String uncommittedAuid = "auid00015";
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, uncommittedAuid, url, 1);
    idxdb.addArtifact(uncommittedSpec.getArtifact());

    // More committed
    for (int i = 20; i < 25; i++) {
      String auid = String.format("auid%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(
        ns, url, VersionsEnum.ALL).iterator();

    for (int i = 0; i < 10; i++) {
      iter.next();
    }

    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see newly committed plus remaining", 6, remaining.size());
    assertTrue("Newly committed should be included",
        remaining.stream().anyMatch(a -> a.getAuid().equals(uncommittedAuid)));
  }

  @Test
  public void testConcurrentModification_CommitDuringIteration_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/data/";

    for (int i = 0; i < 10; i++) {
      String url = String.format("%sfile%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    String uncommittedUrl = prefix + "file00015";
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, "auid1", uncommittedUrl, 1);
    idxdb.addArtifact(uncommittedSpec.getArtifact());

    for (int i = 20; i < 25; i++) {
      String url = String.format("%sfile%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, "auid1", url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
        ns, prefix, VersionsEnum.ALL).iterator();

    for (int i = 0; i < 10; i++) {
      iter.next();
    }

    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see newly committed plus remaining", 6, remaining.size());
    assertTrue("Newly committed should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(uncommittedUrl)));
  }

  @Test
  public void testConcurrentModification_CommitDuringIteration_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";

    for (int i = 0; i < 10; i++) {
      String url = String.format("%sarticle%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    String uncommittedUrl = prefix + "article00015";
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, auid, uncommittedUrl, 1);
    idxdb.addArtifact(uncommittedSpec.getArtifact());

    for (int i = 20; i < 25; i++) {
      String url = String.format("%sarticle%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    for (int i = 0; i < 10; i++) {
      iter.next();
    }

    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see newly committed plus remaining", 6, remaining.size());
    assertTrue("Newly committed should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(uncommittedUrl)));
  }

  @Test
  public void testConcurrentModification_CommitDuringIteration_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";

    for (int i = 0; i < 10; i++) {
      String url = String.format("%sdoc%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    String uncommittedUrl = prefix + "doc00015";
    ArtifactSpec uncommittedSpec = makeArtifactSpec(ns, auid, uncommittedUrl, 1);
    idxdb.addArtifact(uncommittedSpec.getArtifact());

    for (int i = 20; i < 25; i++) {
      String url = String.format("%sdoc%05d", prefix, i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix).iterator();

    for (int i = 0; i < 10; i++) {
      iter.next();
    }

    idxdb.commitArtifact(uncommittedSpec.getArtifactUuid());

    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see newly committed plus remaining", 6, remaining.size());
    assertTrue("Newly committed should be included",
        remaining.stream().anyMatch(a -> a.getUri().equals(uncommittedUrl)));
  }

  // ============================================================================
  // Query-Specific Tests
  // ============================================================================
  // Tests that exercise behavior unique to specific queries.

  // ---------------------------------------------------------------------------
  // Q1/Q6: Latest Version Selection Tests
  // ---------------------------------------------------------------------------
  // Q1 and Q6 return only the latest version of each URL. These tests verify
  // that the correct version is returned when multiple versions exist.

  /**
   * Tests that Q1 returns only the latest version when multiple versions exist.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testLatestVersionSelection_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create URLs with varying numbers of versions
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/doc%05d", i);
      int numVersions = (i % 5) + 1; // 1-5 versions per URL

      for (int v = 1; v <= numVersions; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return exactly one artifact per URL", 15, artifacts.size());

    // Verify each artifact is the latest version
    for (Artifact a : artifacts) {
      // Extract URL index to determine expected version
      String url = a.getUri();
      int urlIndex = Integer.parseInt(url.substring(url.length() - 5));
      int expectedVersion = (urlIndex % 5) + 1;
      assertEquals("Should return latest version for " + url,
          Integer.valueOf(expectedVersion), a.getVersion());
    }
  }

  /**
   * Tests that Q6 returns only the latest version when multiple versions exist.
   * Q6: findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testLatestVersionSelection_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/articles/";

    // Create URLs with varying numbers of versions
    for (int i = 0; i < 15; i++) {
      String url = String.format("%sarticle%05d", prefix, i);
      int numVersions = (i % 5) + 1; // 1-5 versions per URL

      for (int v = 1; v <= numVersions; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Also create some URLs that don't match the prefix
    for (int i = 0; i < 5; i++) {
      String url = String.format("http://example.com/other/doc%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return exactly one artifact per matching URL", 15, artifacts.size());

    // Verify each artifact matches prefix and is the latest version
    for (Artifact a : artifacts) {
      assertTrue("Should match prefix", a.getUri().startsWith(prefix));
      String url = a.getUri();
      int urlIndex = Integer.parseInt(url.substring(url.length() - 5));
      int expectedVersion = (urlIndex % 5) + 1;
      assertEquals("Should return latest version for " + url,
          Integer.valueOf(expectedVersion), a.getVersion());
    }
  }

  /**
   * Tests that a new higher version committed during iteration is seen if after cursor.
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testLatestVersionSelection_NewVersionDuringIteration_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create 15 URLs, each with version 1
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/doc%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    Iterator<Artifact> iter = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false).iterator();

    // Fetch first page (10 items)
    for (int i = 0; i < 10; i++) {
      Artifact a = iter.next();
      assertEquals("First page should have version 1", Integer.valueOf(1), a.getVersion());
    }

    // Add version 2 to a URL that's AFTER the cursor (doc00012)
    String urlAfterCursor = "http://example.com/doc00012";
    ArtifactSpec newVersionSpec = makeArtifactSpec(ns, auid, urlAfterCursor, 2);
    idxdb.addArtifact(newVersionSpec.getArtifact());
    idxdb.commitArtifact(newVersionSpec.getArtifactUuid());

    // Continue iteration - should see version 2 for doc00012
    List<Artifact> remaining = new ArrayList<>();
    while (iter.hasNext()) {
      remaining.add(iter.next());
    }

    assertEquals("Should see remaining 5 URLs", 5, remaining.size());

    // Find doc00012 in results - should be version 2
    Artifact doc12 = remaining.stream()
        .filter(a -> a.getUri().equals(urlAfterCursor))
        .findFirst()
        .orElse(null);
    assertNotNull("Should find doc00012", doc12);
    assertEquals("Should be version 2 (latest)", Integer.valueOf(2), doc12.getVersion());
  }

  // ---------------------------------------------------------------------------
  // Q2: Uncommitted Artifacts Tests
  // ---------------------------------------------------------------------------
  // Q2 is unique in that it can include uncommitted artifacts.

  /**
   * Tests that Q2 includes uncommitted artifacts when includeUncommitted=true.
   * Q2: findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testUncommittedArtifacts_Included_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create mix of committed and uncommitted artifacts
    List<String> uncommittedUuids = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/doc%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());

      if (i % 2 == 0) {
        // Commit even-indexed artifacts
        idxdb.commitArtifact(spec.getArtifactUuid());
      } else {
        // Keep odd-indexed uncommitted
        uncommittedUuids.add(spec.getArtifactUuid());
      }
    }

    // Query with includeUncommitted=true (second parameter is includeUncommitted)
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, true));

    assertEquals("Should return all 15 artifacts (committed + uncommitted)", 15, artifacts.size());

    // Verify uncommitted artifacts are present
    long uncommittedCount = artifacts.stream()
        .filter(a -> !a.getCommitted())
        .count();
    assertEquals("Should have 7 uncommitted artifacts", 7, uncommittedCount);
  }

  /**
   * Tests that Q2 excludes uncommitted artifacts when includeUncommitted=false.
   * Q2: findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testUncommittedArtifacts_Excluded_Q2() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create mix of committed and uncommitted artifacts
    for (int i = 0; i < 15; i++) {
      String url = String.format("http://example.com/doc%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());

      if (i % 2 == 0) {
        // Commit even-indexed artifacts (8 total: 0,2,4,6,8,10,12,14)
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query with includeUncommitted=false
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false));

    assertEquals("Should return only 8 committed artifacts", 8, artifacts.size());

    // Verify all returned artifacts are committed
    for (Artifact a : artifacts) {
      assertTrue("All artifacts should be committed", a.getCommitted());
    }
  }

  // ---------------------------------------------------------------------------
  // Q3: Single URL Focus Tests
  // ---------------------------------------------------------------------------
  // Q3 queries versions of a single URL, so it should return nothing for
  // non-existent URLs and exclude other URLs.

  /**
   * Tests that Q3 returns empty result for non-existent URL.
   * Q3: findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid
   */
  @Test
  public void testSingleUrlFocus_NonExistentUrl_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create some artifacts with different URLs
    for (int i = 0; i < 10; i++) {
      String url = String.format("http://example.com/exists%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query for a URL that doesn't exist
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(
            ns, auid, "http://example.com/does-not-exist"));

    assertEquals("Should return empty result for non-existent URL", 0, artifacts.size());
  }

  /**
   * Tests that Q3 only returns versions of the specified URL.
   * Q3: findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid
   */
  @Test
  public void testSingleUrlFocus_ExcludesOtherUrls_Q3() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String targetUrl = "http://example.com/target";

    // Create versions of the target URL
    for (int v = 1; v <= 5; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, targetUrl, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Create artifacts with similar URLs that should NOT be returned
    String[] similarUrls = {
        "http://example.com/target2",
        "http://example.com/target/subpath",
        "http://example.com/targe",
        "http://example.com/targetx"
    };
    for (String url : similarUrls) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, targetUrl));

    assertEquals("Should return exactly 5 versions of target URL", 5, artifacts.size());
    for (Artifact a : artifacts) {
      assertEquals("All artifacts should have target URL", targetUrl, a.getUri());
    }
  }

  // ---------------------------------------------------------------------------
  // Q4/Q5: Cross-AUID Tests
  // ---------------------------------------------------------------------------
  // Q4 and Q5 query across all AUIDs in a namespace.

  /**
   * Tests that Q4 returns artifacts from multiple AUIDs correctly interleaved.
   * Q4: findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace
   */
  @Test
  public void testCrossAuid_InterleavedResults_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    // Create the same URL in multiple AUIDs with multiple versions
    String[] auids = {"auid-aaa", "auid-bbb", "auid-ccc"};
    for (String auid : auids) {
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals("Should return 9 artifacts (3 AUIDs x 3 versions)", 9, artifacts.size());

    // Verify all AUIDs are represented
    Set<String> foundAuids = new HashSet<>();
    for (Artifact a : artifacts) {
      foundAuids.add(a.getAuid());
      assertEquals("All should have same URL", url, a.getUri());
    }
    assertEquals("Should have all 3 AUIDs", 3, foundAuids.size());

    // Verify sort order: auid ascending, then version descending
    String prevAuid = "";
    int prevVersion = Integer.MAX_VALUE;
    for (Artifact a : artifacts) {
      if (!a.getAuid().equals(prevAuid)) {
        assertTrue("AUIDs should be ascending", a.getAuid().compareTo(prevAuid) > 0);
        prevAuid = a.getAuid();
        prevVersion = Integer.MAX_VALUE;
      }
      assertTrue("Versions should be descending within AUID", a.getVersion() < prevVersion);
      prevVersion = a.getVersion();
    }
  }

  /**
   * Tests that Q5 returns artifacts from multiple AUIDs for prefix-matching URLs.
   * Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
   */
  @Test
  public void testCrossAuid_InterleavedResults_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/shared/";

    // Create URLs matching prefix in multiple AUIDs
    String[] auids = {"auid-aaa", "auid-bbb", "auid-ccc"};
    for (String auid : auids) {
      for (int i = 0; i < 5; i++) {
        String url = String.format("%sdoc%05d", prefix, i);
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Also create URLs NOT matching prefix
    for (String auid : auids) {
      String url = "http://example.com/other/doc";
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));

    assertEquals("Should return 15 artifacts (3 AUIDs x 5 URLs)", 15, artifacts.size());

    // Verify all match prefix and all AUIDs are represented
    Set<String> foundAuids = new HashSet<>();
    for (Artifact a : artifacts) {
      assertTrue("All should match prefix", a.getUri().startsWith(prefix));
      foundAuids.add(a.getAuid());
    }
    assertEquals("Should have all 3 AUIDs", 3, foundAuids.size());
  }

  /**
   * Tests that Q1 does NOT return artifacts from other AUIDs (AUID isolation).
   * Q1: findLatestArtifactsOfAllUrlsWithNamespaceAndAuid
   */
  @Test
  public void testAuidIsolation_Q1() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";

    // Create same URLs in multiple AUIDs
    String[] auids = {"auid-aaa", "auid-bbb", "auid-ccc"};
    for (String auid : auids) {
      for (int i = 0; i < 5; i++) {
        String url = String.format("http://example.com/doc%05d", i);
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query for only auid-bbb
    List<Artifact> artifacts = toList(
        idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, "auid-bbb", false));

    assertEquals("Should return only 5 artifacts from auid-bbb", 5, artifacts.size());
    for (Artifact a : artifacts) {
      assertEquals("All should be from auid-bbb", "auid-bbb", a.getAuid());
    }
  }

  // ---------------------------------------------------------------------------
  // Q5/Q6/Q7: Prefix Matching Tests
  // ---------------------------------------------------------------------------
  // These queries filter by URL prefix.

  /**
   * Tests that prefix matching returns empty for non-matching prefix.
   * Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
   */
  @Test
  public void testPrefixMatching_NoMatch_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create URLs with specific prefix
    for (int i = 0; i < 10; i++) {
      String url = String.format("http://example.com/existing/%05d", i);
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query for non-existent prefix
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
            ns, "http://example.com/nonexistent/", VersionsEnum.ALL));

    assertEquals("Should return empty for non-matching prefix", 0, artifacts.size());
  }

  /**
   * Tests prefix boundary - URLs that almost match but don't.
   * Q6: findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testPrefixMatching_Boundary_Q6() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/docs/";

    // Create URLs that match the prefix
    for (int i = 0; i < 5; i++) {
      String url = prefix + "file" + i;
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Create URLs that almost match but shouldn't
    String[] nonMatchingUrls = {
        "http://example.com/doc/file",      // 'doc' not 'docs'
        "http://example.com/docs",          // missing trailing slash
        "http://example.com/documents/file", // 'documents' not 'docs'
        "http://example.com/DOCS/file",     // case-sensitive
        "http://example.org/docs/file"      // different domain
    };
    for (String url : nonMatchingUrls) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    List<Artifact> artifacts = toList(
        idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, prefix));

    assertEquals("Should return only 5 matching URLs", 5, artifacts.size());
    for (Artifact a : artifacts) {
      assertTrue("All should match prefix exactly", a.getUri().startsWith(prefix));
    }
  }

  /**
   * Tests prefix matching with empty prefix (should match all).
   * Q7: findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
   */
  @Test
  public void testPrefixMatching_EmptyPrefix_Q7() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create URLs with various prefixes
    String[] urls = {
        "http://example.com/a",
        "http://example.org/b",
        "https://secure.com/c",
        "ftp://files.com/d"
    };
    for (String url : urls) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    // Query with empty prefix
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(ns, auid, ""));

    assertEquals("Empty prefix should match all URLs", 4, artifacts.size());
  }

  // ---------------------------------------------------------------------------
  // Q4/Q5: VersionsEnum Parameter Tests
  // ---------------------------------------------------------------------------
  // Q4 and Q5 accept a VersionsEnum parameter (ALL or LATEST).

  /**
   * Tests Q4 with VersionsEnum.LATEST returns only latest version per URL/AUID.
   * Q4: findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace
   */
  @Test
  public void testVersionsEnum_Latest_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    // Create same URL in multiple AUIDs with multiple versions
    String[] auids = {"auid-aaa", "auid-bbb", "auid-ccc"};
    for (String auid : auids) {
      for (int v = 1; v <= 5; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query with LATEST
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.LATEST));

    assertEquals("Should return 3 artifacts (1 per AUID)", 3, artifacts.size());

    // Verify each is version 5 (latest)
    for (Artifact a : artifacts) {
      assertEquals("Should be latest version", Integer.valueOf(5), a.getVersion());
    }

    // Verify all AUIDs represented
    Set<String> foundAuids = new HashSet<>();
    for (Artifact a : artifacts) {
      foundAuids.add(a.getAuid());
    }
    assertEquals("Should have all 3 AUIDs", 3, foundAuids.size());
  }

  /**
   * Tests Q4 with VersionsEnum.ALL returns all versions.
   * Q4: findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace
   */
  @Test
  public void testVersionsEnum_All_Q4() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared";

    // Create same URL in multiple AUIDs with multiple versions
    String[] auids = {"auid-aaa", "auid-bbb"};
    for (String auid : auids) {
      for (int v = 1; v <= 3; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query with ALL
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, VersionsEnum.ALL));

    assertEquals("Should return 6 artifacts (2 AUIDs x 3 versions)", 6, artifacts.size());
  }

  /**
   * Tests Q5 with VersionsEnum.LATEST returns only latest version per URL/AUID.
   * Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
   */
  @Test
  public void testVersionsEnum_Latest_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/docs/";

    // Create URLs in multiple AUIDs with multiple versions
    String[] auids = {"auid-aaa", "auid-bbb"};
    for (String auid : auids) {
      for (int i = 0; i < 3; i++) {
        String url = String.format("%sdoc%d", prefix, i);
        for (int v = 1; v <= 4; v++) {
          ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
          idxdb.addArtifact(spec.getArtifact());
          idxdb.commitArtifact(spec.getArtifactUuid());
        }
      }
    }

    // Query with LATEST
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.LATEST));

    assertEquals("Should return 6 artifacts (2 AUIDs x 3 URLs)", 6, artifacts.size());

    // Verify each is version 4 (latest)
    for (Artifact a : artifacts) {
      assertEquals("Should be latest version", Integer.valueOf(4), a.getVersion());
    }
  }

  /**
   * Tests Q5 with VersionsEnum.ALL returns all versions.
   * Q5: findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
   */
  @Test
  public void testVersionsEnum_All_Q5() throws Exception {
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/docs/";

    // Create URLs in multiple AUIDs with multiple versions
    String[] auids = {"auid-aaa", "auid-bbb"};
    for (String auid : auids) {
      for (int i = 0; i < 2; i++) {
        String url = String.format("%sdoc%d", prefix, i);
        for (int v = 1; v <= 3; v++) {
          ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
          idxdb.addArtifact(spec.getArtifact());
          idxdb.commitArtifact(spec.getArtifactUuid());
        }
      }
    }

    // Query with ALL
    List<Artifact> artifacts = toList(
        idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(ns, prefix, VersionsEnum.ALL));

    assertEquals("Should return 12 artifacts (2 AUIDs x 2 URLs x 3 versions)", 12, artifacts.size());
  }
}
