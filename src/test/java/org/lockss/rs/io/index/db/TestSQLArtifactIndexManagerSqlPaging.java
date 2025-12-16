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

import org.junit.Test;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.Logger;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactVersions;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.postgresql.ds.PGSimpleDataSource;

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
 * <p>Methods tested:
 * <ul>
 *   <li>{@link SQLArtifactIndexManagerSql#findLatestArtifactsOfAllUrlsWithNamespaceAndAuid}</li>
 *   <li>{@link SQLArtifactIndexManagerSql#findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid}</li>
 *   <li>{@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid}</li>
 *   <li>{@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace}</li>
 *   <li>{@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace}</li>
 *   <li>{@link SQLArtifactIndexManagerSql#findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid}</li>
 *   <li>{@link SQLArtifactIndexManagerSql#findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid}</li>
 * </ul>
 *
 * <p>Tests use a small page size (10) and smaller data sets to speed up execution
 * while still exercising multi-page scenarios.</p>
 */
public class TestSQLArtifactIndexManagerSqlPaging extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  /** Small page size for testing - allows testing paging with fewer artifacts */
  private static final int TEST_PAGE_SIZE = 10;

  private MockLockssDaemon theDaemon;
  private String tempDirPath;
  private SQLArtifactIndexDbManager idxDbManager;
  private String dbPort;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();
    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);
    dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
    ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER, dbPort);
  }

  @Override
  public void tearDown() throws Exception {
    if (idxDbManager != null)
      idxDbManager.stopService();
    theDaemon.stopDaemon();
    super.tearDown();
  }

  protected void initializePostgreSQL() throws Exception {
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgresx");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.enabled", "true",
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.initialSize", "2");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, PGSimpleDataSource.class.getCanonicalName(),
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    idxDbManager = new SQLArtifactIndexDbManager();
    startEmbeddedPgDbManager(idxDbManager);
    idxDbManager.initService(getMockLockssDaemon());

    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private void initializeDatabase() throws Exception {
    initializePostgreSQL();
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
    idxdb.setPagingPageSize(TEST_PAGE_SIZE);
    return idxdb;
  }

  // ============================================================================
  // Tests for findLatestArtifactsOfAllUrlsWithNamespaceAndAuid with paging
  // ============================================================================

  /**
   * Tests that paging works correctly with a large number of URLs,
   * ensuring all artifacts are returned without duplicates or gaps.
   */
  @Test
  public void testFindLatestArtifacts_PagingWithManyUrls() throws Exception {
    initializeDatabase();
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
   */
  @Test
  public void testFindLatestArtifacts_ExactPageSizeMultiple() throws Exception {
    initializeDatabase();
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
   * Tests paging with URLs that have special characters, ensuring cursor tracking
   * works correctly when URLs contain characters that might affect sorting.
   */
  @Test
  public void testFindLatestArtifacts_PagingWithSpecialCharacterUrls() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Create URLs with various special characters
    String[] urlPatterns = {
        "http://example.com/path/with spaces/file%d.html",
        "http://example.com/path/with?query=param&num=%d",
        "http://example.com/path/with#fragment%d",
        "http://example.com/unicode/资源/%d",
        "http://example.com/path/with//double//slashes/%d",
        "http://example.com/path/normal/%d"
    };

    int numPerPattern = 6; // 6 patterns * 6 = 36 total, spanning multiple pages
    int totalUrls = urlPatterns.length * numPerPattern;
    Set<String> expectedUrls = new HashSet<>();

    for (String pattern : urlPatterns) {
      for (int i = 0; i < numPerPattern; i++) {
        String url = String.format(pattern, i);
        expectedUrls.add(url);

        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, 1);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query for latest artifacts
    Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);

    // Collect results
    Set<String> returnedUrls = new HashSet<>();
    int count = 0;
    for (Artifact a : result) {
      assertTrue("Duplicate URL found: " + a.getUri(), returnedUrls.add(a.getUri()));
      count++;
    }

    assertEquals("Should return all artifacts", totalUrls, count);

    // Verify all expected URLs are present
    for (String expectedUrl : expectedUrls) {
      assertTrue("Missing URL: " + expectedUrl, returnedUrls.contains(expectedUrl));
    }
  }

  /**
   * Tests that uncommitted artifacts are correctly included/excluded
   * when paging through large result sets.
   */
  @Test
  public void testFindLatestArtifacts_PagingIncludeUncommitted() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    int numUrls = 25;

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/path/%05d", i);

      // Create v1 (committed) and v2 (uncommitted) for each URL
      ArtifactSpec specV1 = makeArtifactSpec(ns, auid, url, 1);
      ArtifactSpec specV2 = makeArtifactSpec(ns, auid, url, 2);

      idxdb.addArtifact(specV1.getArtifact());
      idxdb.addArtifact(specV2.getArtifact());

      // Only commit v1
      idxdb.commitArtifact(specV1.getArtifactUuid());
    }

    // Query excluding uncommitted - should get v1 for all
    {
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("Should return committed version", Integer.valueOf(1), a.getVersion());
        count++;
      }
      assertEquals("Should return all URLs", numUrls, count);
    }

    // Query including uncommitted - should get v2 for all
    {
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("Should return latest version (uncommitted)", Integer.valueOf(2), a.getVersion());
        count++;
      }
      assertEquals("Should return all URLs", numUrls, count);
    }
  }

  /**
   * Tests paging behavior with an empty result set.
   */
  @Test
  public void testFindLatestArtifacts_EmptyResult() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    // Query with no artifacts in database
    Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);

    assertFalse(result.iterator().hasNext());
  }

  /**
   * Tests that artifacts from different namespaces are correctly filtered
   * when paging.
   */
  @Test
  public void testFindLatestArtifacts_PagingNamespaceIsolation() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns1 = "namespace1";
    String ns2 = "namespace2";
    String auid = "auid1";

    int numUrlsPerNamespace = 25;

    // Create artifacts in both namespaces
    for (int i = 0; i < numUrlsPerNamespace; i++) {
      String url = String.format("http://example.com/path/%05d", i);

      ArtifactSpec spec1 = makeArtifactSpec(ns1, auid, url, 1);
      ArtifactSpec spec2 = makeArtifactSpec(ns2, auid, url, 1);

      idxdb.addArtifact(spec1.getArtifact());
      idxdb.addArtifact(spec2.getArtifact());

      idxdb.commitArtifact(spec1.getArtifactUuid());
      idxdb.commitArtifact(spec2.getArtifactUuid());
    }

    // Query ns1 only
    {
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns1, auid, false);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("All artifacts should be from ns1", ns1, a.getNamespace());
        count++;
      }
      assertEquals("Should return all ns1 artifacts", numUrlsPerNamespace, count);
    }

    // Query ns2 only
    {
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns2, auid, false);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("All artifacts should be from ns2", ns2, a.getNamespace());
        count++;
      }
      assertEquals("Should return all ns2 artifacts", numUrlsPerNamespace, count);
    }
  }

  /**
   * Tests that artifacts from different AUIDs are correctly filtered when paging.
   */
  @Test
  public void testFindLatestArtifacts_PagingAuidIsolation() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";

    int numUrlsPerAuid = 25;

    // Create artifacts in both AUIDs
    for (int i = 0; i < numUrlsPerAuid; i++) {
      String url = String.format("http://example.com/path/%05d", i);

      ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, url, 1);
      ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, url, 1);

      idxdb.addArtifact(spec1.getArtifact());
      idxdb.addArtifact(spec2.getArtifact());

      idxdb.commitArtifact(spec1.getArtifactUuid());
      idxdb.commitArtifact(spec2.getArtifactUuid());
    }

    // Query auid1 only
    {
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid1, false);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("All artifacts should be from auid1", auid1, a.getAuid());
        count++;
      }
      assertEquals("Should return all auid1 artifacts", numUrlsPerAuid, count);
    }

    // Query auid2 only
    {
      Iterable<Artifact> result = idxdb.findLatestArtifactsOfAllUrlsWithNamespaceAndAuid(ns, auid2, false);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("All artifacts should be from auid2", auid2, a.getAuid());
        count++;
      }
      assertEquals("Should return all auid2 artifacts", numUrlsPerAuid, count);
    }
  }

  /**
   * Tests paging with URLs that differ only at page boundaries,
   * ensuring the keyset cursor correctly handles the transition.
   */
  @Test
  public void testFindLatestArtifacts_PageBoundaryUrlSimilarity() throws Exception {
    initializeDatabase();
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
   */
  @Test
  public void testFindLatestArtifacts_MultipleIterations() throws Exception {
    initializeDatabase();
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
   */
  @Test
  public void testFindLatestArtifacts_PartialIteration() throws Exception {
    initializeDatabase();
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

  // ============================================================================
  // Tests for findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid
  // ============================================================================

  /**
   * Tests that paging returns all versions of all URLs correctly.
   */
  @Test
  public void testFindAllVersions_PagingWithManyUrlsAndVersions() throws Exception {
    initializeDatabase();
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
   * Tests that uncommitted artifacts are correctly included/excluded when getting all versions.
   */
  @Test
  public void testFindAllVersions_IncludeUncommitted() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";

    int numUrls = 25;

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("http://example.com/path/%05d", i);

      // v1 committed, v2 uncommitted
      ArtifactSpec specV1 = makeArtifactSpec(ns, auid, url, 1);
      ArtifactSpec specV2 = makeArtifactSpec(ns, auid, url, 2);

      idxdb.addArtifact(specV1.getArtifact());
      idxdb.addArtifact(specV2.getArtifact());
      idxdb.commitArtifact(specV1.getArtifactUuid());
    }

    // Query excluding uncommitted - should get only v1 for all URLs
    {
      Iterable<Artifact> result = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, false);

      int count = 0;
      for (Artifact a : result) {
        assertEquals("Should only return committed version", Integer.valueOf(1), a.getVersion());
        count++;
      }
      assertEquals("Should return only committed artifacts", numUrls, count);
    }

    // Query including uncommitted - should get both v1 and v2 for all URLs
    {
      Iterable<Artifact> result = idxdb.findArtifactsAllVersionsOfAllUrlsWithNamespaceAndAuid(ns, auid, true);

      int count = 0;
      for (Artifact a : result) {
        count++;
      }
      assertEquals("Should return all artifacts including uncommitted", numUrls * 2, count);
    }
  }

  // ============================================================================
  // Tests for findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid
  // ============================================================================

  /**
   * Tests paging when fetching all versions of a single URL.
   */
  @Test
  public void testFindVersionsOfUrl_PagingWithManyVersions() throws Exception {
    initializeDatabase();
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
   * Tests that only committed versions are returned.
   */
  @Test
  public void testFindVersionsOfUrl_OnlyCommitted() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String url = "http://example.com/test-url";

    // Create 25 committed versions and 10 uncommitted versions
    int committedVersions = 25;
    int uncommittedVersions = 10;

    for (int v = 1; v <= committedVersions; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      idxdb.commitArtifact(spec.getArtifactUuid());
    }

    for (int v = committedVersions + 1; v <= committedVersions + uncommittedVersions; v++) {
      ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
      idxdb.addArtifact(spec.getArtifact());
      // Don't commit
    }

    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfUrlWithNamespaceAndAuid(ns, auid, url);

    int count = 0;
    for (Artifact a : result) {
      assertTrue("Should only return committed versions", a.getVersion() <= committedVersions);
      count++;
    }

    assertEquals("Should return only committed versions", committedVersions, count);
  }

  // ============================================================================
  // Tests for findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace
  // ============================================================================

  /**
   * Tests fetching artifacts for a URL across multiple AUIDs with multi-page results.
   * This verifies that the extended keyset pagination (sortUri, auid, version) correctly
   * handles cases where the same URL exists across multiple AUIDs with the same versions.
   */
  @Test
  public void testFindUrlAllAuids_MultipleAuids() throws Exception {
    initializeDatabase();
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
        toList(idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(ns, url, ArtifactVersions.ALL));

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
   * Tests fetching latest version only across multiple AUIDs with multi-page results.
   */
  @Test
  public void testFindUrlAllAuids_LatestVersionOnly() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String url = "http://example.com/shared-url";

    int numAuids = 12;  // More than page size to span multiple pages
    int versionsPerAuid = 3;

    for (int a = 0; a < numAuids; a++) {
      String auid = String.format("auid%05d", a);
      for (int v = 1; v <= versionsPerAuid; v++) {
        ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
        idxdb.addArtifact(spec.getArtifact());
        idxdb.commitArtifact(spec.getArtifactUuid());
      }
    }

    // Query for latest versions only
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfUrlAllAuidsInNamespace(
        ns, url, ArtifactVersions.LATEST);

    int count = 0;
    Set<String> returnedAuids = new HashSet<>();
    for (Artifact artifact : result) {
      assertEquals("Should return latest version", Integer.valueOf(versionsPerAuid), artifact.getVersion());
      assertTrue("Duplicate AUID found: " + artifact.getAuid(), returnedAuids.add(artifact.getAuid()));
      count++;
    }

    assertEquals("Should return one artifact per AUID", numAuids, count);
  }

  // ============================================================================
  // Tests for findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace
  // ============================================================================

  /**
   * Tests fetching artifacts by URL prefix across all AUIDs.
   * Uses multiple unique URLs to test paging properly (each URL has distinct sortUri).
   */
  @Test
  public void testFindByPrefixAllAuids_PagingWithManyUrls() throws Exception {
    initializeDatabase();
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
        ns, prefix, ArtifactVersions.ALL);

    int count = 0;
    for (Artifact artifact : result) {
      assertTrue("URL should match prefix: " + artifact.getUri(),
          artifact.getUri().startsWith(prefix));
      count++;
    }

    assertEquals("Should return all matching artifacts", totalArtifacts, count);
  }

  /**
   * Tests fetching latest version only by prefix across all AUIDs with unique URLs.
   */
  @Test
  public void testFindByPrefixAllAuids_LatestVersionOnly() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String prefix = "http://example.com/data/";

    // Create unique URLs to avoid keyset pagination limitation
    int numAuids = 5;
    int urlsPerAuid = 8;  // Total: 40 URLs
    int versionsPerUrl = 3;
    int expectedCount = numAuids * urlsPerAuid; // One per URL

    for (int a = 0; a < numAuids; a++) {
      String auid = String.format("auid%05d", a);
      for (int u = 0; u < urlsPerAuid; u++) {
        // Make each URL unique by including both auid and url index
        String url = String.format("%s%s/item%05d", prefix, auid, u);
        for (int v = 1; v <= versionsPerUrl; v++) {
          ArtifactSpec spec = makeArtifactSpec(ns, auid, url, v);
          idxdb.addArtifact(spec.getArtifact());
          idxdb.commitArtifact(spec.getArtifactUuid());
        }
      }
    }

    // Query by prefix - latest versions only
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfUrlByPrefixAllAuidsInNamespace(
        ns, prefix, ArtifactVersions.LATEST);

    int count = 0;
    for (Artifact artifact : result) {
      assertEquals("Should return latest version", Integer.valueOf(versionsPerUrl), artifact.getVersion());
      assertTrue("URL should match prefix", artifact.getUri().startsWith(prefix));
      count++;
    }

    assertEquals("Should return one artifact per URL", expectedCount, count);
  }

  // ============================================================================
  // Tests for findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
  // ============================================================================

  /**
   * Tests paging when fetching latest versions of URLs matching a prefix.
   */
  @Test
  public void testFindLatestByPrefix_PagingWithManyUrls() throws Exception {
    initializeDatabase();
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
   * Tests that only the specified AUID is returned.
   */
  @Test
  public void testFindLatestByPrefix_AuidIsolation() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid1 = "auid1";
    String auid2 = "auid2";
    String prefix = "http://example.com/shared/";

    int numUrls = 25;

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("%sfile%05d", prefix, i);

      ArtifactSpec spec1 = makeArtifactSpec(ns, auid1, url, 1);
      ArtifactSpec spec2 = makeArtifactSpec(ns, auid2, url, 1);

      idxdb.addArtifact(spec1.getArtifact());
      idxdb.addArtifact(spec2.getArtifact());
      idxdb.commitArtifact(spec1.getArtifactUuid());
      idxdb.commitArtifact(spec2.getArtifactUuid());
    }

    // Query auid1 only
    Iterable<Artifact> result = idxdb.findArtifactsLatestCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid1, prefix);

    int count = 0;
    for (Artifact artifact : result) {
      assertEquals("All artifacts should be from auid1", auid1, artifact.getAuid());
      count++;
    }

    assertEquals("Should return all auid1 artifacts", numUrls, count);
  }

  // ============================================================================
  // Tests for findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid
  // ============================================================================

  /**
   * Tests paging when fetching all versions of URLs matching a prefix.
   */
  @Test
  public void testFindAllVersionsByPrefix_PagingWithManyUrlsAndVersions() throws Exception {
    initializeDatabase();
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

  /**
   * Tests that only committed artifacts are returned when fetching all versions by prefix.
   */
  @Test
  public void testFindAllVersionsByPrefix_OnlyCommitted() throws Exception {
    initializeDatabase();
    SQLArtifactIndexManagerSql idxdb = createIndexManagerSql();

    String ns = "ns1";
    String auid = "auid1";
    String prefix = "http://example.com/test/";

    int numUrls = 25;

    for (int i = 0; i < numUrls; i++) {
      String url = String.format("%sfile%05d", prefix, i);

      // v1 committed, v2 uncommitted
      ArtifactSpec specV1 = makeArtifactSpec(ns, auid, url, 1);
      ArtifactSpec specV2 = makeArtifactSpec(ns, auid, url, 2);

      idxdb.addArtifact(specV1.getArtifact());
      idxdb.addArtifact(specV2.getArtifact());
      idxdb.commitArtifact(specV1.getArtifactUuid());
    }

    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, prefix);

    int count = 0;
    for (Artifact artifact : result) {
      assertEquals("Should only return committed version", Integer.valueOf(1), artifact.getVersion());
      count++;
    }

    assertEquals("Should return only committed artifacts", numUrls, count);
  }

  /**
   * Tests paging with an empty prefix (should match all URLs).
   */
  @Test
  public void testFindAllVersionsByPrefix_EmptyPrefix() throws Exception {
    initializeDatabase();
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

    // Query with empty prefix - should match all
    Iterable<Artifact> result = idxdb.findArtifactsAllCommittedVersionsOfAllUrlsMatchingPrefixWithNamespaceAndAuid(
        ns, auid, "");

    int count = 0;
    for (Artifact artifact : result) {
      count++;
    }

    assertEquals("Empty prefix should match all URLs", numUrls, count);
  }

  // ============================================================================
  // Concurrent Modification Tests
  // ============================================================================

  /**
   * Tests that artifacts added AFTER the current cursor position during iteration
   * are included in subsequent pages.
   */
  @Test
  public void testConcurrentModification_AddArtifactAfterCursor() throws Exception {
    initializeDatabase();
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
  public void testConcurrentModification_AddArtifactBeforeCursor() throws Exception {
    initializeDatabase();
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
  public void testConcurrentModification_DeleteArtifactAfterCursor() throws Exception {
    initializeDatabase();
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
  public void testConcurrentModification_CommitArtifactDuringIteration() throws Exception {
    initializeDatabase();
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
}
