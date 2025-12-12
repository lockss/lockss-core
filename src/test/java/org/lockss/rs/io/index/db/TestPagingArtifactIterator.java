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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lockss.db.DbException;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PagingArtifactIterator}.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Constructor validation (null fetcher, invalid page sizes)</li>
 *   <li>Empty result sets</li>
 *   <li>Single page results (including boundary conditions)</li>
 *   <li>Multi-page iteration</li>
 *   <li>Cursor state management (sortUri/version tracking)</li>
 *   <li>Error handling (DbException wrapping)</li>
 *   <li>Iterator contract compliance (hasNext idempotency, NoSuchElementException)</li>
 * </ul>
 */
public class TestPagingArtifactIterator {
  private static final L4JLogger log = L4JLogger.getLogger();

  // ============================================================================
  // Test Helpers
  // ============================================================================

  /**
   * Creates a list of mock Artifact objects for testing.
   *
   * @param count Number of artifacts to create
   * @param urlPrefix URL prefix for artifacts
   * @param startVersion Starting version (versions decrease for subsequent artifacts)
   * @return List of mock artifacts
   */
  private List<Artifact> createMockArtifacts(int count, String urlPrefix, int startVersion) {
    return IntStream.range(0, count)
        .mapToObj(i -> {
          Artifact a = new Artifact();
          a.setUri(urlPrefix + "/item" + i);
          // Versions decrease to match SQL ORDER BY: sortUri ASC, version DESC
          a.setVersion(startVersion - i);
          a.setUuid("artifact-" + i);
          return a;
        })
        .collect(Collectors.toList());
  }

  /**
   * Test PageFetcher that returns artifacts from a pre-defined list.
   * Simulates keyset pagination and tracks cursor values passed to each call.
   */
  private static class TrackingPageFetcher implements PagingArtifactIterator.PageFetcher {
    private final List<Artifact> allArtifacts;
    private final List<PagingCursor> recordedCursors = new ArrayList<>();
    private final List<Integer> recordedLimits = new ArrayList<>();

    TrackingPageFetcher(List<Artifact> allArtifacts) {
      this.allArtifacts = allArtifacts;
    }

    @Override
    public List<Artifact> fetchPage(PagingCursor cursor, int limit)
        throws DbException {
      recordedCursors.add(cursor);
      recordedLimits.add(limit);

      if (allArtifacts.isEmpty()) {
        return Collections.emptyList();
      }

      int startIndex = 0;
      if (!cursor.isInitial()) {
        // Find the index after the last returned artifact
        for (int i = 0; i < allArtifacts.size(); i++) {
          Artifact a = allArtifacts.get(i);
          String sortUri = a.getUri().replace("/", "\t");
          if (sortUri.equals(cursor.getSortUri()) && a.getVersion().equals(cursor.getVersion())) {
            startIndex = i + 1;
            break;
          }
        }
      }

      int endIndex = Math.min(startIndex + limit, allArtifacts.size());
      if (startIndex >= allArtifacts.size()) {
        return Collections.emptyList();
      }

      return new ArrayList<>(allArtifacts.subList(startIndex, endIndex));
    }

    public int getFetchCount() {
      return recordedCursors.size();
    }

    public List<PagingCursor> getRecordedCursors() {
      return recordedCursors;
    }

    public List<Integer> getRecordedLimits() {
      return recordedLimits;
    }
  }

  // ============================================================================
  // Constructor/Initialization Tests
  // ============================================================================

  @Test
  public void testConstructor_NullPageFetcher_ThrowsIllegalArgumentException() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new PagingArtifactIterator(null)
    );
    assertEquals("PageFetcher cannot be null", ex.getMessage());
  }

  @Test
  public void testConstructor_NullPageFetcherWithPageSize_ThrowsIllegalArgumentException() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new PagingArtifactIterator(null, 100)
    );
    assertEquals("PageFetcher cannot be null", ex.getMessage());
  }

  @Test
  public void testConstructor_InvalidPageSize_Zero_ThrowsIllegalArgumentException() {
    PagingArtifactIterator.PageFetcher dummyFetcher = (cursor, limit) -> Collections.emptyList();

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new PagingArtifactIterator(dummyFetcher, 0)
    );
    assertEquals("Page size must be at least 1", ex.getMessage());
  }

  @Test
  public void testConstructor_InvalidPageSize_Negative_ThrowsIllegalArgumentException() {
    PagingArtifactIterator.PageFetcher dummyFetcher = (cursor, limit) -> Collections.emptyList();

    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> new PagingArtifactIterator(dummyFetcher, -1)
    );
    assertEquals("Page size must be at least 1", ex.getMessage());
  }

  @Test
  public void testConstructor_MinimalPageSize_Accepted() {
    PagingArtifactIterator.PageFetcher dummyFetcher = (cursor, limit) -> Collections.emptyList();

    PagingArtifactIterator iterator = new PagingArtifactIterator(dummyFetcher, 1);
    assertEquals(1, iterator.getPageSize());
  }

  @Test
  public void testConstructor_DefaultPageSize() {
    PagingArtifactIterator.PageFetcher dummyFetcher = (cursor, limit) -> Collections.emptyList();

    PagingArtifactIterator iterator = new PagingArtifactIterator(dummyFetcher);
    assertEquals(PagingArtifactIterator.DEFAULT_PAGE_SIZE, iterator.getPageSize());
    assertEquals(1000, iterator.getPageSize());
  }

  @Test
  public void testConstructor_CustomPageSize() {
    PagingArtifactIterator.PageFetcher dummyFetcher = (cursor, limit) -> Collections.emptyList();

    PagingArtifactIterator iterator = new PagingArtifactIterator(dummyFetcher, 500);
    assertEquals(500, iterator.getPageSize());
  }

  // ============================================================================
  // Empty Result Set Tests
  // ============================================================================

  @Test
  public void testHasNext_EmptyResult_ReturnsFalse() {
    TrackingPageFetcher fetcher = new TrackingPageFetcher(Collections.emptyList());

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    assertFalse(iterator.hasNext());
    assertEquals(1, fetcher.getFetchCount(), "Should fetch exactly once to discover empty result");
  }

  @Test
  public void testNext_EmptyResult_ThrowsNoSuchElementException() {
    PagingArtifactIterator.PageFetcher fetcher = (cursor, limit) -> Collections.emptyList();

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testIteration_EmptyResult_NoFetchAfterFirst() {
    TrackingPageFetcher fetcher = new TrackingPageFetcher(Collections.emptyList());

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    // Call hasNext multiple times
    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNext());

    assertEquals(1, fetcher.getFetchCount(), "Should only fetch once for empty result");
  }

  // ============================================================================
  // Single Page Tests
  // ============================================================================

  @Test
  public void testIteration_SingleItemResult() {
    List<Artifact> artifacts = createMockArtifacts(1, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    assertTrue(iterator.hasNext());
    Artifact result = iterator.next();
    assertEquals("http://example.com/item0", result.getUri());
    assertEquals(100, result.getVersion());

    assertFalse(iterator.hasNext());
    assertEquals(1, fetcher.getFetchCount());
  }

  @Test
  public void testIteration_LessThanPageSizeResults() {
    List<Artifact> artifacts = createMockArtifacts(5, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(5, results.size());
    assertEquals(1, fetcher.getFetchCount(), "Should only need one fetch for results < pageSize");
  }

  @Test
  public void testIteration_ExactlyPageSizeResults() {
    int pageSize = 10;
    List<Artifact> artifacts = createMockArtifacts(pageSize, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(pageSize, results.size());
    // Fetches pageSize+1, gets exactly pageSize, so isLastPage=true after first fetch
    assertEquals(1, fetcher.getFetchCount());
  }

  @Test
  public void testHasNext_Idempotent_SinglePage() {
    List<Artifact> artifacts = createMockArtifacts(3, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    // Call hasNext multiple times without calling next
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());

    assertEquals(1, fetcher.getFetchCount(), "hasNext should be idempotent - only one fetch");

    // Now consume
    Artifact first = iterator.next();
    assertEquals("http://example.com/item0", first.getUri());

    // hasNext should still be idempotent
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(1, fetcher.getFetchCount(), "Still only one fetch after partial consumption");
  }

  // ============================================================================
  // Multi-Page Tests
  // ============================================================================

  @Test
  public void testIteration_TwoPages() {
    int pageSize = 10;
    // 15 artifacts = 10 + 5 = 2 pages
    List<Artifact> artifacts = createMockArtifacts(15, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(15, results.size());
    assertEquals(2, fetcher.getFetchCount(), "Should need exactly 2 fetches for 15 items with pageSize=10");

    // Verify order
    for (int i = 0; i < results.size(); i++) {
      assertEquals("http://example.com/item" + i, results.get(i).getUri());
    }
  }

  @Test
  public void testIteration_ExactlyTwoPages() {
    int pageSize = 10;
    // Exactly 20 artifacts = 2 full pages
    List<Artifact> artifacts = createMockArtifacts(20, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(20, results.size());
    // First fetch: asks for 11, gets 11, returns 10, marks not last page
    // Second fetch: asks for 11, gets 10, marks last page
    assertEquals(2, fetcher.getFetchCount());
  }

  @Test
  public void testIteration_PageSizePlusOne() {
    int pageSize = 10;
    // 11 artifacts = triggers second page
    List<Artifact> artifacts = createMockArtifacts(11, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(11, results.size());
    assertEquals(2, fetcher.getFetchCount(), "11 items with pageSize=10 needs 2 fetches");
  }

  @Test
  public void testIteration_ManyPages() {
    int pageSize = 10;
    int totalArtifacts = 95; // 9 full pages + 5 extra = 10 fetches
    List<Artifact> artifacts = createMockArtifacts(totalArtifacts, "http://example.com", 1000);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(totalArtifacts, results.size());
    assertEquals(10, fetcher.getFetchCount(), "95 items with pageSize=10 needs 10 fetches");

    // Verify all artifacts retrieved in order
    for (int i = 0; i < results.size(); i++) {
      assertEquals("http://example.com/item" + i, results.get(i).getUri());
    }
  }

  @Test
  public void testPageFetcher_CalledWithCorrectLimit() {
    int pageSize = 10;
    List<Artifact> artifacts = createMockArtifacts(5, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    while (iterator.hasNext()) {
      iterator.next();
    }

    // Verify limit is pageSize + 1 (to detect if there are more pages)
    assertEquals(1, fetcher.getFetchCount());
    assertEquals(pageSize + 1, fetcher.getRecordedLimits().get(0));
  }

  // ============================================================================
  // Cursor State Tests
  // ============================================================================

  @Test
  public void testCursor_FirstPagePassesNull() {
    List<Artifact> artifacts = createMockArtifacts(5, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    iterator.hasNext(); // Trigger first fetch

    assertEquals(1, fetcher.getFetchCount());
    PagingCursor firstCursor = fetcher.getRecordedCursors().get(0);
    assertTrue(firstCursor.isInitial(), "First page should pass INITIAL cursor");
  }

  @Test
  public void testCursor_SubsequentPagePassesLastValues() {
    int pageSize = 5;
    List<Artifact> artifacts = createMockArtifacts(8, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    // Consume all items to trigger both fetches
    while (iterator.hasNext()) {
      iterator.next();
    }

    assertEquals(2, fetcher.getFetchCount());

    // First fetch: INITIAL cursor
    assertTrue(fetcher.getRecordedCursors().get(0).isInitial());

    // Second fetch: should have cursor from last item of first page (item4)
    // URL: http://example.com/item4 -> sortUri: http:\t\texample.com\titem4
    PagingCursor secondCursor = fetcher.getRecordedCursors().get(1);
    assertNotNull(secondCursor);
    String expectedSortUri = "http://example.com/item4".replace("/", "\t");
    int expectedVersion = 96; // 100 - 4

    assertEquals(expectedSortUri, secondCursor.getSortUri());
    assertEquals(expectedVersion, secondCursor.getVersion());
  }

  @Test
  public void testCursor_SortUriTransformation() {
    // Test that URL slashes are correctly transformed to tabs
    // Use pageSize=1 and 2 items to ensure a second fetch occurs
    Artifact artifact1 = new Artifact();
    artifact1.setUri("http://example.com/path/to/resource");
    artifact1.setVersion(2);

    Artifact artifact2 = new Artifact();
    artifact2.setUri("http://example.com/path/to/resource2");
    artifact2.setVersion(1);

    List<Artifact> artifacts = new ArrayList<>();
    artifacts.add(artifact1);
    artifacts.add(artifact2);

    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 1);

    // Consume all items - this will trigger second fetch
    while (iterator.hasNext()) {
      iterator.next();
    }

    // Verify the sortUri transformation in second fetch
    assertEquals(2, fetcher.getFetchCount());

    PagingCursor secondCursor = fetcher.getRecordedCursors().get(1);
    String expectedSortUri = "http:\t\texample.com\tpath\tto\tresource";
    assertEquals(expectedSortUri, secondCursor.getSortUri());
  }

  @Test
  public void testCursor_UrlWithManySlashes() {
    // Use 2 items to ensure a second fetch occurs
    Artifact artifact1 = new Artifact();
    artifact1.setUri("http://example.com/a/b/c/d/e/f/g.html");
    artifact1.setVersion(42);

    Artifact artifact2 = new Artifact();
    artifact2.setUri("http://example.com/z.html");
    artifact2.setVersion(1);

    List<Artifact> artifacts = new ArrayList<>();
    artifacts.add(artifact1);
    artifacts.add(artifact2);

    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 1);

    while (iterator.hasNext()) {
      iterator.next();
    }

    assertEquals(2, fetcher.getFetchCount());
    PagingCursor secondCursor = fetcher.getRecordedCursors().get(1);
    String expectedSortUri = "http:\t\texample.com\ta\tb\tc\td\te\tf\tg.html";
    assertEquals(expectedSortUri, secondCursor.getSortUri());
    assertEquals(42, secondCursor.getVersion());
  }

  // ============================================================================
  // Error Handling Tests
  // ============================================================================

  @Test
  public void testHasNext_DbException_WrappedInRuntimeException() {
    PagingArtifactIterator.PageFetcher failingFetcher = (cursor, limit) -> {
      throw new DbException("Database connection failed");
    };

    PagingArtifactIterator iterator = new PagingArtifactIterator(failingFetcher, 10);

    RuntimeException ex = assertThrows(RuntimeException.class, iterator::hasNext);
    assertEquals("Database error fetching artifact page", ex.getMessage());
    assertTrue(ex.getCause() instanceof DbException);
  }

  @Test
  public void testNext_DbException_WrappedInRuntimeException() {
    PagingArtifactIterator.PageFetcher failingFetcher = (cursor, limit) -> {
      throw new DbException("Database connection failed");
    };

    PagingArtifactIterator iterator = new PagingArtifactIterator(failingFetcher, 10);

    RuntimeException ex = assertThrows(RuntimeException.class, iterator::next);
    assertEquals("Database error fetching artifact page", ex.getMessage());
  }

  @Test
  public void testFetcher_ExceptionOnSecondPage() {
    int pageSize = 5;
    AtomicInteger fetchCount = new AtomicInteger(0);

    PagingArtifactIterator.PageFetcher partialFailFetcher = (cursor, limit) -> {
      int count = fetchCount.incrementAndGet();
      if (count == 1) {
        // First page succeeds
        return createMockArtifacts(pageSize + 1, "http://example.com", 100);
      } else {
        // Second page fails
        throw new DbException("Connection lost");
      }
    };

    PagingArtifactIterator iterator = new PagingArtifactIterator(partialFailFetcher, pageSize);

    // First page should work
    for (int i = 0; i < pageSize; i++) {
      assertTrue(iterator.hasNext());
      iterator.next();
    }

    // Second page fetch should fail
    RuntimeException ex = assertThrows(RuntimeException.class, iterator::hasNext);
    assertTrue(ex.getCause() instanceof DbException);
    assertEquals(2, fetchCount.get());
  }

  // ============================================================================
  // State Consistency Tests
  // ============================================================================

  @Test
  public void testHasNext_CalledMultipleTimes_NoDuplicateFetches() {
    List<Artifact> artifacts = createMockArtifacts(15, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    // Multiple hasNext calls at start
    for (int i = 0; i < 10; i++) {
      assertTrue(iterator.hasNext());
    }
    assertEquals(1, fetcher.getFetchCount());

    // Consume first page
    for (int i = 0; i < 10; i++) {
      iterator.next();
    }
    assertEquals(1, fetcher.getFetchCount());

    // Multiple hasNext calls should trigger exactly one more fetch
    for (int i = 0; i < 10; i++) {
      assertTrue(iterator.hasNext());
    }
    assertEquals(2, fetcher.getFetchCount());
  }

  @Test
  public void testNext_WithoutHasNext_WorksCorrectly() {
    List<Artifact> artifacts = createMockArtifacts(3, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    // Call next() directly without hasNext()
    Artifact a1 = iterator.next();
    assertEquals("http://example.com/item0", a1.getUri());

    Artifact a2 = iterator.next();
    assertEquals("http://example.com/item1", a2.getUri());

    Artifact a3 = iterator.next();
    assertEquals("http://example.com/item2", a3.getUri());

    // Now should throw
    assertThrows(NoSuchElementException.class, iterator::next);
    assertFalse(iterator::hasNext);
  }

  @Test
  public void testIteration_StandardPattern() {
    List<Artifact> artifacts = createMockArtifacts(25, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    int count = 0;
    while (iterator.hasNext()) {
      Artifact a = iterator.next();
      assertEquals("http://example.com/item" + count, a.getUri());
      count++;
    }

    assertEquals(25, count);
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void testPartialIteration_EarlyBreak() {
    List<Artifact> artifacts = createMockArtifacts(15, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    // Only consume 5 items then abandon
    for (int i = 0; i < 5; i++) {
      assertTrue(iterator.hasNext());
      iterator.next();
    }

    // Should have only fetched first page
    assertEquals(1, fetcher.getFetchCount());

    // Iterator can still be used if needed
    assertTrue(iterator.hasNext());
    assertEquals("http://example.com/item5", iterator.next().getUri());
  }

  // ============================================================================
  // Page Size Configuration Tests
  // ============================================================================

  @Test
  public void testGetPageSize_ReturnsConfiguredValue() {
    PagingArtifactIterator.PageFetcher dummyFetcher = (cursor, limit) -> Collections.emptyList();

    assertEquals(1, new PagingArtifactIterator(dummyFetcher, 1).getPageSize());
    assertEquals(100, new PagingArtifactIterator(dummyFetcher, 100).getPageSize());
    assertEquals(5000, new PagingArtifactIterator(dummyFetcher, 5000).getPageSize());
  }

  @Test
  public void testSmallPageSize_ManyFetches() {
    int pageSize = 3;
    int totalArtifacts = 10;
    List<Artifact> artifacts = createMockArtifacts(totalArtifacts, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(totalArtifacts, results.size());
    // 10 items with pageSize=3: 3+3+3+1 = 4 fetches
    assertEquals(4, fetcher.getFetchCount());
  }

  @Test
  public void testLargePageSize_SingleFetch() {
    int pageSize = 100;
    int totalArtifacts = 5;
    List<Artifact> artifacts = createMockArtifacts(totalArtifacts, "http://example.com", 100);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, pageSize);

    List<Artifact> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(iterator.next());
    }

    assertEquals(totalArtifacts, results.size());
    assertEquals(1, fetcher.getFetchCount(), "Should only need one fetch when pageSize > totalItems");
  }

  // ============================================================================
  // Boundary and Edge Case Tests
  // ============================================================================

  @Test
  public void testIteration_VersionZero() {
    // Test artifact with version 0 (edge case - lowest valid version)
    Artifact artifact = new Artifact();
    artifact.setUri("http://example.com/test");
    artifact.setVersion(0);

    List<Artifact> artifacts = Collections.singletonList(artifact);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    assertTrue(iterator.hasNext());
    Artifact result = iterator.next();
    assertEquals(0, result.getVersion());
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteration_MinimalUrl() {
    // Test with minimal valid URL
    Artifact artifact = new Artifact();
    artifact.setUri("x");
    artifact.setVersion(1);

    List<Artifact> artifacts = Collections.singletonList(artifact);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    assertTrue(iterator.hasNext());
    Artifact result = iterator.next();
    assertEquals("x", result.getUri());

    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteration_SpecialCharactersInUrl() {
    // Use 2 items to trigger second fetch and verify cursor
    Artifact artifact1 = new Artifact();
    artifact1.setUri("http://example.com/path?query=value&other=123#fragment");
    artifact1.setVersion(2);

    Artifact artifact2 = new Artifact();
    artifact2.setUri("http://example.com/z");
    artifact2.setVersion(1);

    List<Artifact> artifacts = new ArrayList<>();
    artifacts.add(artifact1);
    artifacts.add(artifact2);

    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 1);

    // Consume first item
    assertTrue(iterator.hasNext());
    Artifact result = iterator.next();
    assertEquals("http://example.com/path?query=value&other=123#fragment", result.getUri());

    // Consume remaining to trigger second fetch
    while (iterator.hasNext()) {
      iterator.next();
    }

    // Verify only slashes are transformed, not other special chars
    assertEquals(2, fetcher.getFetchCount());
    PagingCursor secondCursor = fetcher.getRecordedCursors().get(1);
    String expectedSortUri = "http:\t\texample.com\tpath?query=value&other=123#fragment";
    assertEquals(expectedSortUri, secondCursor.getSortUri());
  }

  @Test
  public void testIteration_UnicodeInUrl() {
    // Use 2 items to trigger second fetch and verify cursor
    Artifact artifact1 = new Artifact();
    artifact1.setUri("http://example.com/资源/文件.html");
    artifact1.setVersion(2);

    Artifact artifact2 = new Artifact();
    artifact2.setUri("http://example.com/z");
    artifact2.setVersion(1);

    List<Artifact> artifacts = new ArrayList<>();
    artifacts.add(artifact1);
    artifacts.add(artifact2);

    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 1);

    assertTrue(iterator.hasNext());
    Artifact result = iterator.next();
    assertEquals("http://example.com/资源/文件.html", result.getUri());

    // Consume remaining to trigger second fetch
    while (iterator.hasNext()) {
      iterator.next();
    }

    assertEquals(2, fetcher.getFetchCount());
    PagingCursor secondCursor = fetcher.getRecordedCursors().get(1);
    String expectedSortUri = "http:\t\texample.com\t资源\t文件.html";
    assertEquals(expectedSortUri, secondCursor.getSortUri());
  }

  @Test
  public void testIteration_ConsecutiveSlashesInUrl() {
    // Use 2 items to trigger second fetch and verify cursor
    Artifact artifact1 = new Artifact();
    artifact1.setUri("http://example.com//double//slashes///triple");
    artifact1.setVersion(2);

    Artifact artifact2 = new Artifact();
    artifact2.setUri("http://example.com/z");
    artifact2.setVersion(1);

    List<Artifact> artifacts = new ArrayList<>();
    artifacts.add(artifact1);
    artifacts.add(artifact2);

    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 1);

    assertTrue(iterator.hasNext());
    iterator.next();

    // Consume remaining to trigger second fetch
    while (iterator.hasNext()) {
      iterator.next();
    }

    assertEquals(2, fetcher.getFetchCount());
    PagingCursor secondCursor = fetcher.getRecordedCursors().get(1);
    String expectedSortUri = "http:\t\texample.com\t\tdouble\t\tslashes\t\t\ttriple";
    assertEquals(expectedSortUri, secondCursor.getSortUri());
  }

  // ============================================================================
  // Integration-Style Tests
  // ============================================================================

  @Test
  public void testFullIteration_CountMatchesExpected() {
    int[] testSizes = {0, 1, 5, 10, 11, 99, 100, 101, 500};

    for (int size : testSizes) {
      List<Artifact> artifacts = createMockArtifacts(size, "http://test.com", 1000);
      TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

      PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

      int count = 0;
      while (iterator.hasNext()) {
        iterator.next();
        count++;
      }

      assertEquals(size, count, "Size mismatch for test size: " + size);
    }
  }

  @Test
  public void testDataIntegrity_NoMissingArtifacts() {
    int totalArtifacts = 47;
    List<Artifact> artifacts = createMockArtifacts(totalArtifacts, "http://example.com", 1000);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    List<String> retrievedUris = new ArrayList<>();
    while (iterator.hasNext()) {
      retrievedUris.add(iterator.next().getUri());
    }

    // Verify all URIs present and in correct order
    assertEquals(totalArtifacts, retrievedUris.size());
    for (int i = 0; i < totalArtifacts; i++) {
      assertEquals("http://example.com/item" + i, retrievedUris.get(i),
          "Missing or out-of-order artifact at index " + i);
    }
  }

  @Test
  public void testDataIntegrity_NoDuplicateArtifacts() {
    int totalArtifacts = 35;
    List<Artifact> artifacts = createMockArtifacts(totalArtifacts, "http://example.com", 1000);
    TrackingPageFetcher fetcher = new TrackingPageFetcher(artifacts);

    PagingArtifactIterator iterator = new PagingArtifactIterator(fetcher, 10);

    List<String> retrievedIds = new ArrayList<>();
    while (iterator.hasNext()) {
      retrievedIds.add(iterator.next().getUuid());
    }

    // Check for duplicates
    long uniqueCount = retrievedIds.stream().distinct().count();
    assertEquals(totalArtifacts, uniqueCount, "Duplicate artifacts detected");
  }
}
