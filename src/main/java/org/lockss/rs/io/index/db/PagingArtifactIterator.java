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

import org.lockss.db.DbException;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.model.Artifact;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An artifact iterator that fetches pages on demand using keyset pagination.
 * Unlike {@link SQLArtifactIndexManagerSql.ArtifactResultSetIterator}, this does NOT hold
 * a database connection between page fetches - connections are opened and closed for each page.
 *
 * <p>This follows the same pattern as {@code SolrQueryArtifactIterator} but for SQL databases,
 * using keyset pagination instead of cursor marks.</p>
 *
 * <p>Keyset pagination uses the last artifact's sort key to efficiently
 * fetch the next page without using OFFSET, which degrades as O(n) for large result sets.</p>
 *
 * <p>The cursor contents depend on the query type:
 * <ul>
 *   <li>Single-AUID queries: cursor contains (sortUri, version)</li>
 *   <li>All-AUIDs queries: cursor contains (sortUri, version, auid)</li>
 * </ul>
 *
 * @see <a href="https://use-the-index-luke.com/no-offset">Why keyset pagination?</a>
 * @see PagingCursor
 */
public class PagingArtifactIterator implements Iterator<Artifact> {
  private static final L4JLogger log = L4JLogger.getLogger();

  /** Default page size - number of artifacts to fetch per page */
  public static final int DEFAULT_PAGE_SIZE = 1000;

  /** Default cursor extractor for single-AUID queries (sortUri, version) */
  public static final CursorExtractor DEFAULT_CURSOR_EXTRACTOR = artifact ->
      PagingCursor.of(
          artifact.getUri().replace("/", "\t"),
          artifact.getVersion()
      );

  /** Cursor extractor for all-AUIDs queries (sortUri, version, auid) */
  public static final CursorExtractor ALL_AUIDS_CURSOR_EXTRACTOR = artifact ->
      PagingCursor.of(
          artifact.getUri().replace("/", "\t"),
          artifact.getVersion(),
          artifact.getAuid()
      );

  // Page fetcher function - supplied by SQLArtifactIndexManagerSql
  private final PageFetcher pageFetcher;

  // Cursor extractor - extracts cursor from last artifact
  private final CursorExtractor cursorExtractor;

  // Current page buffer
  private List<Artifact> pageBuffer;
  private Iterator<Artifact> pageBufferIterator;

  // Cursor state for keyset pagination
  private PagingCursor lastCursor;

  // Pagination state
  private final int pageSize;
  private boolean isLastPage = false;
  private boolean isFirstFetch = true;

  /**
   * Functional interface for fetching a page of artifacts.
   * The implementation handles connection management internally,
   * opening a connection, executing the query, and closing the connection.
   */
  @FunctionalInterface
  public interface PageFetcher {
    /**
     * Fetches a page of artifacts starting after the given cursor position.
     *
     * @param cursor The cursor from the last artifact of the previous page,
     *               or null for the first page
     * @param limit Maximum number of artifacts to fetch
     * @return List of artifacts (empty list if no more results)
     * @throws DbException if a database error occurs
     */
    List<Artifact> fetchPage(PagingCursor cursor, int limit) throws DbException;
  }

  /**
   * Functional interface for extracting a cursor from an artifact.
   * Different query types may extract different cursor fields.
   */
  @FunctionalInterface
  public interface CursorExtractor {
    /**
     * Extracts a cursor from the given artifact.
     *
     * @param artifact the artifact to extract cursor from
     * @return the cursor representing this artifact's position
     */
    PagingCursor extractCursor(Artifact artifact);
  }

  /**
   * Creates a new paging iterator with default page size and cursor extractor.
   *
   * @param pageFetcher Function to fetch pages from the database
   */
  public PagingArtifactIterator(PageFetcher pageFetcher) {
    this(pageFetcher, DEFAULT_PAGE_SIZE, DEFAULT_CURSOR_EXTRACTOR);
  }

  /**
   * Creates a new paging iterator with specified page size and default cursor extractor.
   *
   * @param pageFetcher Function to fetch pages from the database
   * @param pageSize Number of artifacts to fetch per page (must be at least 1)
   * @throws IllegalArgumentException if pageFetcher is null or pageSize < 1
   */
  public PagingArtifactIterator(PageFetcher pageFetcher, int pageSize) {
    this(pageFetcher, pageSize, DEFAULT_CURSOR_EXTRACTOR);
  }

  /**
   * Creates a new paging iterator with specified page size and cursor extractor.
   *
   * @param pageFetcher Function to fetch pages from the database
   * @param pageSize Number of artifacts to fetch per page (must be at least 1)
   * @param cursorExtractor Function to extract cursor from artifacts
   * @throws IllegalArgumentException if pageFetcher is null, cursorExtractor is null, or pageSize < 1
   */
  public PagingArtifactIterator(PageFetcher pageFetcher, int pageSize, CursorExtractor cursorExtractor) {
    if (pageFetcher == null) {
      throw new IllegalArgumentException("PageFetcher cannot be null");
    }
    if (cursorExtractor == null) {
      throw new IllegalArgumentException("CursorExtractor cannot be null");
    }
    if (pageSize < 1) {
      throw new IllegalArgumentException("Page size must be at least 1");
    }

    this.pageFetcher = pageFetcher;
    this.pageSize = pageSize;
    this.cursorExtractor = cursorExtractor;
    this.pageBuffer = new ArrayList<>(0);
    this.pageBufferIterator = pageBuffer.iterator();
  }

  /**
   * Returns {@code true} if there are more artifacts to iterate over.
   * May trigger a page fetch if the current page buffer is exhausted.
   *
   * @return true if there are more artifacts, false otherwise
   * @throws RuntimeException if a database error occurs during page fetch
   */
  @Override
  public boolean hasNext() {
    log.debug2("Invoked");

    // Check if current page buffer has more items
    if (pageBufferIterator.hasNext()) {
      log.debug2("Buffer has next, returning true");
      return true;
    }

    // If this was the last page, we're done
    if (isLastPage) {
      log.debug2("Last page exhausted, returning false");
      return false;
    }

    // Fetch next page
    try {
      fetchNextPage();
    } catch (DbException e) {
      log.error("Database error fetching artifact page", e);
      throw new RuntimeException("Database error fetching artifact page", e);
    }

    boolean hasNext = pageBufferIterator.hasNext();
    log.debug2("After fetch, hasNext = {}", hasNext);
    return hasNext;
  }

  /**
   * Returns the next artifact in the iteration.
   *
   * @return the next Artifact
   * @throws NoSuchElementException if no more artifacts are available
   */
  @Override
  public Artifact next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return pageBufferIterator.next();
  }

  /**
   * Fetches the next page of artifacts from the database.
   * Connection is opened and closed within the pageFetcher call.
   */
  private void fetchNextPage() throws DbException {
    log.debug2("Fetching next page, cursor={}", lastCursor);

    // Fetch one extra to detect if there are more pages
    PagingCursor cursorToUse = isFirstFetch ? PagingCursor.INITIAL : lastCursor;
    List<Artifact> results = pageFetcher.fetchPage(cursorToUse, pageSize + 1);

    isFirstFetch = false;

    // Check if this is the last page
    if (results.size() <= pageSize) {
      isLastPage = true;
      pageBuffer = results;
    } else {
      // More results exist - remove the extra one
      isLastPage = false;
      pageBuffer = new ArrayList<>(results.subList(0, pageSize));
    }

    // Update cursor for next page
    if (!pageBuffer.isEmpty()) {
      Artifact lastArtifact = pageBuffer.get(pageBuffer.size() - 1);
      lastCursor = cursorExtractor.extractCursor(lastArtifact);
    }

    pageBufferIterator = pageBuffer.iterator();

    log.debug2("Fetched {} artifacts, isLastPage={}", pageBuffer.size(), isLastPage);
  }

  /**
   * Returns the page size being used by this iterator.
   *
   * @return the page size
   */
  public int getPageSize() {
    return pageSize;
  }
}
