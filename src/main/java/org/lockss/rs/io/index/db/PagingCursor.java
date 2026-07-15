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

import java.util.Objects;

/**
 * Represents a cursor position for keyset pagination.
 *
 * <p>The cursor contains the values needed to resume pagination from a specific
 * position in the result set. Different query types may use different combinations
 * of fields:</p>
 * <ul>
 *   <li>Single-AUID queries: use sortUri and version</li>
 *   <li>All-AUIDs queries: use sortUri, version, and auid</li>
 * </ul>
 *
 * <p>Use {@link #INITIAL} for the first page of results, and {@link #of} factory
 * methods to create cursors for subsequent pages.</p>
 */
public class PagingCursor {

  /**
   * The initial cursor representing the start of pagination (first page).
   * Use this instead of null when requesting the first page of results.
   */
  public static final PagingCursor INITIAL = new PagingCursor(null, null, null);

  private final String sortUri;
  private final Integer version;
  private final String auid;

  /**
   * Creates a cursor for single-AUID queries.
   *
   * @param sortUri the sortUri of the last artifact (URL with '/' replaced by '\t')
   * @param version the version of the last artifact
   * @return a new PagingCursor
   * @throws NullPointerException if sortUri or version is null
   */
  public static PagingCursor of(String sortUri, Integer version) {
    Objects.requireNonNull(sortUri, "sortUri cannot be null");
    Objects.requireNonNull(version, "version cannot be null");
    return new PagingCursor(sortUri, version, null);
  }

  /**
   * Creates a cursor for all-AUIDs queries.
   *
   * @param sortUri the sortUri of the last artifact (URL with '/' replaced by '\t')
   * @param version the version of the last artifact
   * @param auid the AUID of the last artifact
   * @return a new PagingCursor
   * @throws NullPointerException if sortUri, version, or auid is null
   */
  public static PagingCursor of(String sortUri, Integer version, String auid) {
    Objects.requireNonNull(sortUri, "sortUri cannot be null");
    Objects.requireNonNull(version, "version cannot be null");
    Objects.requireNonNull(auid, "auid cannot be null");
    return new PagingCursor(sortUri, version, auid);
  }

  private PagingCursor(String sortUri, Integer version, String auid) {
    this.sortUri = sortUri;
    this.version = version;
    this.auid = auid;
  }

  /**
   * Returns the sortUri component of the cursor.
   * This is the URL with '/' replaced by '\t' to match the SQL sort order.
   *
   * @return the sortUri, or null if this is the initial cursor
   */
  public String getSortUri() {
    return sortUri;
  }

  /**
   * Returns the version component of the cursor.
   *
   * @return the version, or null if this is the initial cursor
   */
  public Integer getVersion() {
    return version;
  }

  /**
   * Returns the AUID component of the cursor.
   * Only used for all-AUIDs queries.
   *
   * @return the AUID, or null if not applicable or initial cursor
   */
  public String getAuid() {
    return auid;
  }

  /**
   * Returns whether this cursor has an AUID component.
   *
   * @return true if this cursor includes an AUID
   */
  public boolean hasAuid() {
    return auid != null;
  }

  /**
   * Returns whether this is the initial cursor for the first page.
   *
   * @return true if this is the {@link #INITIAL} cursor
   */
  public boolean isInitial() {
    return this == INITIAL;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PagingCursor that = (PagingCursor) o;
    return Objects.equals(sortUri, that.sortUri) &&
        Objects.equals(version, that.version) &&
        Objects.equals(auid, that.auid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortUri, version, auid);
  }

  @Override
  public String toString() {
    return "PagingCursor{" +
        "sortUri='" + sortUri + '\'' +
        ", version=" + version +
        ", auid='" + auid + '\'' +
        '}';
  }
}
