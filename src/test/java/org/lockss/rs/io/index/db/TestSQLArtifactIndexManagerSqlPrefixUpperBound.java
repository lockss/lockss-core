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
import org.lockss.test.LockssTestCase4;

/**
 * Unit tests for {@link SQLArtifactIndexManagerSql#prefixUpperBound(String)}.
 *
 * <p>No database is involved. {@code prefixUpperBound} computes the exclusive
 * upper bound of the range predicate that the URL-prefix queries use in place of
 * {@code LIKE prefix || '%'}: for a prefix {@code p}, the set of strings starting
 * with {@code p} is exactly the half-open interval
 * {@code [p, prefixUpperBound(p))} <em>under byte-order (C) collation</em>, which
 * is why the generated SQL carries an explicit {@code COLLATE "C"}.
 *
 * <p>The increment must be by <b>code point</b>, not by {@code char}: UTF-8 byte
 * order is code point order, so incrementing the trailing {@code char} of a
 * surrogate pair would produce both a wrong bound and an unpaired surrogate.
 *
 * @see TestSQLArtifactIndexManagerSqlPrefixWildcards for the end-to-end query
 *      behaviour this bound produces.
 */
public class TestSQLArtifactIndexManagerSqlPrefixUpperBound extends LockssTestCase4 {

  /** The ordinary case: increment the last character. */
  @Test
  public void testSimpleAsciiPrefix() {
    assertEquals("abd", SQLArtifactIndexManagerSql.prefixUpperBound("abc"));
    assertEquals("b", SQLArtifactIndexManagerSql.prefixUpperBound("a"));
    assertEquals("http://example.com0",
        SQLArtifactIndexManagerSql.prefixUpperBound("http://example.com/"));
  }

  /**
   * A trailing '9' becomes ':' (0x39 -> 0x3A), and a trailing 'z' becomes '{'
   * (0x7A -> 0x7B). Neither is a "next letter"; the bound is a byte-order
   * successor, not an alphabetic one.
   */
  @Test
  public void testSuccessorIsByteOrderNotAlphabetic() {
    assertEquals("a:", SQLArtifactIndexManagerSql.prefixUpperBound("a9"));
    assertEquals("a{", SQLArtifactIndexManagerSql.prefixUpperBound("az"));
  }

  /**
   * U+FFFF is the last code point of the Basic Multilingual Plane but is
   * <em>not</em> the last code point, so it does not carry: U+FFFF + 1 =
   * U+10000, which is a valid supplementary code point. The bound is therefore
   * {@code "a" + U+10000}, not {@code "b"}.
   */
  @Test
  public void testTrailingUFFFFDoesNotCarry() {
    String prefix = "a" + (char) 0xFFFF;
    String expected = "a" + new String(Character.toChars(0x10000));

    assertEquals("U+FFFF must increment to U+10000, not carry into the previous character",
        expected, SQLArtifactIndexManagerSql.prefixUpperBound(prefix));
  }

  /**
   * U+10FFFF is the maximum code point, so it has no successor: it is dropped and
   * the carry propagates to the code point to its left.
   */
  @Test
  public void testTrailingMaxCodePointCarriesLeft() {
    String maxCp = new String(Character.toChars(Character.MAX_CODE_POINT));

    assertEquals("a trailing U+10FFFF must be dropped and the carry propagated left",
        "b", SQLArtifactIndexManagerSql.prefixUpperBound("a" + maxCp));

    assertEquals("the carry must propagate across a run of U+10FFFF",
        "b", SQLArtifactIndexManagerSql.prefixUpperBound("a" + maxCp + maxCp + maxCp));
  }

  /**
   * The increment is by code point, not by {@code char}. U+103FF is encoded as
   * the surrogate pair (high U+D800, low U+DFFF); its successor is U+10400
   * (high U+D801, low U+DC00). Incrementing the trailing {@code char} instead
   * would carry U+DFFF to U+E000, leaving the high surrogate U+D800 unpaired
   * followed by a BMP character: both the wrong bound and not encodable as UTF-8.
   *
   * <p>Note that the common emoji code points do not discriminate between the two
   * implementations; a code point whose low surrogate is U+DFFF, like this
   * one, is required to make the difference observable.
   */
  @Test
  public void testSurrogatePairIncrementsCodePointNotChar() {
    String prefix = "a" + new String(Character.toChars(0x103FF));
    String expected = "a" + new String(Character.toChars(0x10400));
    String charIncrementBug = "a" + (char) 0xD800 + (char) 0xE000;

    String actual = SQLArtifactIndexManagerSql.prefixUpperBound(prefix);

    assertEquals("U+103FF must increment to U+10400", expected, actual);
    assertFalse("the bound must not be produced by incrementing the low surrogate",
        charIncrementBug.equals(actual));
    assertEquals("the bound must end in a well-formed supplementary code point",
        0x10400, actual.codePointBefore(actual.length()));
  }

  /**
   * The successor of U+D7FF is U+E000, not U+D800: the surrogate range holds no
   * Unicode scalar values and must be skipped.
   */
  @Test
  public void testSurrogateRangeIsSkipped() {
    String actual = SQLArtifactIndexManagerSql.prefixUpperBound("a" + (char) 0xD7FF);
    String expected = "a" + (char) 0xE000;

    assertEquals("U+D7FF must increment to U+E000, skipping the surrogate range",
        expected, actual);
    assertFalse("the bound must never contain an unpaired surrogate",
        Character.isSurrogate(actual.charAt(actual.length() - 1)));
  }

  /**
   * The empty prefix has no code point to increment, so it has no upper bound:
   * every string starts with it. Callers translate null into a one-sided
   * ({@code >= ''}) predicate.
   */
  @Test
  public void testEmptyPrefixHasNoUpperBound() {
    assertNull(SQLArtifactIndexManagerSql.prefixUpperBound(""));
  }

  /**
   * A prefix consisting entirely of U+10FFFF carries all the way out of the
   * string, so it too has no upper bound.
   */
  @Test
  public void testAllMaxCodePointsHaveNoUpperBound() {
    String maxCp = new String(Character.toChars(Character.MAX_CODE_POINT));

    assertNull(SQLArtifactIndexManagerSql.prefixUpperBound(maxCp));
    assertNull(SQLArtifactIndexManagerSql.prefixUpperBound(maxCp + maxCp + maxCp));
  }

  /** Null is a caller error, not an open-ended range. */
  @Test
  public void testNullPrefixIsRejected() {
    try {
      SQLArtifactIndexManagerSql.prefixUpperBound(null);
      fail("prefixUpperBound(null) should throw IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  /**
   * The defining property: for a prefix {@code p} with bound {@code u}, a string
   * {@code s} satisfies {@code s.startsWith(p)} exactly when
   * {@code p <= s < u} in code point order. Java's {@code String.compareTo}
   * compares UTF-16 code units rather than code points, which disagrees with
   * byte order only for supplementary characters, so the samples here stay in the
   * BMP.
   */
  @Test
  public void testRangeIsEquivalentToStartsWith() {
    String[] prefixes = {"a", "http://example.com/a%b/", "my_page", "a\\b", "%", "q9"};
    String[] samples = {
        "", "a", "ab", "b", "`", "http://example.com/a%b/", "http://example.com/a%b/x",
        "http://example.com/aXb/", "http://example.com/ab/", "my_page", "my_page/x",
        "myXpage", "my-page", "a\\b", "a\\b/x", "ab", "%", "%x", "x", "q9", "q9z", "q:",
    };

    for (String p : prefixes) {
      String u = SQLArtifactIndexManagerSql.prefixUpperBound(p);
      assertNotNull("non-empty prefix should have a bound: " + p, u);

      for (String s : samples) {
        boolean inRange = s.compareTo(p) >= 0 && s.compareTo(u) < 0;
        assertEquals("prefix=[" + p + "] bound=[" + u + "] sample=[" + s + "]",
            s.startsWith(p), inRange);
      }
    }
  }
}
