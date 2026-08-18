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

package org.lockss.rs.io.index.db;

import org.junit.Test;
import org.lockss.test.LockssTestCase4;

import java.util.ArrayList;
import java.util.List;

import static org.lockss.rs.io.index.db.SQLArtifactIndexManagerSql.URL_ANALYZE_CAP_ROWS;
import static org.lockss.rs.io.index.db.SQLArtifactIndexManagerSql.URL_ANALYZE_MIN_ROWS;
import static org.lockss.rs.io.index.db.SQLArtifactIndexManagerSql.shouldAnalyzeUrls;

/**
 * Unit tests for the geometric urls-table ANALYZE cadence
 * ({@link SQLArtifactIndexManagerSql#shouldAnalyzeUrls}). Pure decision logic;
 * no database required.
 */
public class TestAnalyzeCadence extends LockssTestCase4 {

  /** On a cold table (last ANALYZE at 0 rows) the first refresh waits for the
   *  floor and does not fire before it. */
  @Test
  public void testColdStartFiresAtFloor() {
    assertFalse(shouldAnalyzeUrls(0, 0));
    assertFalse(shouldAnalyzeUrls(URL_ANALYZE_MIN_ROWS - 1, 0));
    assertTrue(shouldAnalyzeUrls(URL_ANALYZE_MIN_ROWS, 0));
    assertTrue(shouldAnalyzeUrls(URL_ANALYZE_MIN_ROWS + 1, 0));
  }

  /** While the table is smaller than the floor, growth of at least the floor
   *  (not merely doubling) is required, so we don't ANALYZE on every batch. */
  @Test
  public void testFloorDominatesWhenLastAnalyzeIsSmall() {
    long last = 100; // well below the floor
    // Doubling alone (to 200) is not enough; need last + floor.
    assertFalse(shouldAnalyzeUrls(200, last));
    assertFalse(shouldAnalyzeUrls(last + URL_ANALYZE_MIN_ROWS - 1, last));
    assertTrue(shouldAnalyzeUrls(last + URL_ANALYZE_MIN_ROWS, last));
  }

  /** Once past the floor, the trigger is a straight doubling of the size at the
   *  last ANALYZE. */
  @Test
  public void testDoublingOncePastFloor() {
    long last = 50_000;
    assertFalse(shouldAnalyzeUrls(last, last));
    assertFalse(shouldAnalyzeUrls(2 * last - 1, last));
    assertTrue(shouldAnalyzeUrls(2 * last, last));
    assertTrue(shouldAnalyzeUrls(2 * last + 1, last));
  }

  /** The estimate may be seeded from a non-empty index at startup; the very next
   *  ANALYZE is then a doubling of that seeded size, not the floor. */
  @Test
  public void testSeededNonEmptyStartDoublesFromSeed() {
    long seed = 250_000;
    assertFalse(shouldAnalyzeUrls(seed + URL_ANALYZE_MIN_ROWS, seed));
    assertFalse(shouldAnalyzeUrls(2 * seed - 1, seed));
    assertTrue(shouldAnalyzeUrls(2 * seed, seed));
  }

  /** At or above the cap we never ANALYZE, however much the table has grown;
   *  just below the cap (with enough growth) we still do. */
  @Test
  public void testCapStopsAnalyze() {
    // Way past the doubling threshold, but at/above the cap: no ANALYZE.
    assertFalse(shouldAnalyzeUrls(URL_ANALYZE_CAP_ROWS, 1));
    assertFalse(shouldAnalyzeUrls(URL_ANALYZE_CAP_ROWS + 1_000_000, 100));

    // Just below the cap, having doubled since the last ANALYZE: still fires.
    long last = URL_ANALYZE_CAP_ROWS / 4;
    assertTrue(shouldAnalyzeUrls(URL_ANALYZE_CAP_ROWS - 1, last));
  }

  /**
   * Walks the table's growth in batch-sized steps and records every point at
   * which an ANALYZE fires. Verifies the emergent schedule: the first refresh is
   * at the floor, each subsequent one is at exactly double the previous, all are
   * below the cap, and the schedule stops precisely because the next doubling
   * would reach the cap.
   */
  @Test
  public void testGeometricScheduleOverFullGrowth() {
    final long batch = 1000; // maybeAnalyzeUrls() is consulted per committed batch

    long lastAnalyze = 0;
    List<Long> analyzedAt = new ArrayList<>();

    for (long rows = batch; rows <= 4 * URL_ANALYZE_CAP_ROWS; rows += batch) {
      if (shouldAnalyzeUrls(rows, lastAnalyze)) {
        analyzedAt.add(rows);
        lastAnalyze = rows;
      }
    }

    assertFalse("expected at least one ANALYZE", analyzedAt.isEmpty());

    // First refresh is at the floor.
    assertEquals(URL_ANALYZE_MIN_ROWS, (long) analyzedAt.get(0));

    // Each subsequent refresh is at double the previous.
    for (int i = 1; i < analyzedAt.size(); i++) {
      assertEquals("ANALYZE #" + i + " should be double the previous",
          2 * analyzedAt.get(i - 1), (long) analyzedAt.get(i));
    }

    // Every refresh is below the cap...
    long lastPoint = analyzedAt.get(analyzedAt.size() - 1);
    assertTrue("last ANALYZE (" + lastPoint + ") should be below the cap",
        lastPoint < URL_ANALYZE_CAP_ROWS);
    // ...and the schedule stopped because the next doubling would reach the cap.
    assertTrue("schedule should stop only because the next doubling hits the cap",
        2 * lastPoint >= URL_ANALYZE_CAP_ROWS);

    // No ANALYZE was ever scheduled at or beyond the cap.
    for (long point : analyzedAt) {
      assertTrue("no ANALYZE should fire at/after the cap: " + point,
          point < URL_ANALYZE_CAP_ROWS);
    }
  }
}
