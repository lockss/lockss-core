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

package org.lockss.rs.io.storage;

import org.junit.jupiter.api.Test;
import org.lockss.util.test.LockssTestCase5;

/**
 * Tests for {@link ReindexResult}, in particular the {@code aborted} flag added for the
 * #737 review's residual finding: an aborted run must not read as successful or
 * failure-free, since callers ({@code BaseLockssRepository.reindexArtifacts()},
 * {@code reindexArtifactsInListedAus()}) key resume decisions off exactly those two methods.
 */
public class TestReindexResult extends LockssTestCase5 {

  @Test
  public void testFreshResultIsSuccessful() throws Exception {
    ReindexResult result = new ReindexResult();
    assertTrue(result.isSuccessful());
    assertFalse(result.hasFailures());
    assertFalse(result.isAborted());
    assertNull(result.getAbortReason());
  }

  /**
   * The specific case the #737 follow-up fix exists for: a run that recorded no WARC
   * failures, no AU failures, and no missing AUs, but was aborted, must not be mistaken for
   * a clean success.
   */
  @Test
  public void testAbortedResultWithNoOtherFailuresIsNotSuccessful() throws Exception {
    ReindexResult result = new ReindexResult();
    result.markAborted("a worker did not confirm it stopped");

    assertFalse(result.isSuccessful(),
        "An aborted result with no other recorded failures must still not be successful");
    assertTrue(result.hasFailures(),
        "An aborted result with no other recorded failures must still report failures");
  }

  @Test
  public void testMarkAbortedIsIdempotentAndKeepsTheFirstReason() throws Exception {
    ReindexResult result = new ReindexResult();
    result.markAborted("first reason");
    result.markAborted("second reason");

    assertTrue(result.isAborted());
    assertEquals("first reason", result.getAbortReason());
  }

  /**
   * add() must propagate an aborted sub-result into the aggregate, since
   * reindexArtifactsInListedAus() aggregates one ReindexResult per AU and relies on the
   * aggregate reflecting any one AU's abort.
   */
  @Test
  public void testAddPropagatesAbortedIntoTheAggregate() throws Exception {
    ReindexResult aggregate = new ReindexResult();
    aggregate.addWarcSucceeded(3);
    assertTrue(aggregate.isSuccessful());

    ReindexResult abortedPart = new ReindexResult();
    abortedPart.markAborted("worker stuck");

    aggregate.add(abortedPart);

    assertTrue(aggregate.isAborted());
    assertEquals("worker stuck", aggregate.getAbortReason());
    assertFalse(aggregate.isSuccessful());
  }

  @Test
  public void testAddDoesNotClobberAnAlreadyAbortedAggregatesReason() throws Exception {
    ReindexResult aggregate = new ReindexResult();
    aggregate.markAborted("first AU's worker stuck");

    ReindexResult otherPart = new ReindexResult();
    otherPart.markAborted("second AU's worker stuck");

    aggregate.add(otherPart);

    assertEquals("first AU's worker stuck", aggregate.getAbortReason());
  }
}
