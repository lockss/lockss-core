/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.state;

import org.lockss.test.LockssTestCase;

public class TestPollState extends LockssTestCase {

  public void setUp() throws Exception {
    super.setUp();
    PollState state = new PollState(1, "none", null, 1, 0, null, false);
  }

  public void testCompareToEqual() {
    PollState state = new PollState(1, null, null, 1, 0, null, false);
    PollState state2 = new PollState(1, null, null, 1, 0, null, false);
    assertEquals(0, state.compareTo(state2));
    assertEquals(0, state2.compareTo(state));

    state = new PollState(1, "blah", null, 1, 0, null, false);
    state2 = new PollState(1, "blah", null, 1, 0, null, false);
    assertEquals(0, state.compareTo(state2));
    assertEquals(0, state2.compareTo(state));

    state = new PollState(1, null, "blah", 1, 0, null, false);
    state2 = new PollState(1, null, "blah", 1, 0, null, false);
    assertEquals(0, state.compareTo(state2));
    assertEquals(0, state2.compareTo(state));
  }

  public void testCompareToDiffType() {
    PollState state = new PollState(1, "none", null, 1, 0, null, false);
    PollState state2 = new PollState(2, "none", null, 1, 0, null, false);
    assertCompareIsGreaterThan(state2, state);
  }

  public void testCompareToDiffLower() {
    PollState state = new PollState(1, "none", null, 1, 0, null, false);
    PollState state2 = new PollState(1, "none2", null, 1, 0, null, false);
    PollState state3 = new PollState(1, null, null, 1, 0, null, false);
    assertCompareIsGreaterThan(state2, state);

    assertCompareIsGreaterThan(state, state3);

    assertCompareIsGreaterThan(state2, state3);
  }

  public void testCompareToDiffUpper() {
    PollState state = new PollState(1, null, "none", 1, 0, null, false);
    PollState state2 = new PollState(1, null, "none2", 1, 0, null, false);
    PollState state3 = new PollState(1, null, null, 1, 0, null, false);
    assertCompareIsGreaterThan(state2, state);

    assertCompareIsGreaterThan(state, state3);

    assertCompareIsGreaterThan(state2, state3);
  }

  public void testHashCodeEquals() {
    PollState state = new PollState(1, null, null, 1, 0, null, false);
    PollState state2 = new PollState(1, null, null, 1, 0, null, false);
    assertEquals(state.hashCode(), state2.hashCode());
  }

  /**
   * While it is allowed for hashCode for different objects to be equal, these
   * tests are meant to catch simple errors which break the hash function by
   * making it insensitive to one of the unique vars
   */
  public void testHashCodeDifferentType() {
    PollState state = new PollState(1, "none", null, 1, 0, null, false);
    PollState state2 = new PollState(2, "none", null, 1, 0, null, false);
    assertNotEquals(state.hashCode(), state2.hashCode());
  }

  public void testHashCodeDifferentLwr() {
    PollState state = new PollState(1, "none", null, 1, 0, null, false);
    PollState state2 = new PollState(1, "none2", null, 1, 0, null, false);
    PollState state3 = new PollState(1, null, null, 1, 0, null, false);
    assertNotEquals(state.hashCode(), state2.hashCode());
    assertNotEquals(state.hashCode(), state3.hashCode());
    assertNotEquals(state2.hashCode(), state3.hashCode());
  }

  public void testHashCodeDifferentUpr() {
    PollState state = new PollState(1, null, "none", 1, 0, null, false);
    PollState state2 = new PollState(1, null, "none2", 1, 0, null, false);
    PollState state3 = new PollState(1, null, null, 1, 0, null, false);
    assertNotEquals(state.hashCode(), state2.hashCode());
    assertNotEquals(state.hashCode(), state3.hashCode());
    assertNotEquals(state2.hashCode(), state3.hashCode());
  }

}
