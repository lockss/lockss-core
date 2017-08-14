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

package org.lockss.daemon;

import org.lockss.test.LockssTestCase;

/**
 * This is the test class for org.lockss.daemon.SingleNodeCachedUrlSetSpec
 */

public class TestSingleNodeCachedUrlSetSpec extends LockssTestCase {

  public void testIll() {
    try {
      new SingleNodeCachedUrlSetSpec(null);
      fail("SingleNodeCachedUrlSetSpec with null url should throw");
    } catch (NullPointerException e) {
    }
  }

  public void testGetUrl() {
    CachedUrlSetSpec cuss = new SingleNodeCachedUrlSetSpec("1/2/3");
    assertEquals("1/2/3", cuss.getUrl());
  }

  public void testMatches() {
    CachedUrlSetSpec cuss1 = new SingleNodeCachedUrlSetSpec("foo");
    assertTrue(cuss1.matches("foo"));
    assertFalse(cuss1.matches("fo"));
    assertFalse(cuss1.matches("foobar"));
    assertFalse(cuss1.matches("bar"));
    assertFalse(cuss1.matches(""));
  }

  public void testEquals() throws Exception {
    CachedUrlSetSpec cuss1 = new SingleNodeCachedUrlSetSpec("foo");
    CachedUrlSetSpec cuss2 = new SingleNodeCachedUrlSetSpec("foo");
    CachedUrlSetSpec cuss3 = new SingleNodeCachedUrlSetSpec("bar");
    assertEquals(cuss1, cuss2);
    assertNotEquals(cuss1, cuss3);
    assertNotEquals(cuss1, new AuCachedUrlSetSpec());
    assertNotEquals(cuss1, new RangeCachedUrlSetSpec("foo"));
  }

  public void testHashCode() throws Exception {
    CachedUrlSetSpec spec1 = new SingleNodeCachedUrlSetSpec("foo");
    assertEquals("foo".hashCode(), spec1.hashCode());
  }

  public void testTypePredicates() {
    CachedUrlSetSpec cuss = new SingleNodeCachedUrlSetSpec("foo");
    assertTrue(cuss.isSingleNode());
    assertFalse(cuss.isAu());
    assertFalse(cuss.isRangeRestricted());
  }

  public void testDisjoint() {
    CachedUrlSetSpec cuss1 = new SingleNodeCachedUrlSetSpec("a/b");

    assertFalse(cuss1.isDisjoint(new SingleNodeCachedUrlSetSpec("a/b")));
    assertTrue(cuss1.isDisjoint(new SingleNodeCachedUrlSetSpec("a")));
    assertTrue(cuss1.isDisjoint(new SingleNodeCachedUrlSetSpec("a/b1")));
    assertTrue(cuss1.isDisjoint(new SingleNodeCachedUrlSetSpec("a/c")));

    assertFalse(cuss1.isDisjoint(new AuCachedUrlSetSpec()));

    assertFalse(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/")));
    assertFalse(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/", "b", null)));
    assertFalse(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/", null, "b")));
    assertTrue(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/", "c", null)));
    assertTrue(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/", null, "a")));
    assertTrue(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/c")));

    // not disjoint with unrestricted RCUSS at same node, disjoint with a
    // restricted one
    assertFalse(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/b")));
    assertTrue(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/b", "", null)));
    assertTrue(cuss1.isDisjoint(new RangeCachedUrlSetSpec("a/b", null, "z")));
  }

  public void testSubsumes() {
    CachedUrlSetSpec cuss = new SingleNodeCachedUrlSetSpec("foo");

    assertTrue(cuss.subsumes(new SingleNodeCachedUrlSetSpec("foo")));
    assertFalse(cuss.subsumes(new SingleNodeCachedUrlSetSpec("foo/bar")));
    assertFalse(cuss.subsumes(new SingleNodeCachedUrlSetSpec("fo")));
    assertFalse(cuss.subsumes(new SingleNodeCachedUrlSetSpec("bar")));

    assertFalse(cuss.subsumes(new RangeCachedUrlSetSpec("foo")));
    assertFalse(cuss.subsumes(new RangeCachedUrlSetSpec("bar")));
    assertFalse(cuss.subsumes(new RangeCachedUrlSetSpec("foo", "1", "2")));
    assertFalse(cuss.subsumes(new RangeCachedUrlSetSpec("foo", null, "")));

    assertFalse(cuss.subsumes(new AuCachedUrlSetSpec()));
  }

}
