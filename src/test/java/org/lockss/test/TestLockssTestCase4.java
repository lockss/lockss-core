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

package org.lockss.test;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import org.junit.Test;

public class TestLockssTestCase4 extends LockssTestCase4 {

  @Test
  public void testLoggerName() {
    assertEquals("[Logger LockssTest4]", log.toString());
  }
  
  @Test
  public void testGetTestMethodName() {
    assertEquals("testGetTestMethodName", getTestMethodName());
  }
  
  @Test
  public void testCartesian() {
    String[] strings = {"foo", "bar"};
    Character[] chars = {'a', 'b', 'c'};
    Integer[] ints = {1, 2, 3, 4};
    Collection<Object[]> tuples = LockssTestCase4.cartesian(strings, chars, ints);
    assertEquals(strings.length * chars.length * ints.length, tuples.size());
    for (int s = 0 ; s < strings.length ; ++s) {
      for (int c = 0 ; c < chars.length ; ++c) {
        for (int i = 0 ; i < ints.length ; ++i) {
          assertThat(tuples, hasItem(new Object[] {strings[s], chars[c], ints[i]}));
        }
      }
    }
  }
  
}
