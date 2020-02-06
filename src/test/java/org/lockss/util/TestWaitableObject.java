/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of Stanford University shall not
be used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from Stanford University.

*/

package org.lockss.util;

import java.util.*;
import java.io.*;
import org.junit.*;
import org.lockss.daemon.*;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimerUtil;
import org.lockss.test.*;


public class TestWaitableObject extends LockssTestCase4 {

  /** Storer stores in a WaitableObject in a while */
  class Storer<V> extends DoLater {
    WaitableObject<V> wo;
    V value;

    Storer(long waitMs, WaitableObject<V> wo, V val) {
      super(waitMs);
      this.wo = wo;
      this.value = val;
    }

    protected void doit() {
      wo.setValue(value);
    }
  }

  @Test
  public void testTimeout() {
    WaitableObject<String> wo = new WaitableObject<>(10);
    assertFalse(wo.hasValue());
    try {
      String val = wo.waitValue();
      fail("Should have timed out");
    } catch (IllegalStateException e) {
    }
    assertFalse(wo.hasValue());
  }

  @Test
  public void testNorm() {
    WaitableObject<String> wo = new WaitableObject<>();
    Storer s = new Storer(500, wo, "foo");
    assertFalse(wo.hasValue());
    s.start();
    assertEquals("foo", wo.waitValue());
    assertTrue(wo.hasValue());

    wo.reset();
    assertFalse(wo.hasValue());
    try {
      String val = wo.waitValue(10);
      fail("Should have timed out");
    } catch (IllegalStateException e) {
    }

    s = new Storer(100, wo, "bar");
    assertFalse(wo.hasValue());
    s.start();
    assertEquals("bar", wo.waitValue());
    assertTrue(wo.hasValue());
  }
}

