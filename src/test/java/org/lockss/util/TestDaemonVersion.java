/*
 * $Id$
 */

/*

Copyright (c) 2000-2003 Board of Trustees of Leland Stanford Jr. University,
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
import java.util.Arrays;

import org.lockss.test.*;

/**
 * Test class for org.lockss.util.DaemonVersion
 *
 */

public class TestDaemonVersion extends LockssTestCase {
  public static Class testedClasses[] = {
    org.lockss.util.DaemonVersion.class
  };

  public void testMakeVersion() {
    try {
      Version a = new DaemonVersion("1.0.0");
      Version b = new DaemonVersion("2.3.1-test");
    } catch (Throwable t) {
      fail("Unparsable daemon version. " + t);
    }
  }

  public void testEquals() {
    Version a = new DaemonVersion("2.3.3");
    Version b = new DaemonVersion("2.3.3");
    assertEquals(a.toLong(), b.toLong());

    Version c = new DaemonVersion("2.3.3-test");
    assertEquals(a.toLong(), c.toLong());

    Version d = new DaemonVersion("2.3.4");
    assertNotEquals(a.toLong(), d.toLong());
  }

  public void testLegalFormat() throws Exception {
    for (String v : Arrays.asList("0.0.0",
                                  "0-test.0-test.0-test",
                                  "1.1.1",
                                  "1-test.1-test.1-test",
                                  "2.0.1",
                                  "2.0.4-alpha9",
                                  "2.1.16-alpha9-FOO",
                                  "2-test.0-test4.77-sunsetstrip",
                                  "999.999.888")) {
      new DaemonVersion(v);
    }
  }
  
  public void testIllegalFormat() {
    assertIllegalFormat("1.2.3.4");
    assertIllegalFormat("1234.123.123");
    assertIllegalFormat("1.2.3.4");
    assertIllegalFormat("1.2.$");
    assertIllegalFormat("2.3b.5");
    assertIllegalFormat("2.b.5");
    assertIllegalFormat("2.0.5.9");
    assertIllegalFormat("2.0.5.9.9");
  }

  public void testDisplayString() {
    assertEquals("1.2.3", new DaemonVersion("1.2.3").displayString());
    assertEquals("1.2.3-test", new DaemonVersion("1.2.3-test").displayString());
  }

  public void testToLong() {
    try {
      Version a = new DaemonVersion("123.123.4");
    } catch (IllegalArgumentException ex) {
      fail("Should not have thrown.");
    }

    try {
      Version a = new DaemonVersion("1234.123.5");
      fail("Should have thrown.");
    } catch (IllegalArgumentException ex) {
    }

    try {
      Version a = new DaemonVersion("123.1234.0");
      fail("Should have thrown.");
    } catch (IllegalArgumentException ex) {
    }

    // I mean, this doesn't really test toLong()...
  }

  void assertIllegalFormat(String s) {
    try {
      Version a = new DaemonVersion(s);
      fail("Daemon version '" + s + "' should be illegal, but wasn't.");
    } catch (IllegalArgumentException ex) {
    }
  }

  public void testGreaterOrLessThan() {
    Version a = new DaemonVersion("2.5.1");
    Version b = new DaemonVersion("2.6.1");
    Version c = new DaemonVersion("3.0.1");
    Version d = new DaemonVersion("3.0.2");
    Version e = new DaemonVersion("2.5.1-test"); // should equal a

    assertTrue(a.toLong() < b.toLong());
    assertTrue(a.toLong() < c.toLong());
    assertTrue(a.toLong() < d.toLong());
    assertTrue(b.toLong() < c.toLong());
    assertTrue(c.toLong() < d.toLong());
    assertFalse(a.toLong() < e.toLong());
    assertEquals(a.toLong(), e.toLong());
  }

  public void testCompareTo() {
    DaemonVersion a = new DaemonVersion("2.5.3");
    DaemonVersion b = new DaemonVersion("2.6.3");
    DaemonVersion c = new DaemonVersion("3.0.1");
    DaemonVersion e = new DaemonVersion("2.5.3-test"); // should equal a

    assertTrue(a.compareTo(a) == 0);
    assertTrue(a.compareTo(e) == 0);
    assertTrue(a.compareTo(b) == -1);
    assertTrue(b.compareTo(a) == 1);
  }

}
