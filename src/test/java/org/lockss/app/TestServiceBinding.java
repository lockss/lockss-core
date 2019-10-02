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

package org.lockss.app;

import org.lockss.test.*;
import org.lockss.util.*;

public class TestServiceBinding extends LockssTestCase {
  private final static Logger log = Logger.getLogger();

  public void testEqualsHash() {
    assertEquals(new ServiceBinding("foo", 12345, 0),
		 new ServiceBinding("foo", 12345, 0));
    assertEquals(new ServiceBinding("foo", 12345, 123),
		 new ServiceBinding("foo", 12345, 123));
    assertNotEquals(new ServiceBinding("foo", 12345, 123),
		    new ServiceBinding("bar", 12345, 123));
    assertNotEquals(new ServiceBinding("foo", 12345, 123),
		    new ServiceBinding("foo", 12346, 123));
    assertNotEquals(new ServiceBinding("foo", 12345, 123),
		    new ServiceBinding("foo", 12345, 124));
    assertNotEquals(new ServiceBinding("foo", 12345, 123),
		    new ServiceBinding("foo", 0, 123));
    assertNotEquals(new ServiceBinding("foo", 12345, 123),
		    new ServiceBinding("foo", 12345, 0));

    assertEquals(new ServiceBinding("foo", 12345, 0).hashCode(),
		 new ServiceBinding("foo", 12345, 0).hashCode());
    assertEquals(new ServiceBinding("foo", 12345, 123).hashCode(),
		 new ServiceBinding("foo", 12345, 123).hashCode());
    assertNotEquals(new ServiceBinding("foo", 12345, 123).hashCode(),
		    new ServiceBinding("bar", 12345, 123).hashCode());
    assertNotEquals(new ServiceBinding("foo", 12345, 123).hashCode(),
		    new ServiceBinding("foo", 12346, 123).hashCode());
    assertNotEquals(new ServiceBinding("foo", 12345, 123).hashCode(),
		    new ServiceBinding("foo", 12345, 124).hashCode());
    assertNotEquals(new ServiceBinding("foo", 12345, 123).hashCode(),
		    new ServiceBinding("foo", 0, 123).hashCode());
    assertNotEquals(new ServiceBinding("foo", 12345, 123).hashCode(),
		    new ServiceBinding("foo", 12345, 0).hashCode());
  }

  public void testGetRestStem() {
    assertEquals("http://foo.host:22222",
		 new ServiceBinding("foo.host", 22222, 12345).getRestStem());
    assertEquals("http://localhost:666",
		 new ServiceBinding(null, 666, 0).getRestStem());
    assertEquals(666, new ServiceBinding(null, 666, 0).getRestPort());
    assertTrue(new ServiceBinding(null, 666, 0).hasRestPort());
    assertFalse(new ServiceBinding(null, 0, 666).hasRestPort());
  }

  public void testGetUiStem() {
    assertEquals("https://foo.host:12345",
		 new ServiceBinding("foo.host", 0, 12345).getUiStem("https"));
    assertEquals("http://localhost:666",
		 new ServiceBinding(null, 123, 666).getUiStem("http"));
    assertEquals(666, new ServiceBinding(null, 111, 666).getUiPort());
    assertTrue(new ServiceBinding(null, 111, 666).hasUiPort());
    assertFalse(new ServiceBinding(null, 111, 0).hasUiPort());
  }
}

