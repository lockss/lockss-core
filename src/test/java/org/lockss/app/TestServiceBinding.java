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
    ServiceBinding sb1 = new ServiceBinding("foo", 12345);
    ServiceBinding sb2 = new ServiceBinding(null, 12347);
    ServiceBinding sb3 = new ServiceBinding("foo", 0);
    assertEquals(new ServiceBinding("foo", 12345), sb1);
    assertEquals(new ServiceBinding("foo", 12345).hashCode(), sb1.hashCode());

    assertNotEquals(new ServiceBinding("fooo", 12345), sb1);
    assertNotEquals(new ServiceBinding("fooo", 12345).hashCode(),
		    sb1.hashCode());
    assertNotEquals(new ServiceBinding("foo", 12346), sb1);
    assertEquals(new ServiceBinding(null, 12347), sb2);
    assertEquals(new ServiceBinding(null, 12347).hashCode(), sb2.hashCode());
    assertNotEquals(new ServiceBinding(null, 12346), sb2);
    assertNotEquals(new ServiceBinding("xx", 12347), sb2);
  }

  public void testGetUiStem() {
    assertEquals("https://foo.host:12345",
		 new ServiceBinding("foo.host", 12345).getUiStem("https"));
    assertEquals("http://localhost:666",
		 new ServiceBinding(null, 666).getUiStem("http"));
  }


}

