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
package org.lockss.protocol;

import java.util.*;
import java.util.stream.*;
import org.junit.*;

import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.state.*;

public class TestPersistentPeerIdSet extends StateTestCase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Override
  protected StateManager makeStateManager() {
    return new InMemoryStateManager();
  }

  @Test
  public void testOps() throws Exception {
    MyPersistentPeerIdSetImpl set = new MyPersistentPeerIdSetImpl(idMgr);
    MyPersistentPeerIdSetImpl set2 = new MyPersistentPeerIdSetImpl(idMgr);

    assertTrue(set.isEmpty());
    assertEquals(0, set.size());
    assertFalse(set.iterator().hasNext());

    assertEquals(set, set2);
    assertEquals(set.hashCode(), set2.hashCode());

    set.add(pid0);
    assertFalse(set.isEmpty());
    assertEquals(1, set.size());
    assertTrue(set.contains(pid0));
    assertFalse(set.contains(pid1));
    assertFalse(set.contains(pid2));
    assertFalse(set.contains(pid3));
    assertEquals(ListUtil.list(pid0), ListUtil.fromIterator(set.iterator()));
    assertNotEquals(set, set2);

    set.addAll(ListUtil.list(pid2, pid3));
    assertFalse(set.isEmpty());
    assertEquals(3, set.size());
    assertTrue(set.contains(pid0));
    assertFalse(set.contains(pid1));
    assertTrue(set.contains(pid2));
    assertTrue(set.contains(pid3));
    assertTrue(set.containsAll(ListUtil.list(pid2, pid3)));
    assertTrue(set.containsAll(ListUtil.list(pid2, pid3, pid0)));
    assertEquals(SetUtil.set(pid0, pid2, pid3),
		 SetUtil.fromIterator(set.iterator()));

    set2.addAll(ListUtil.list(pid0, pid2, pid3));
    assertEquals(set, set2);
    assertEquals(set.hashCode(), set2.hashCode());

    assertFalse(set.remove(pid1));
    assertEquals(3, set.size());
    assertTrue(set.remove(pid2));
    assertEquals(2, set.size());
    assertTrue(set.containsAll(ListUtil.list(pid0, pid3)));
    assertFalse(set.contains(pid1));
    assertFalse(set.contains(pid2));

    set.clear();
    assertTrue(set.isEmpty());
    assertEquals(0, set.size());
    assertFalse(set.contains(pid0));
    assertFalse(set.iterator().hasNext());
    
    set2.removeAll(ListUtil.list(pid0, pid3));
    assertEquals(1, set2.size());
    assertTrue(set2.contains(pid2));
    set2.addAll(ListUtil.list(pid0, pid3));
    set2.retainAll(ListUtil.list(pid0, pid1, pid3));
    assertFalse(set2.contains(pid2));
    assertTrue(set2.contains(pid0));
    assertTrue(set2.contains(pid3));

    Object[] pids = set2.toArray();
    assertEquals(SetUtil.set(pid0, pid3),
		 Arrays.stream(pids).collect(Collectors.toSet()));
  }

  static class MyPersistentPeerIdSetImpl extends PersistentPeerIdSetImpl {
    public MyPersistentPeerIdSetImpl(IdentityManager identityManager) {
      super(identityManager);
    }
  }
}
