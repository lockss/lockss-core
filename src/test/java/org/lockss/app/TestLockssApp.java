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

package org.lockss.app;

import java.io.*;
import java.util.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.daemon.*;
import org.lockss.plugin.*;

/**
 * This is the test class for org.lockss.util.LockssApp
 */
public class TestLockssApp extends LockssTestCase {
  private final static Logger log = Logger.getLogger(TestLockssApp.class);

  MyMockLockssApp app;
  private String tempDirPath;

  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();

    app = new MyMockLockssApp(null);
  }

  public void testGetStartupOptions() throws Exception {
    // good options.
    String[] test1 = {"-p", "foo;bar;baz",
		      "-g", "test1-group"};
    String[] test2 = {"-p", "foo",
		      "-p", "bar",
		      "-p", "baz"};
    String[] test3 = {"-p", "foo;bar;baz",
		      "-p", "quux",
		      "-g", "test3-group"};
    String[] test4 = {"-p", "foo1;bar1;baz1",
		      "-p", "foo2;bar2;baz2"};


    // bad options (-p without argument, should be ignored)
    String[] test5 = {"-p", "foo",
		      "-p"};
    // bad options (-p without argument, should be ignored)
    String[] test6 = {"-g", "test6-group",
		      "-p"};
    // bad options (-g without argument, should be ignored)
    String[] test7 = {"-p", "foo",
		      "-g"};

    // Good options, including -x.
    String[] test9 = {"-p", "foo;bar;baz",
	      "-g", "test1-group", "-x", tempDirPath};

    // bad options (-x without argument, should be ignored)
    String[] test10 = {"-p", "foo",
		      "-x"};

    // Good options, including -b.
    String[] test11 = {"-p", "foo;bar;baz",
	      "-g", "test1-group", "-b", "boot"};

    // bad options (-b without argument, should be ignored)
    String[] test12 = {"-p", "foo",
		      "-b"};

    // Ensure that only one URL is chosen from a semicolon-separated
    // list of URLs
    LockssDaemon.StartupOptions opt1 =
      LockssDaemon.getStartupOptions(test1);
    assertNotNull(opt1.getGroupNames());
    assertEquals("test1-group", opt1.getGroupNames());
    List<String> list1 = opt1.getPropUrls();
    assertNotNull(list1);
    assertEquals(1, list1.size());
    assertTrue("foo".equals(list1.get(0)) ||
	       "bar".equals(list1.get(0)) ||
	       "baz".equals(list1.get(0)));
    assertNull(opt1.getBootstrapPropsUrl());

    // Ensure that multiple prop URLs can be set with multiple "-p"
    // options.
    LockssDaemon.StartupOptions opt2 =
      LockssDaemon.getStartupOptions(test2);
    // Must be null!  No group specified.
    assertNull(opt2.getGroupNames());
    List<String> list2 = opt2.getPropUrls();
    assertNotNull(list2);
    assertEquals(3, list2.size());
    assertEquals("foo", list2.get(0));
    assertEquals("bar", list2.get(1));
    assertEquals("baz", list2.get(2));
    assertNull(opt2.getBootstrapPropsUrl());

    // Ensure that only one URL is chosen from a semicolon-separated
    // list of URLs, and that additional -p parameters can be provided.
    LockssDaemon.StartupOptions opt3 =
      LockssDaemon.getStartupOptions(test3);
    assertNotNull(opt3.getGroupNames());
    assertEquals("test3-group", opt3.getGroupNames());
    List<String> list3 = opt3.getPropUrls();
    assertNotNull(list3);
    assertEquals(2, list3.size());
    assertTrue("foo".equals(list3.get(0)) ||
	       "bar".equals(list3.get(0)) ||
	       "baz".equals(list3.get(0)));
    assertEquals("quux", list3.get(1));
    assertNull(opt3.getBootstrapPropsUrl());

    // Ensure that only one URL is chosen from each semicolon-separated
    // list of URLs
    LockssDaemon.StartupOptions opt4 =
      LockssDaemon.getStartupOptions(test4);
    assertNull(opt4.getGroupNames());
    List<String> list4 = opt4.getPropUrls();
    assertNotNull(list4);
    assertEquals(2, list4.size());
    assertTrue("foo1".equals(list4.get(0)) ||
	       "bar1".equals(list4.get(0)) ||
	       "baz1".equals(list4.get(0)));
    assertTrue("foo2".equals(list4.get(1)) ||
	       "bar2".equals(list4.get(1)) ||
	       "baz2".equals(list4.get(1)));
    assertNull(opt4.getBootstrapPropsUrl());

    // Test some bad options.  Second -p should be ignored.
    LockssDaemon.StartupOptions opt5 =
      LockssDaemon.getStartupOptions(test5);
    assertNull(opt5.getGroupNames());
    List<String> list5 = opt5.getPropUrls();
    assertEquals(1, list5.size());
    assertEquals("foo", list5.get(0));
    assertNull(opt5.getBootstrapPropsUrl());

    // -p should be ignored, no prop URLS.
    LockssDaemon.StartupOptions opt6 =
      LockssDaemon.getStartupOptions(test6);
    assertNotNull(opt6.getGroupNames());
    assertEquals("test6-group", opt6.getGroupNames());
    List<String> list6 = opt6.getPropUrls();
    assertNotNull(list6);
    assertEquals(0, list6.size());
    assertNull(opt6.getBootstrapPropsUrl());

    // -g should be ignored, no group name.
    LockssDaemon.StartupOptions opt7 =
      LockssDaemon.getStartupOptions(test7);
    assertNull(opt7.getGroupNames());
    List<String> list7 = opt7.getPropUrls();
    assertNotNull(list7);
    assertEquals(1, list7.size());
    assertEquals("foo", list7.get(0));
    assertNull(opt7.getBootstrapPropsUrl());

    // Test -x.
    String xmlFilename = "file.xml";
    FileOutputStream fos =
	new FileOutputStream(new File(tempDirPath, xmlFilename));
    InputStream sis = new StringInputStream("some content");
    StreamUtil.copy(sis, fos);
    sis.close();
    fos.close();

    LockssDaemon.StartupOptions opt9 =
      LockssDaemon.getStartupOptions(test9);
    assertNotNull(opt9.getGroupNames());
    assertEquals("test1-group", opt9.getGroupNames());
    List<String> list9 = opt9.getPropUrls();
    assertNotNull(list9);
    assertEquals(2, list9.size());
    assertTrue("foo".equals(list9.get(0)) ||
	       "bar".equals(list9.get(0)) ||
	       "baz".equals(list9.get(0)));
    assertTrue(list9.get(1).endsWith(File.separator + xmlFilename));

    // Test some bad options. -x should be ignored.
    LockssDaemon.StartupOptions opt10 =
      LockssDaemon.getStartupOptions(test10);
    assertNull(opt10.getGroupNames());
    List<String> list10 = opt10.getPropUrls();
    assertEquals(1, list10.size());
    assertNull(opt10.getBootstrapPropsUrl());

    // Test -b.
    LockssDaemon.StartupOptions opt11 =
      LockssDaemon.getStartupOptions(test11);
    assertNotNull(opt11.getGroupNames());
    assertEquals("test1-group", opt11.getGroupNames());
    List<String> list11 = opt11.getPropUrls();
    assertNotNull(list11);
    assertEquals(2, list11.size());
    assertTrue("foo".equals(list11.get(0)) ||
	       "bar".equals(list11.get(0)) ||
	       "baz".equals(list11.get(0)));
    assertTrue("boot".equals(list11.get(1)));
    assertEquals("boot", opt11.getBootstrapPropsUrl());

    // Test some bad options. -b should be ignored.
    LockssDaemon.StartupOptions opt12 =
      LockssDaemon.getStartupOptions(test12);
    assertNull(opt12.getGroupNames());
    List<String> list12 = opt12.getPropUrls();
    assertEquals(1, list12.size());
    assertNull(opt12.getBootstrapPropsUrl());
  }

  // load & init default manager
  public void testInitManagerNoParam() throws Exception {
    LockssApp.ManagerDesc d1 =
      new LockssApp.ManagerDesc("param", mockMgrName);
    LockssManager m1 = app.initManager(d1);
    assertTrue(((MyMockMgr)m1).isInited());
  }

  // configure alternate manager class
  public void testInitManagerParam() throws Exception {
    ConfigurationUtil.setFromArgs(LockssApp.MANAGER_PREFIX + "param",
				  mockMgrName);
    LockssApp.ManagerDesc d1 =
      new LockssApp.ManagerDesc("param", "not.found");
    LockssManager m1 = app.initManager(d1);
    assertTrue(((MyMockMgr)m1).isInited());
  }

  // if configured class not found, fall back to default class
  public void testInitManagerParamFallback() throws Exception {
    ConfigurationUtil.setFromArgs(LockssApp.MANAGER_PREFIX + "param",
				  "not.found.class");
    LockssApp.ManagerDesc d1 =
      new LockssApp.ManagerDesc("param", mockMgrName);
    LockssManager m1 = app.initManager(d1);
    assertTrue(((MyMockMgr)m1).isInited());
  }

  // fail if class not LockssManager
  public void testInitManagerNotManager() throws Exception {
    LockssApp.ManagerDesc d1 =
      new LockssApp.ManagerDesc("param", "java.lang.String");
    try {
      LockssManager m1 = app.initManager(d1);
      fail("initManager() shouldn't succeed on non-LockssManager class");
    } catch (ClassCastException e) {
    }
  }

  // Configured class not LockssManager shouldn't cause fallback to default
  public void testInitManagerParamNoFallbackIfNotManager() throws Exception {
    ConfigurationUtil.setFromArgs(LockssApp.MANAGER_PREFIX + "param",
				  "java.lang.String");
    LockssApp.ManagerDesc d1 =
      new LockssApp.ManagerDesc("param", mockMgrName);
    try {
      LockssManager m1 = app.initManager(d1);
      fail("initManager() shouldn't succeed on non-LockssManager class");
    } catch (ClassCastException e) {
    }
  }

  public void testInitManagers() throws Exception {
    LockssApp.ManagerDesc[] descrs = {
      new LockssApp.ManagerDesc("mgr_1", MockMgr1.class.getName()),
      new LockssApp.ManagerDesc("mgr_2", MockMgr2.class.getName()) {
      public boolean shouldStart() {
	return false;
      }},
      new LockssApp.ManagerDesc("mgr_3", MockMgr3.class.getName()),
    };
    app.setDescrs(descrs);

    app.initManagers();

    MockLockssManager mgr1 = (MockLockssManager)LockssApp.getManager("mgr_1");
    assertTrue(mgr1 instanceof MockMgr1);
    assertEquals(1, mgr1.inited);
    assertEquals(1, mgr1.started);
    assertEquals(0, mgr1.stopped);
    MockLockssManager mgr3 = (MockLockssManager)LockssApp.getManager("mgr_3");
    assertTrue(mgr3 instanceof MockMgr3);
    assertEquals(1, mgr3.inited);
    assertEquals(1, mgr3.started);
    assertEquals(0, mgr3.stopped);
    try {
      LockssApp.getManager("mgr_2");
      fail("mgr_2 shouldn't have been created");
    } catch (IllegalArgumentException e) {
    }

    app.stop();
    assertEquals(1, mgr1.stopped);
    assertEquals(1, mgr3.stopped);
  }

  static final String mockMgrName = MyMockMgr.class.getName();
  static class MyMockMgr implements LockssManager {
    boolean isInited = false;

    public void initService(LockssApp app)
	throws LockssAppException {
      isInited = true;
    }

    public void startService() {
    }

    public void stopService() {
    }
    public LockssApp getApp() {
      throw new UnsupportedOperationException("Not implemented");
    }

    boolean isInited() {
      return isInited;
    }
  }

  static class Event {
    Object caller;
    String event;
    Object arg;
    Event(Object caller, String event, Object arg) {
      this.caller = caller;
      this.event = event;
      this.arg = arg;
    }
    Event(Object caller, String event) {
      this(caller, event, null);
    }
    public String toString() {
      return "[Ev: " + caller + "." + event + "(" + arg + ")";
    }
    public boolean equals(Object o) {
      if (o instanceof Event) {
	Event oe = (Event)o;
	return caller == oe.caller && arg == oe.arg &&
	  StringUtil.equalStrings(event, oe.event);
      }
      return false;
    }
  }
  List events;

  public static class MyMockLockssApp extends LockssApp {
    ManagerDesc[] descrs = null;

    MyMockLockssApp() {
      super();
    }

    MyMockLockssApp(List propUrls) {
      super(propUrls);
    }

    protected ManagerDesc[] getManagerDescs() {
      if (descrs == null) {
	return super.getManagerDescs();
      }
      return descrs;
    }

    protected ManagerDesc[] getAppManagerDescs() {
      return new ManagerDesc[0];
    }
 
    void setDescrs(ManagerDesc[] descrs) {
      this.descrs = descrs;
    }

    protected void systemExit(int val) {
      log.critical("System.exit(" + val + ")");
      throw new RuntimeException("System.exit(" + val + ")");
    }
  }

  static class MockLockssManager implements LockssManager {
    int inited = 0;
    int started = 0;
    int stopped = 0;

    public void initService(LockssApp app) throws LockssAppException {
      inited++;
    }

    public void startService() {
      started++;
    }

    public void stopService() {
      stopped++;
    }

    public LockssApp getApp() {
      return null;
    }
  }

  static class MockMgr1 extends MockLockssManager {
  }

  static class MockMgr2 extends MockLockssManager {
  }

  static class MockMgr3 extends MockLockssManager {
  }
}
