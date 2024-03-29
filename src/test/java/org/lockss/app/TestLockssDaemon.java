/*

Copyright (c) 2000-2017 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.*;
import org.apache.commons.lang3.JavaVersion;
import org.junit.Before;
import org.junit.Test;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.plugin.*;

/**
 * This is the test class for org.lockss.util.LockssDaemon
 */
public class TestLockssDaemon extends LockssTestCase4 {

  private String tempDirPath;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();
  }

  // AU specific manager tests

  @Test
  public void testStartAuManager() throws Exception {
    LockssDaemon daemon = new TestAuLockssDaemon();
    Configuration config = ConfigManager.newConfiguration();
    MockArchivalUnit mau1 = new MockArchivalUnit();
    mau1.setAuId("mau1");
    MockArchivalUnit mau2 = new MockArchivalUnit();
    mau2.setAuId("mau2");

    TestAuMgr.clearEvents();
    daemon.startOrReconfigureAuManagers(mau1, config);

    TestAuMgr1 mgr11 = (TestAuMgr1)daemon.getAuManager("MgrKey1", mau1);
    TestAuMgr2 mgr21 = (TestAuMgr2)daemon.getAuManager("MgrKey2", mau1);
    assertSame(mau1, mgr11.getAu());
    assertSame(mau1, mgr21.getAu());
    assertEquals(ListUtil.list(mgr11), daemon.getAuManagersOfType("MgrKey1"));
    assertEquals(ListUtil.list(mgr21), daemon.getAuManagersOfType("MgrKey2"));
    Event[] exp1 = {
      new Event(mgr11, "initService", daemon),
      new Event(mgr21, "initService", daemon),
      new Event(mgr11, "setAuConfig", config),
      new Event(mgr21, "setAuConfig", config),
      new Event(mgr11, "startService", null),
      new Event(mgr21, "startService", null),
    };
    assertIsomorphic(exp1, TestAuMgr.getEvents());
    Configuration config2 = ConfigManager.newConfiguration();
    config2.put("1", "2");
    TestAuMgr.clearEvents();
    daemon.startOrReconfigureAuManagers(mau1, config2);
    Event[] exp2 = {
      new Event(mgr11, "setAuConfig", config2),
      new Event(mgr21, "setAuConfig", config2),
    };
    assertIsomorphic(exp2, TestAuMgr.getEvents());

    try {
      daemon.getAuManager("MgrKey1", mau2);
    } catch (IllegalArgumentException e) {
    }

    daemon.startOrReconfigureAuManagers(mau2, config2);
    TestAuMgr1 mgr12 = (TestAuMgr1)daemon.getAuManager("MgrKey1", mau2);
    TestAuMgr2 mgr22 = (TestAuMgr2)daemon.getAuManager("MgrKey2", mau2);
    assertSame(mau2, mgr12.getAu());
    assertSame(mau2, mgr22.getAu());
    assertEquals(SetUtil.set(mgr11, mgr12),
		 SetUtil.theSet(daemon.getAuManagersOfType("MgrKey1")));
    assertEquals(SetUtil.set(mgr21, mgr22),
		 SetUtil.theSet(daemon.getAuManagersOfType("MgrKey2")));
  }

  @Test
  public void testCrawlMode() throws Exception {
    assertTrue(LockssDaemon.CrawlMode.All.isCrawlPlugins());
    assertTrue(LockssDaemon.CrawlMode.All.isCrawlNonPlugins());
    assertFalse(LockssDaemon.CrawlMode.All.isCrawlNothing());

    assertFalse(LockssDaemon.CrawlMode.None.isCrawlPlugins());
    assertFalse(LockssDaemon.CrawlMode.None.isCrawlNonPlugins());
    assertTrue(LockssDaemon.CrawlMode.None.isCrawlNothing());

    assertTrue(LockssDaemon.CrawlMode.Plugins.isCrawlPlugins());
    assertFalse(LockssDaemon.CrawlMode.Plugins.isCrawlNonPlugins());
    assertFalse(LockssDaemon.CrawlMode.Plugins.isCrawlNothing());

    assertFalse(LockssDaemon.CrawlMode.NonPlugins.isCrawlPlugins());
    assertTrue(LockssDaemon.CrawlMode.NonPlugins.isCrawlNonPlugins());
    assertFalse(LockssDaemon.CrawlMode.NonPlugins.isCrawlNothing());
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
  
  @Test
  public void testMinJavaVersion() throws Exception {
    assertEquals(JavaVersion.JAVA_1_8, LockssDaemon.MIN_JAVA_VERSION);
  }
  
  List<Event> events;

  class TestAuLockssDaemon extends LockssDaemon {
    ManagerDesc[] testAuManagerDescs = {
      new ManagerDesc("MgrKey1", TestAuMgr1Factory.class.getName()),
      new ManagerDesc("MgrKey2", TestAuMgr2Factory.class.getName()),
    };

    TestAuLockssDaemon() {
      super(null);
    }

    protected ManagerDesc[] getAuManagerDescs() {
      return testAuManagerDescs;
    }
  }

  static class TestAuMgr implements LockssAuManager {
    static List<Event> events;
    static void clearEvents() {
      events = new ArrayList<Event>();
    }
    static List<Event> getEvents() {
      return events;
    }
    private ArchivalUnit au;
    TestAuMgr(ArchivalUnit au) {
      this.au = au;
    }
    ArchivalUnit getAu() {
      return au;
    }
    protected void setConfig(Configuration newConfig,
			     Configuration prevConfig,
			     Configuration.Differences changedKeys) {
    }
    public void initService(LockssApp app) {
      events.add(new Event(this, "initService", app));
    }

    @Override
    public void startService() {
      events.add(new Event(this, "startService"));
    }

    @Override
    public void serviceStarted() {
    }

    @Override
    public void stopService() {
      events.add(new Event(this, "stopService"));
    }

    public LockssApp getApp() {
      throw new UnsupportedOperationException("Not implemented");
    }

    public void setAuConfig(Configuration auConfig) {
      events.add(new Event(this, "setAuConfig", auConfig));
    }
  }
  static class TestAuMgr1 extends TestAuMgr {
    TestAuMgr1(ArchivalUnit au) {
      super(au);
    }
  }
  static class TestAuMgr1Factory implements LockssAuManager.Factory {
    public LockssAuManager createAuManager(ArchivalUnit au) {
      return new TestAuMgr1(au);
    }
  }
  static class TestAuMgr2 extends TestAuMgr {
    TestAuMgr2(ArchivalUnit au) {
      super(au);
    }
  }
  static class TestAuMgr2Factory implements LockssAuManager.Factory {
    public LockssAuManager createAuManager(ArchivalUnit au) {
      return new TestAuMgr2(au);
    }
  }

}
