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
import static org.lockss.app.LockssApp.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.*;

/**
 * Functional tests for org.lockss.util.LockssApp
 */
public class FuncLockssApp extends LockssTestCase {
  private final static Logger log = Logger.getLogger(FuncLockssApp.class);

  private String tempDirPath;
  private OneShotSemaphore startedSem = new OneShotSemaphore();
  Exception startThreadException = null;

  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = setUpDiskSpace();
  }

  // Prevent MockLockssDaemon being created by LockssTestCase
  protected MockLockssDaemon newMockLockssDaemon() {
    return null;
  }

  public void testStartApp() throws Exception {
    String propurl =
      FileTestUtil.urlOfString("org.lockss.app.nonesuch=foo");
    String[] testArgs = new String[] {"-p", propurl, "-g", "w"};

    LockssApp.AppSpec spec = new LockssApp.AppSpec()
      .setName("Test App")
      .setArgs(testArgs)
      .addAppConfig("o.l.p22", "vvv3")
      .addAppConfig("o.l.p333", "vvv4")
//       .setKeepRunning(true)
//       .setAppManagers(managerDescs)
      ;
    LockssApp app = LockssApp.startStatic(MyMockLockssApp.class, spec);

    assertTrue(app.isAppRunning());
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("w", config.get(ConfigManager.PARAM_DAEMON_GROUPS));
    assertClass(org.lockss.daemon.RandomManager.class,
		app.getRandomManager());
    assertClass(org.lockss.alert.AlertManagerImpl.class,
		app.getManagerByType(org.lockss.alert.AlertManager.class));
    assertEquals("foo", config.get("org.lockss.app.nonesuch"));
    assertEquals("vvv3", config.get("o.l.p22"));
  }
  
  public void testStartAppKeepRunning() throws Exception {
    String propurl =
      FileTestUtil.urlOfString("foo=bar");
    String[] testArgs = new String[] {"-p", propurl, "-g", "w bench"};

    LockssApp.AppSpec spec = new LockssApp.AppSpec()
      .setName("Test App KeepRunning")
      .setArgs(testArgs)
      .addAppConfig("o.l.p22", "vvv3")
      .addAppConfig("o.l.p333", "vvv4")
      .setKeepRunning(true)
      .setStartedSem(startedSem)
      .setAppManagers(new ManagerDesc[] {
	  new ManagerDesc(MyLockssManager.class.getName()),
	}) ;
    final SimpleBinarySemaphore sem = new SimpleBinarySemaphore();

    Thread th = new Thread() {
	public void run() {
	  try {
	    LockssApp.startStatic(MyMockLockssApp.class, spec);
	  } catch (Exception e) {
	    startThreadException = e;
	  }
	  sem.give();
	}};
    th.start();

    assertTrue(startedSem.waitFull(Deadline.in(TIMEOUT_SHOULDNT)));
    LockssApp app = LockssApp.getLockssApp();
    app.waitUntilAppRunning();
    assertTrue(app.isAppRunning());
    assertEquals("w bench",
		 CurrentConfig.getParam(ConfigManager.PARAM_DAEMON_GROUPS));
    assertClass(org.lockss.daemon.RandomManager.class,
		app.getRandomManager());
    assertClass(org.lockss.alert.AlertManagerImpl.class,
		app.getManagerByType(org.lockss.alert.AlertManager.class));
    assertClass(MyLockssManager.class,
		app.getManagerByType(MyLockssManager.class));
    assertFalse(sem.take(0));
    ConfigurationUtil.addFromArgs("org.lockss.app.exitOnce", "true");
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
    assertClass(RuntimeException.class, startThreadException);
    assertEquals("System.exit(0)", startThreadException.getMessage());
  }
  

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

    void setDescrs(ManagerDesc[] descrs) {
      this.descrs = descrs;
    }

    protected void systemExit(int val) {
      log.critical("System.exit(" + val + ")");
      throw new RuntimeException("System.exit(" + val + ")");
    }
  }

  static class MyLockssManager extends BaseLockssManager {
  }

}
