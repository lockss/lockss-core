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

import java.io.*;
import java.util.*;
import static org.lockss.app.LockssApp.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.time.Deadline;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.daemon.status.*;
import org.lockss.util.test.FileTestUtil;
import org.lockss.plugin.*;
import static org.lockss.app.ManagerDescs.*;

/**
 * Functional tests for org.lockss.util.LockssApp
 */
public class FuncLockssApp extends LockssTestCase {
  private final static Logger log = Logger.getLogger();

  private String tempDirPath;
  private OneShotSemaphore startedSem = new OneShotSemaphore();
  Exception startThreadException = null;

  public void setUp() throws Exception {
    super.setUp();
    LockssApp.testingReinitialize();
    tempDirPath = setUpDiskSpace();
    org.lockss.truezip.TrueZipManager.setTempDir(getTempDir());

  }

  // Prevent MockLockssDaemon being created by LockssTestCase
  protected MockLockssDaemon newMockLockssDaemon() {
    return null;
  }

  public void testStartApp() throws Exception {
    String propurl =
      FileTestUtil.urlOfString("org.lockss.app.nonesuch=foo\n" +
			       "deftest1=file\n" +
			       "deftest2=file");

    final SimpleQueue appq = new SimpleQueue.Fifo();
    Thread th = new Thread() {
	public void run() {
	  appq.put(LockssApp.getLockssApp());
	}};
    th.start();
    assertTrue(appq.isEmpty());

    String[] testArgs = new String[] {"-p", propurl, "-g", "w"};

    LockssApp.AppSpec spec = new LockssApp.AppSpec()
      .setService(ServiceDescr.SVC_CONFIG)
      .setArgs(testArgs)
      .addAppConfig(StatusServiceImpl.PARAM_JMS_ENABLED, "false")
      .addAppConfig("o.l.p22", "vvv3")
      .addAppConfig("deftest1", "app")
      .addAppDefault("deftest2", "app")
      .addAppDefault("deftest3", "app3")
      .addBootDefault("o.l.plat.xxy", "zzz")
      .addAppConfig("org.lockss.app.serviceBindings",
		    "cfg=:24620:24621;mdx=:1234");
      ;

    assertTrue(appq.isEmpty());
    LockssApp app = LockssApp.startStatic(MyMockLockssApp.class, spec);
    assertSame(app, appq.get(TIMEOUT_SHOULDNT));

    assertTrue(app.isAppRunning());
    Configuration config = ConfigManager.getCurrentConfig();
    assertEquals("w", config.get(ConfigManager.PARAM_DAEMON_GROUPS));
    assertClass(org.lockss.daemon.RandomManager.class,
		app.getRandomManager());
    assertClass(org.lockss.alert.AlertManagerImpl.class,
		app.getManagerByType(org.lockss.alert.AlertManager.class));
    assertEquals("foo", config.get("org.lockss.app.nonesuch"));
    assertEquals("vvv3", config.get("o.l.p22"));
    assertEquals("app", config.get("deftest1"));
    assertEquals("file", config.get("deftest2"));
    assertEquals("app3", config.get("deftest3"));
    assertEquals(ServiceDescr.SVC_CONFIG, app.getMyServiceDescr());
    assertEquals("Config Service", app.getAppName());
    assertTrue(app.isMyService(ServiceDescr.SVC_CONFIG));
    assertFalse(app.isMyService(ServiceDescr.SVC_POLLER));

    assertEquals(new ServiceBinding(null, 24620, 24621),
		 app.getMyServiceBinding());

    assertEquals("zzz",
		 ConfigManager.getPlatformConfigOnly().get("o.l.plat.xxy"));
    assertEquals("zzz", config.get("o.l.plat.xxy"));
  }
  

  private final ManagerDesc aMgr =
    new ManagerDesc(MyLockssManager.class.getName());

  private final ManagerDesc[] expMgrs = {
    MISC_PARAMS_MANAGER_DESC,
    RANDOM_MANAGER_DESC,
    RESOURCE_MANAGER_DESC,
    JMS_MANAGER_DESC,
    MAIL_SERVICE_DESC,
    ALERT_MANAGER_DESC,
    STATUS_SERVICE_DESC,
    REST_SERVICES_MANAGER_DESC,
    TRUEZIP_MANAGER_DESC,
    URL_MANAGER_DESC,
    TIMER_SERVICE_DESC,
    // keystore manager must be started before any others that need to
    // access managed keystores
    KEYSTORE_MANAGER_DESC,
    BUILD_INFO_STATUS_DESC,
    aMgr,
    new ManagerDesc(CRON, "org.lockss.daemon.Cron"),
    new ManagerDesc(WATCHDOG_SERVICE, "org.lockss.daemon.WatchdogService"),
  };

  private final ManagerDesc[] mine = {
    new ManagerDesc(MyLockssManager.class.getName()),
    STATUS_SERVICE_DESC,
    ALERT_MANAGER_DESC,
  };

  public void testGetManagerDescs() throws Exception {
    LockssApp.AppSpec spec = new LockssApp.AppSpec()
      .setName("Test descs")
      .setAppManagers(new ManagerDesc[] {
	  new ManagerDesc(MyLockssManager.class.getName()),
	}) ;

    LockssApp app = new LockssApp(spec);
    assertEquals(ListUtil.list(expMgrs), ListUtil.list(app.getManagerDescs()));
  }


  public void testStartAppKeepRunning() throws Exception {
    String propurl =
      FileTestUtil.urlOfString("foo=bar");
    String[] testArgs = new String[] {"-p", propurl, "-g", "w bench"};

    LockssApp.AppSpec spec = new LockssApp.AppSpec()
      .setName("Test App KeepRunning")
      .setArgs(testArgs)
      .addAppConfig(StatusServiceImpl.PARAM_JMS_ENABLED, "false")
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

    assertTrue(startedSem.waitFull(Deadline.in(TIMEOUT_SHOULDNT * 10)));
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
    ConfigurationUtil.addFromArgs("org.lockss.app.exitImmediately", "true");
    assertTrue(sem.take(TIMEOUT_SHOULDNT));
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
    }
  }

  static class MyLockssManager extends BaseLockssManager {
  }

}
