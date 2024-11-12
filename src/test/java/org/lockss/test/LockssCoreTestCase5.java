package org.lockss.test;

import org.lockss.app.LockssDaemon;
import org.lockss.config.ConfigManager;
import org.lockss.config.CurrentConfig;
import org.lockss.daemon.*;
import org.lockss.util.StringUtil;
import org.lockss.util.test.LockssTestCase5;

import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.*;

public class LockssCoreTestCase5 extends LockssTestCase5 {
  private MockLockssDaemon mockDaemon = null;

  /** Create a fresh config manager.  This is overridden in
   * SpringLockssTestCase4 in lockss-spring-bundle */
  protected void makeConfigManager() {
    ConfigManager.makeConfigManager();
  }

  protected MockLockssDaemon newMockLockssDaemon() {
    return new MockLockssDaemon();
  }

  /**
   * Return the MockLockssDaemon instance for this testcase.  All test code
   * should use this method rather than creating a MockLockssDaemon.
   */
  public MockLockssDaemon getMockLockssDaemon() throws IOException {
    if (mockDaemon == null) {
      ensureTempTmpDir();
      makeConfigManager();
      mockDaemon = newMockLockssDaemon();
      LockssDaemon.setLockssDaemon(mockDaemon);
    }

    return mockDaemon;
  }

  /** Always install RandomManager */
  @BeforeEach
  public void beforeEachRandom(TestInfo info) throws IOException {
    MockLockssDaemon mockDaemon = getMockLockssDaemon();
    if (mockDaemon != null) {
      RandomManager rmgr = new LockssTestCase4.TestingRandomManager();
      rmgr.initService(mockDaemon);
      mockDaemon.setRandomManager(rmgr);
    }
  }

  public String setUpDiskSpace() throws IOException {
    String diskList =
        CurrentConfig.getParam(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST);
    if (!StringUtil.isNullString(diskList)) {
      return StringUtil.breakAt(diskList, ";").get(0);
    }
    String tmpdir = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
        tmpdir);
    return tmpdir;
  }
}
