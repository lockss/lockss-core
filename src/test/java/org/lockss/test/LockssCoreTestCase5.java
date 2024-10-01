package org.lockss.test;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.lockss.app.LockssDaemon;
import org.lockss.config.ConfigManager;
import org.lockss.config.CurrentConfig;
import org.lockss.daemon.RandomManager;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.util.StringUtil;
import org.lockss.util.test.LockssTestCase5;

import java.io.File;
import java.io.IOException;

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

  EmbeddedPostgres embeddedPg;

  // If this cuases "Permission denied" trying to start PostgreSQL,
  // ensure tha the tmp directory in use isn't mounted noexec, or set
  // the maven property dir.executableTemp, or the System property
  // org.lockss.executableTempDir to one from which programs can be
  // execed.
  protected void startEmbeddedPgDbManager(DbManager mgr) throws DbException {
    try {
      if (embeddedPg == null) {
        EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();
        String extemp = System.getProperty("org.lockss.executableTempDir");
        if (!StringUtil.isNullString(extemp)) {
          builder.setOverrideWorkingDirectory(new File(extemp));
        }
        embeddedPg = builder.start();
      }
      String dbName = mgr.getDatabaseNamePrefix()
          + mgr.getClass().getSimpleName();
      mgr.setTestingDataSource(embeddedPg.getDatabase("postgres", dbName));
    } catch (IOException e) {
      throw new DbException("Can't start embedded PostgreSQL", e);
    }
  }
}
