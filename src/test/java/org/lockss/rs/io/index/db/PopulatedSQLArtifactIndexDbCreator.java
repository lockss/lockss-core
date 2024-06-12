package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.lockss.config.ConfigManager;
import org.lockss.db.DbException;
import org.lockss.rs.io.index.db.TestSQLArtifactIndexMetrics.ArtifactSpecGenerator;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.MockLockssDaemon;
import org.lockss.util.Logger;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PopulatedSQLArtifactIndexDbCreator {
  private static final Logger log = Logger.getLogger();

  public static void main(String[] args) throws Exception {
    String tempDirPath =
        new File(PlatformUtil.getSystemTempDir(), "pgsqlindexdb").toString();

    String arg;
    for (int i = 0; i < args.length; i++) {
      arg = args[i];

      if (i < args.length - 1 && "-d".equals(arg)) {
        tempDirPath = args[++i];
      }
    }

    int dbPort = startEmbeddedPostgreSQL(tempDirPath);

    ConfigManager.makeConfigManager();
    Logger.resetLogs();

    // Set the database log.
    System.setProperty("derby.stream.error.file",
        new File(tempDirPath, "derby.log").getAbsolutePath());

    ConfigurationUtil.setFromArgs(
        ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgresx");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.enabled", "true",
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.initialSize", "2");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, PGSimpleDataSource.class.getCanonicalName(),
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PORTNUMBER, String.valueOf(dbPort));

    MockLockssDaemon daemon = new MockLockssDaemon() {
    };
    SQLArtifactIndexDbManager idxDbManager = new SQLArtifactIndexDbManager();
    daemon.setSQLArtifactIndexDbManager(idxDbManager);
    idxDbManager.initService(daemon);

    idxDbManager.setTargetDatabaseVersion(3);
    idxDbManager.startService();

    populateDatabase(idxDbManager);

    idxDbManager.waitForThreadsToFinish(500);
    System.exit(0);
  }

  private static void populateDatabase(SQLArtifactIndexDbManager idxDbManager) throws DbException {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    int ns = 1;
    int auids = 1000;
    int urls = 100000;

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(ns, auids, urls);

    long count = 0;
    long start = TimeBase.nowMs();

    List<Artifact> artifacts = new ArrayList<>();
    for (ArtifactSpec spec : specs) {
      artifacts.add(spec.getArtifact());
      count++;
      if (count % 1000 == 999) {
        float progess = (float) (count + 1) / (ns * auids * urls) * 100;
        log.info("Adding artifacts (%.2f%%)".formatted(progess));
        idxdb.addArtifacts(artifacts);
        artifacts.clear();
      }
    }
    idxdb.addArtifacts(artifacts);
    artifacts.clear();

    long end = TimeBase.nowMs();

    float artifactsPerSecond = (float) count / ((float) (end - start) / 1000);

    log.info("addArtifacts(): " + count + " artifacts in " + (end - start) + " ms; " + artifactsPerSecond + " calls/sec");
  }

  private static int startEmbeddedPostgreSQL(String tmpDir) throws DbException {
    log.info("tmpDir = " + tmpDir);
    try {
      EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();

      File baseDir = new File(tmpDir);
      builder.setOverrideWorkingDirectory(baseDir);

      File dataDir = new File(baseDir, "db");
      builder.setDataDirectory(dataDir);
      builder.setCleanDataDirectory(false);

      EmbeddedPostgres embeddedPg = builder.start();
      return embeddedPg.getPort();
    } catch (IOException e) {
      throw new DbException("Can't start embedded PostgreSQL", e);
    }
  }
}