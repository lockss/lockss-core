/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.rs.io.index.db;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.lockss.config.ConfigManager;
import org.lockss.db.DbException;
import org.lockss.rs.io.index.db.SQLArtifactIndexMetrics.ArtifactSpecGenerator;
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

    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    populateDatabase(idxDbManager);

    idxDbManager.waitForThreadsToFinish(500);
    System.exit(0);
  }

  private static void populateDatabase(SQLArtifactIndexDbManager idxDbManager) throws DbException {
    SQLArtifactIndexManagerSql idxdb = new SQLArtifactIndexManagerSql(idxDbManager);

    int ns = 10;
    int auids = 100;
    int urls = 100;

    int total = ns * auids * urls;
    int onePercent = total / 100;

    ArtifactSpecGenerator specs =
        new ArtifactSpecGenerator(ns, auids, urls);

    long count = 0;
    long start = TimeBase.nowMs();

    List<Artifact> artifacts = new ArrayList<>();
    for (ArtifactSpec spec : specs) {
      artifacts.add(spec.getArtifact());
      count++;
      if (count % onePercent == onePercent - 1) {
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