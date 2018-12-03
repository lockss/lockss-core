/*

Copyright (c) 2000, Board of Trustees of Leland Stanford Jr. University.
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.test;

import java.io.File;
import java.security.MessageDigest;

import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.util.*;
import org.lockss.plugin.*;
import org.lockss.plugin.simulated.*;
import org.lockss.crawler.FollowLinkCrawler;
import org.lockss.protocol.*;

import java.util.Properties;

public class HashSpeedTest extends LockssTestCase {
  private SimulatedArchivalUnit sau;
  private MockLockssDaemon theDaemon;
  private static final int DEFAULT_DURATION = 1000;
  private static final int DEFAULT_BYTESTEP = 1024;
  private static final int DEFAULT_FILESIZE = 3000;
  private static int duration = DEFAULT_DURATION;
  private static int byteStep = DEFAULT_BYTESTEP;
  private static int fileSize = DEFAULT_FILESIZE;

  public static void main(String[] args) throws Exception {
    HashSpeedTest test = new HashSpeedTest();
    if (args.length>0) {
      try {
        duration = Integer.parseInt(args[0]);
        if (args.length>1) {
          byteStep = Integer.parseInt(args[1]);
          if (args.length>2) {
            fileSize = Integer.parseInt(args[2]);
          }
        }
      } catch (NumberFormatException ex) { }
    }
    test.setUp(duration, byteStep);
    test.testRunSelf();
    test.tearDown();
  }

  public void setUp() throws Exception {
    super.setUp();
    theDaemon = getMockLockssDaemon();
    this.setUp(DEFAULT_DURATION, DEFAULT_BYTESTEP);
  }

  public void setUp(int duration, int byteStep) throws Exception {
    String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    Properties props = new Properties();
    props.setProperty(SystemMetrics.PARAM_HASH_TEST_DURATION, ""+duration);
    props.setProperty(SystemMetrics.PARAM_HASH_TEST_BYTE_STEP, ""+byteStep);
    props.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
		      tempDirPath);
    ConfigurationUtil.setCurrentConfigFromProps(props);
    useOldRepo();

    theDaemon.getPluginManager();
    theDaemon.getSystemMetrics();
    theDaemon.getHashService();
    theDaemon.setDaemonInited(true);
    theDaemon.getPluginManager().startService();
    theDaemon.getSystemMetrics().startService();
    theDaemon.getHashService().startService();
    theDaemon.getPluginManager().startLoadablePlugins();

    sau = PluginTestUtil.createAndStartSimAu(simAuConfig(tempDirPath));
  }

  public void tearDown() throws Exception {
    theDaemon.stopDaemon();
    super.tearDown();
  }

  Configuration simAuConfig(String rootPath) {
    Configuration conf = ConfigManager.newConfiguration();
    conf.put("root", rootPath);
    conf.put("depth", "3");
    conf.put("branch", "5");
    conf.put("numFiles", "5");
    conf.put("fileTypes", "" + SimulatedContentGenerator.FILE_TYPE_BIN);
    conf.put("binFileSize", ""+fileSize);
    return conf;
  }

  public void testRunSelf() throws Exception {
    createContent();
    crawlContent();
    hashContent();
  }

  private void createContent() {
    System.out.println("Generating tree of size 3x5x5 with "+fileSize
                       +"byte files...");
    sau.generateContentTree();
  }

  private void crawlContent() {
    System.out.println("Crawling tree...");
    Crawler crawler = new FollowLinkCrawler(sau, AuUtil.getAuState(sau));
    crawler.doCrawl();
  }

  private void hashContent() throws Exception {
    MessageDigest digest = V3LcapMessage.getDefaultMessageDigest();
    System.out.println("Hashing-");
    System.out.println("  Algorithm: "+digest.getAlgorithm());
    System.out.println("  Duration: "+duration+"ms");
    System.out.println("  Byte/step: "+byteStep+"bytes");
    CachedUrlSetHasher hasher = sau.getAuCachedUrlSet().getContentHasher(digest);

    SystemMetrics metrics = theDaemon.getSystemMetrics();
    double estimate = metrics.measureHashSpeed(hasher, digest);
    System.out.println("Estimate-");
    System.out.println("  Bytes/ms: "+estimate);
    System.out.println("  GB/hr: "+
                       ((estimate*Constants.HOUR)/(1024*1024*1024)));
  }
}

