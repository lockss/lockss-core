/*
 * $Id$
 */

/*
 Copyright (c) 2000-2012 Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.repository;

import java.io.*;
import java.util.*;

import org.lockss.app.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.TimerUtil;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.laaws.rs.core.*;

public class TestRepositoryManager extends LockssTestCase {
  private MockArchivalUnit mau;
  private MyRepositoryManager mgr;

  private MockLockssDaemon theDaemon;

  public void setUp() throws Exception {
    super.setUp();
    theDaemon = getMockLockssDaemon();
    mgr = new MyRepositoryManager();
    theDaemon.setRepositoryManager(mgr);
    mgr.initService(theDaemon);
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void testConfig() throws Exception {
    PlatformUtil.DF warn = mgr.getDiskWarnThreshold();
    PlatformUtil.DF full = mgr.getDiskFullThreshold();
    assertEquals(5000 * 1024, warn.getAvail());
    assertEquals(0.98, warn.getPercent(), .00001);
    assertEquals(100 * 1024, full.getAvail());
    assertEquals(0.99, full.getPercent(), .00001);

    Properties p = new Properties();
    p.put(RepositoryManager.PARAM_DISK_WARN_FRRE_MB, "17");
    p.put(RepositoryManager.PARAM_DISK_WARN_FRRE_PERCENT, "20");
    p.put(RepositoryManager.PARAM_DISK_FULL_FRRE_MB, "7");
    p.put(RepositoryManager.PARAM_DISK_FULL_FRRE_PERCENT, "10");
    ConfigurationUtil.setCurrentConfigFromProps(p);
    warn = mgr.getDiskWarnThreshold();
    full = mgr.getDiskFullThreshold();
    assertEquals(17 * 1024, warn.getAvail());
    assertEquals(0.80, warn.getPercent(), .00001);
    assertEquals(7 * 1024, full.getAvail());
    assertEquals(0.90, full.getPercent(), .00001);
  }

  public void testGetRepositoryList() throws Exception {
    assertEmpty(mgr.getRepositoryList());
    String tempDirPath = setUpDiskSpace();
    assertEquals(ListUtil.list("local:" + tempDirPath),
		 mgr.getRepositoryList());
    String tempdir2 = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.setFromArgs("org.lockss.platform.diskSpacePaths",
				  tempdir2 + ";" + tempDirPath);
    assertEquals(ListUtil.list("local:" + tempdir2, "local:" + tempDirPath),
		 mgr.getRepositoryList());
  }

  public void testGetRepositoryDF () throws Exception {
    PlatformUtil.DF df = mgr.getRepositoryDF("local:.");
    assertNotNull(df);
  }

  public void testFindLeastFullRepository () throws Exception {
    Map repoMap = MapUtil.map("local:one", new MyDF("/one", 1000),
			      "local:two",  new MyDF("/two", 3000),
			      "local:three",  new MyDF("/three", 2000));
    mgr.setRepoMap(repoMap);

    assertEquals("local:two", mgr.findLeastFullRepository());
  }

  public void testGetV2RepositoryIll () throws Exception {
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile");
    assertNull(mgr.getV2Repository());

    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "local:coll");
    assertNull(mgr.getV2Repository());

    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "rest:coll:illscheme:url");
    assertNull(mgr.getV2Repository());
  }

  public void testGetV2RepositoryVolatile () throws Exception {
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile:coll_1");
    assertNotNull(mgr.getV2Repository());
    assertEquals("coll_1", mgr.getV2Repository().getCollection());
  }

  public void testGetV2RepositoryLocal () throws Exception {
    String tmpdir = getTempDir().toString();
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "local:coll_1:" + tmpdir);
    assertNotNull(mgr.getV2Repository());
    assertEquals("coll_1", mgr.getV2Repository().getCollection());
    LockssRepository repo = mgr.getV2Repository().getRepository();
    assertClass(LocalLockssRepository.class, repo);
  }

  public void testGetV2RepositoryRest () throws Exception {
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "rest:coll_1:http://foo.bar/endpoint");
    assertNotNull(mgr.getV2Repository());
    assertEquals("coll_1", mgr.getV2Repository().getCollection());
    LockssRepository repo = mgr.getV2Repository().getRepository();
    assertClass(RestLockssRepository.class, repo);
  }

  class MyRepositoryManager extends RepositoryManager {
    List nodes = new ArrayList();
    SimpleBinarySemaphore sem;
    List repos;
    Map repoMap;

    void setSem(SimpleBinarySemaphore sem) {
      this.sem = sem;
    }
    List getNodes() {
      return nodes;
    }
    public List<String> getRepositoryList() {
      if (repos != null) return repos;
      return super.getRepositoryList();
    }
    public void setRepos(List repos) {
      this.repos = repos;
    }
    public PlatformUtil.DF getRepositoryDF(String repoName) {
      if (repoMap != null) return (PlatformUtil.DF)repoMap.get(repoName);
      return super.getRepositoryDF(repoName);
    }
    public void setRepoMap(Map<String,PlatformUtil.DF> repoMap) {
      List repos = new ArrayList();
      this.repoMap = repoMap;
      for (String repo : repoMap.keySet()) {
	repos.add(repo);
      }
      setRepos(repos);
    }
  }

  class MyDF extends PlatformUtil.DF {
    MyDF(String path, int avail) {
      super();
      this.path = path;
      this.avail = avail;
    }
  }
}
