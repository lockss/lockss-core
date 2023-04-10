/*
 Copyright (c) 2000-2020 Board of Trustees of Leland Stanford Jr. University,
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
import org.apache.commons.lang3.StringUtils;

import org.junit.Test;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.test.FileTestUtil;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.TimerUtil;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.rs.LocalLockssRepository;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactVersions;

public class TestRepositoryManager extends LockssTestCase4 {
  private static L4JLogger log = L4JLogger.getLogger();

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

  void assertArt(Artifact art, String expUrl, int expVer, String expContent)
      throws IOException {
    assertEquals(expUrl, art.getUri());
    assertEquals(expVer, (long)art.getVersion());
    if (expContent != null) {
      LockssRepository repo = mgr.getV2Repository().getRepository();
      ArtifactData ad = repo.getArtifactData(art);
      assertEquals(expContent, StringUtil.fromInputStream(ad.getInputStream()));
    }
  }

  @Test
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

  @Test
  public void testGetRepositoryUrlList() throws Exception {
    assertEmpty(mgr.getRepositoryUrlList());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile:ns42");
    assertEquals(ListUtil.list("volatile:ns42"), mgr.getRepositoryUrlList());
  }

  @Test
  public void testGetRepositoryList() throws Exception {
    assertEmpty(mgr.getRepositoryUrlList());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile:ns42");
    assertEquals(ListUtil.list(mgr.getRepoRepo("volatile:ns42")),
                 mgr.getRepositoryList());
  }

  @Test
  public void testGetRepositoryDF () throws Exception {
    String tmpdir = getTempDir().toString();
    // Ensure at least 1K in tmpdir so df.used > 0
    FileTestUtil.writeTempFile("pad", StringUtils.repeat("0123456789", 103));
    assertNull(mgr.getV2Repository());
    String spec = "local:ns_1:" + tmpdir;
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY, spec);
    assertNotNull(mgr.getV2Repository());
    PlatformUtil.DF df = mgr.getRepositoryDF(spec);
    assertNotNull(df);
    Map<String,PlatformUtil.DF> repoMap = mgr.getRepositoryDFMap();
    PlatformUtil.DF mapDf = repoMap.get(spec);
    assertTrue(equalsDF(df, mapDf));
    assertTrue(""+df.getSize(), df.getSize() > 0);
    // Can't assume tmpdir has any space used as it may not be the same
    // filesyste that local: uses
    assertTrue(""+df.getUsed(), df.getUsed() >= 0);
  }

  boolean equalsDF(PlatformUtil.DF df1, PlatformUtil.DF df2) {
    return Objects.equals(df1.getPath(), df2.getPath())
      && Objects.equals(df1.getFs(), df2.getFs())
      && Objects.equals(df1.getMnt(), df2.getMnt())
      && df1.getSize() == df2.getSize()
//       && df1.getUsed() == df2.getUsed()
//       && df1.getAvail() == df2.getAvail()
//       && df1.getPercent() == df2.getPercent()
      ;
  }


  @Test
  public void testFindLeastFullRepository () throws Exception {
    Map repoMap = MapUtil.map("local:one", new MyDF("/one", 1000),
			      "local:two",  new MyDF("/two", 3000),
			      "local:three",  new MyDF("/three", 2000));
    mgr.setRepoMap(repoMap);

    assertEquals("local:two", mgr.findLeastFullRepository());
  }

  @Test
  public void testGetV2RepositoryIll () throws Exception {
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile");
    assertNull(mgr.getV2Repository());

    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "local:ns");
    assertNull(mgr.getV2Repository());

    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "rest:ns:illscheme:url");
    assertNull(mgr.getV2Repository());
  }

  @Test
  public void testGetV2RepositoryVolatile () throws Exception {
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "volatile:ns_1");
    assertNotNull(mgr.getV2Repository());
    assertEquals("ns_1", mgr.getV2Repository().getNamespace());
  }

  @Test
  public void testGetV2RepositoryLocal () throws Exception {
    String tmpdir = getTempDir().toString();
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "local:ns_1:" + tmpdir);
    assertNotNull(mgr.getV2Repository());
    assertEquals("ns_1", mgr.getV2Repository().getNamespace());
    LockssRepository repo = mgr.getV2Repository().getRepository();
    assertClass(LocalLockssRepository.class, repo);
  }

  @Test
  public void testGetV2RepositoryRest () throws Exception {
    assertNull(mgr.getV2Repository());
    ConfigurationUtil.addFromArgs(RepositoryManager.PARAM_V2_REPOSITORY,
				  "rest:ns_1:http://foo.bar/endpoint");
    assertNotNull(mgr.getV2Repository());
    assertEquals("ns_1", mgr.getV2Repository().getNamespace());
    LockssRepository repo = mgr.getV2Repository().getRepository();
    assertClass(RestLockssRepository.class, repo);
  }

  @Test
  public void testFindArtifactsByUrl() throws Exception {
    ConfigurationUtil.addFromArgs(org.lockss.repository.RepositoryManager.PARAM_V2_REPOSITORY,
                                  "volatile:foo");
    String url1 = "http://www.example.com/testDir/foo";
    String url2 = "http://www.example.com/testDir/bar";
    String url3 = "http://www.example.com/testDir/bar/1";

    LockssRepository repo = mgr.getV2Repository().getRepository();
    Artifact art;
    storeArt(repo, url1, "111", null);
    storeArt(repo, url2, "222", null);
    storeArt(repo, url3, "333", null);
    TimerUtil.guaranteedSleep(1000);
    List<Artifact> arts1 = mgr.findArtifactsByUrl(url1);
    assertEquals(1, arts1.size());
    assertArt(arts1.get(0), url1, 1, "111");
    List<Artifact> arts2 = mgr.findArtifactsByUrl(url2);
    assertEquals(1, arts2.size());
    assertArt(arts2.get(0), url2, 1, "222");

    storeArt(repo, url1, "xxxx", null);
    storeArt(repo, url1, "yyyyyyy", null);

    List<Artifact> arts3 = mgr.findArtifactsByUrl(url1);
    assertEquals(1, arts3.size());
    assertArt(arts3.get(0), url1, 3, "yyyyyyy");

    List<Artifact> arts4 = mgr.findArtifactsByUrl(url1, ArtifactVersions.ALL);
    assertEquals(3, arts4.size());
    assertArt(arts4.get(0), url1, 3, "yyyyyyy");
    assertArt(arts4.get(1), url1, 2, "xxxx");
    assertArt(arts4.get(2), url1, 1, "111");

    assertEmpty(mgr.findArtifactsByUrl(url1 + "/notpresent"));
  }

  @Test
  public void testFindArtifactsByUrlAllVersions() throws Exception {
    ConfigurationUtil.addFromArgs(org.lockss.repository.RepositoryManager.PARAM_V2_REPOSITORY,
                                  "volatile:foo");
    String url1 = "http://www.example.com/testDir/foo";
    String url2 = "http://www.example.com/testDir/bar";

    LockssRepository repo = mgr.getV2Repository().getRepository();
    Artifact art;
    storeArt(repo, url1, "111", null);
    storeArt(repo, url2, "222", null);
    TimerUtil.guaranteedSleep(1000);
    List<Artifact> arts1 = mgr.findArtifactsByUrl(url1,
                                                  ArtifactVersions.ALL);
    assertEquals(1, arts1.size());
    art = arts1.get(0);
    assertEquals(url1, art.getUri());
    assertEquals(1, (long)art.getVersion());
    List<Artifact> arts2 = mgr.findArtifactsByUrl(url2,
                                                  ArtifactVersions.ALL);
    art = arts2.get(0);
    assertEquals(1, arts2.size());
    assertEquals(url2, art.getUri());
    assertEquals(1, (long)art.getVersion());

    storeArt(repo, url1, "xxxx", null);
    storeArt(repo, url1, "yyyyyyy", null);
    List<Artifact> arts3 = mgr.findArtifactsByUrl(url1,
                                                  ArtifactVersions.ALL);
    assertEquals(3, arts3.size());
    for (int ix = 1; ix <= 3; ix++) {
      art = arts3.get(ix - 1);
      assertEquals(url1, art.getUri());
      assertEquals(4 - ix, (long)art.getVersion());
    }
    assertEmpty(mgr.findArtifactsByUrl(url1 + "/notpresent",
                                       ArtifactVersions.ALL));
  }

  protected Artifact storeArt(LockssRepository repo, String url, String content,
			      CIProperties props) throws Exception {
    return storeArt(repo, url, new StringInputStream(content), props);
  }

  protected Artifact storeArt(LockssRepository repo, String url, InputStream in,
			      CIProperties props) throws Exception {
    if (props == null) props = new CIProperties();
    return V2RepoUtil.storeArt(repo, "foo", "auidauid42", url, in, props);
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
    public List<String> getRepositoryUrlList() {
      if (repos != null) return repos;
      return super.getRepositoryUrlList();
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
