/*

Copyright (c) 2000-2021, Board of Trustees of Leland Stanford Jr. University
All rights reserved.

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

package org.lockss.crawler;

import java.util.*;
import java.io.*;

import org.lockss.daemon.Crawler.CrawlerFacade;
import org.lockss.daemon.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;
import org.lockss.plugin.*;
import org.lockss.plugin.base.BaseUrlFetcher;
import org.lockss.test.*;
import org.lockss.state.*;
import org.lockss.util.urlconn.*;

import org.lockss.extractor.*;

/**
 * Tests for the new content crawler.
 */
public class TestFollowLinkCrawlerErrorPaths extends LockssTestCase {
  
  public static String LOGIN_ERROR_MSG = "Sample login page error";

  protected MockLockssDaemon theDaemon;
  protected CrawlManagerImpl crawlMgr;
  protected MyMockArchivalUnit mau = null;
  protected MockCachedUrlSet mcus = null;
  protected MockAuState aus;
  protected static List testUrlList = ListUtil.list("http://example.com");
  protected MockCrawlRule crawlRule = null;
  protected String startUrl = "http://www.example.com/index.html";
  protected String permissionPage = "http://www.example.com/permission.html";
  protected List startUrls;
  protected TestableFollowLinkCrawler crawler = null;
  protected MockLinkExtractor extractor = new MockLinkExtractor();
  protected CrawlerFacade cf;
  CrawlerStatus status;
  private MockPlugin plug;

  public void setUp() throws Exception {
    super.setUp();
    TimeBase.setSimulated(10);

    theDaemon = getMockLockssDaemon();
    crawlMgr = new NoPauseCrawlManagerImpl();
    theDaemon.setCrawlManager(crawlMgr);
    crawlMgr.initService(theDaemon);
    
    theDaemon.getAlertManager();
    
    plug = new MockPlugin(getMockLockssDaemon());
    plug.initPlugin(getMockLockssDaemon());
    mau = newMyMockArchivalUnit();
    mau.setPlugin(plug);
    mau.setAuId("MyMockTestAu");

    startUrls = ListUtil.list(startUrl);
    mcus = (MockCachedUrlSet)mau.getAuCachedUrlSet();

    crawlRule = new MockCrawlRule();
    crawlRule.addUrlToCrawl(startUrl);
    crawlRule.addUrlToCrawl(permissionPage);
    mau.addUrl(permissionPage);
    mau.setStartUrls(startUrls);
    mau.setPermissionUrls(ListUtil.list(permissionPage));
    mau.setCrawlRule(crawlRule);
    mau.setRefetchDepth(1);
    crawlMgr.newCrawlRateLimiter(mau);
    crawler = new TestableFollowLinkCrawler(mau, aus);
    crawler.setDaemonPermissionCheckers(ListUtil.list(new MockPermissionChecker(2)));
    status = crawler.getCrawlerStatus();
    mau.setLinkExtractor("*", extractor);
    Properties p = new Properties();
    p.setProperty(FollowLinkCrawler.PARAM_DEFAULT_RETRY_DELAY, "0");
    p.setProperty(FollowLinkCrawler.PARAM_MIN_RETRY_DELAY, "0");
    ConfigurationUtil.setCurrentConfigFromProps(p);
    cf = crawler.getCrawlerFacade();
  }
  
  public void testPermissionException() {
    crawler.setPermissionMap(new AlwaysPermissionMap(cf));
    mau.setUrlFetchers(ListUtil.list(new TestableUrlFetcher(cf, startUrl, new CacheException.PermissionException(LOGIN_ERROR_MSG))));
    assertFalse(crawler.fetch(new CrawlUrlData(startUrl, 0)));
    assertEquals(Crawler.STATUS_FETCH_ERROR, status.getCrawlStatus());
    assertEquals(true, crawler.isAborted());
    assertEquals(LOGIN_ERROR_MSG, status.getCrawlStatusMsg());
  }
  
  public void testPermission404(){
    CacheException.setDefaultSuppressStackTrace(false);
    String permissionUrl1 = "http://www.example.com/index.html";
    List permissionList = ListUtil.list(permissionUrl1);

    // Arrange for the UrlFetcher to throw RedirectOutsideCrawlSpecException
    CrawlerFacade cf = crawler.getCrawlerFacade();
    CacheException rocs = 
      new CacheException.NoRetryDeadLinkException("permission 404");
    TestableUrlFetcher uf =
      new TestableUrlFetcher(cf, permissionUrl1, rocs);
    mau.setUrlFetchers(ListUtil.list(uf));

    MockCachedUrlSet cus = (MockCachedUrlSet)mau.getAuCachedUrlSet();
    mau.setStartUrls(startUrls);
    mau.addUrl(permissionUrl1);
    mau.setPermissionUrls(permissionList);
    
    assertFalse(crawler.doCrawl());
    assertEquals(Crawler.STATUS_FETCH_ERROR, status.getCrawlStatus());
    assertEquals("Unable to fetch permission page", status.getCrawlStatusMsg());
    assertEmpty(cus.getCachedUrls());
  }

  public void testPermissionPageExcludedRedirect(){
    CacheException.setDefaultSuppressStackTrace(false);
    String permissionUrl1 = "http://www.example.com/index.html";
    List permissionList = ListUtil.list(permissionUrl1);

    // Arrange for the UrlFetcher to throw RedirectOutsideCrawlSpecException
    CrawlerFacade cf = crawler.getCrawlerFacade();
    CacheException rocs = 
      new CacheException.RedirectOutsideCrawlSpecException("ffff -> tttt");
    TestableUrlFetcher uf =
      new TestableUrlFetcher(cf, permissionUrl1, rocs);
    mau.setUrlFetchers(ListUtil.list(uf));

    MockCachedUrlSet cus = (MockCachedUrlSet)mau.getAuCachedUrlSet();
    mau.setStartUrls(startUrls);
    mau.addUrl(permissionUrl1);
    mau.setPermissionUrls(permissionList);
    
    assertFalse(crawler.doCrawl());
    assertEquals(Crawler.STATUS_FETCH_ERROR, status.getCrawlStatus());
    assertEquals("Unable to fetch permission page", status.getCrawlStatusMsg());
    assertEquals("ffff -> tttt", status.getErrorForUrl(permissionUrl1));
    assertEmpty(cus.getCachedUrls());
  }


  MyMockArchivalUnit newMyMockArchivalUnit() {
    StateManager smgr = theDaemon.getManagerByType(StateManager.class);
    MyMockArchivalUnit mau = new MyMockArchivalUnit();
    aus = new MockAuState(mau);
    smgr.storeAuState(aus);
    return mau;
  }

  private class AlwaysPermissionMap extends PermissionMap {
    public AlwaysPermissionMap(CrawlerFacade cf) {
      super(cf,null,null,null);
    }
    
    @Override
    public boolean hasPermission(String url) {
      return true;
    }
  }
  
  private class TestableFollowLinkCrawler extends FollowLinkCrawler {
    List<PermissionChecker> daemonPermissionCheckers;

    protected TestableFollowLinkCrawler(ArchivalUnit au, AuState aus){
      super(au, aus);
      crawlStatus = new CrawlerStatus(au,
            au.getStartUrls(),
            null);
    }
    
    @Override
    public boolean fetch(CrawlUrlData curl) {
      return super.fetch(curl);
    }
    
    public void setPermissionMap(PermissionMap pMap) {
      permissionMap = pMap;
    }
    
    List<PermissionChecker> getDaemonPermissionCheckers() {
      if(daemonPermissionCheckers != null) {
        return daemonPermissionCheckers;
      } else {
        return super.getDaemonPermissionCheckers();
      }
    }
    
    public void setDaemonPermissionCheckers(List<PermissionChecker> pc) {
      this.daemonPermissionCheckers = pc;
    }
      
   }
  
  protected class MyMockArchivalUnit extends MockArchivalUnit {
    List<UrlFetcher> fetcherList;
    RuntimeException getLinkExtractorThrows = null;
    
    public void setUrlFetchers(List<UrlFetcher> fetcherList) {
      this.fetcherList = fetcherList;
    }
    @Override
    public UrlFetcher makeUrlFetcher(CrawlerFacade mcf,
        String url) {
      if(fetcherList != null && !fetcherList.isEmpty()) {
        return fetcherList.remove(0);
      }
      return null;
    }

    public LinkExtractor getLinkExtractor(String mimeType) {
      if (getLinkExtractorThrows != null) {
        throw getLinkExtractorThrows;
      }
      return super.getLinkExtractor(mimeType);
    }
  }

  public class TestableUrlFetcher extends BaseUrlFetcher {
    IOException error;
    
    public TestableUrlFetcher(CrawlerFacade crawlFacade, String url, IOException error) {
      super(crawlFacade, url);
      this.error = error;
    }
    
    @Override
    protected InputStream getUncachedInputStreamOnly(String lastModified) throws IOException{
      if(error != null) {
	error.fillInStackTrace();
	throw error;
      }
      return null;
    }
    
    @Override
    public CIProperties getUncachedProperties() throws UnsupportedOperationException {
      return new CIProperties();
    }
    
  }

}

