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

package org.lockss.test;

import java.util.*;
import org.lockss.app.*;
import org.lockss.state.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.crawler.*;

/**
 * Mock implementation of the CrawlManager
 */
public class MockCrawlManager implements CrawlManager, LockssManager {
  public HashMap scheduledRepairs = new HashMap();
  public HashMap scheduledCrawls = new HashMap();
  public static final String SCHEDULED = "scheduled";
  public boolean shouldCrawlNewContent = true;
  private CrawlRateLimiter crl;

  public void initService(LockssApp app) throws LockssAppException { }
  @Override
  public void startService() { }
  @Override
  public void serviceStarted() {}

  @Override
  public void stopService() {
    scheduledRepairs = new HashMap();
    scheduledCrawls = new HashMap();
  }

  public int getAuPriority(ArchivalUnit au) {
    return 0;
  }

  public LockssApp getApp() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public CrawlerStatus startRepair(ArchivalUnit au, Collection urls,
			   Object cookie) {
    Iterator urlIt = urls.iterator();
    while (urlIt.hasNext()) {
      scheduledRepairs.put(urlIt.next(), SCHEDULED);
    }
    return null;
  }

  /**
   * Currently returns false if last crawl time > 0, schedules new content
   * crawl and returns true otherwise.
   * @param au the ArchivalUnit
   * @param cookie the cookie
   * @return true if a crawl is running on this AU
   */
  public boolean isCrawlingAu(ArchivalUnit au,
			       Object cookie) {
    if (shouldCrawlNewContent) {
      scheduleNewContentCrawl(au, cookie);
      return true;
    }
    return false;

  }

  public CrawlerStatus startNewContentCrawl(ArchivalUnit au,
					    
					    Object cookie) {
    scheduleNewContentCrawl(au, cookie);
    return new CrawlerStatus(au, au.getStartUrls(), "mock");
  }

  public CrawlerStatus startNewContentCrawl(ArchivalUnit au, int priority,
					    
	                                    Object cookie) {
    scheduleNewContentCrawl(au, cookie);
    return new CrawlerStatus(au, au.getStartUrls(), "mock");
  }

  public CrawlerStatus startNewContentCrawl(CrawlReq req) {
    return new CrawlerStatus(req.getAu(), req.getAu().getStartUrls(), "mock");
  }

  public CrawlRateLimiter getCrawlRateLimiter(Crawler crawler) {
    if (crl != null) {
      return crl;
    }
    return CrawlRateLimiter.Util.forAu(crawler.getAu());
  }

  public void setCrawlRateLimiter(CrawlRateLimiter crl) {
    this.crl = crl;
  }

  public boolean isCrawlStarterEnabled() {
    return CurrentConfig.getBooleanParam(CrawlManagerImpl.PARAM_CRAWL_STARTER_ENABLED,
					 CrawlManagerImpl.DEFAULT_CRAWL_STARTER_ENABLED);
  }

  public boolean isCrawlerEnabled() {
    return CurrentConfig.getBooleanParam(CrawlManagerImpl.PARAM_CRAWLER_ENABLED,
					 CrawlManagerImpl.DEFAULT_CRAWLER_ENABLED);
  }

  public boolean isCrawlStarterRunning() {
    return false;
  }

  public boolean isGloballyPermittedHost(String hoat) {
    return false;
  }

  public boolean isAllowedPluginPermittedHost(String hoat) {
    return false;
  }

  public void setShouldCrawlNewContent(boolean shouldCrawlNewContent) {
    this.shouldCrawlNewContent = shouldCrawlNewContent;
  }

  public String getUrlStatus(String url) {
    return (String)scheduledRepairs.get(url);
  }

  public String getAuStatus(ArchivalUnit au) {
    return (String)scheduledCrawls.get(au);
  }

  public Iterator getScheduledUrlRepairs() {
    return scheduledRepairs.keySet().iterator();
  }

  public Iterator getScheduledAuCrawls() {
    return scheduledCrawls.keySet().iterator();
  }

  private void scheduleNewContentCrawl(ArchivalUnit au,
				       
				       Object cookie) {
    scheduledCrawls.put(au, SCHEDULED);
  } 

  public StatusSource getStatusSource() {
    throw new UnsupportedOperationException("Not Implemented");
  }
}
