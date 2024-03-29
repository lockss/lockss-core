/*

Copyright (c) 2000-2020, Board of Trustees of Leland Stanford Jr. University.
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

package org.lockss.crawler;

import java.util.*;
import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.lockss.util.*;
import org.lockss.util.lang.LockssRandom;
import org.lockss.util.os.PlatformUtil;
import org.lockss.util.time.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;
import org.lockss.test.*;
import org.lockss.state.*;
import org.lockss.plugin.*;
import org.lockss.daemon.*;
import org.lockss.config.*;
import org.lockss.alert.*;

import static java.util.UUID.randomUUID;
import static org.lockss.crawler.CrawlEvent.KEY_COOKIE;

/**
 * Test class for CrawlManagerImpl.
 */
public class TestCrawlManagerImpl extends LockssTestCase {
  public static final String GENERIC_URL = "http://www.example.com/index.html";
  protected TestableCrawlManagerImpl crawlManager = null;
  protected MockArchivalUnit mau = null;
  protected MockLockssDaemon theDaemon;
  protected MockCrawler crawler;
  protected CachedUrlSet cus;
  protected CrawlRule rule;
  protected CrawlManager.StatusSource statusSource;
  protected MyPluginManager pluginMgr;
  //protected MockAuState maus;
  protected Plugin plugin;
  protected Properties cprops = new Properties();
  protected List semsToGive;
  protected Map<String,TestCrawlCB>  callbacks;
  static CrawlEventHandler testHandler;

  public void setUp() throws Exception {
    super.setUp();
    semsToGive = new ArrayList();
    // default enable is now false for laaws.
    ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWLER_ENABLED, "true");
    ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_ENABLE_JMS_RECEIVE, "false");
    ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_ENABLE_JMS_SEND, "false");

    // some tests start the service, but most don't want the crawl starter
    // to run.
    cprops.put(CrawlManagerImpl.PARAM_START_CRAWLS_INTERVAL, "0");

    plugin = new MockPlugin(getMockLockssDaemon());
    rule = new MockCrawlRule();

    theDaemon = getMockLockssDaemon();
    pluginMgr = new MyPluginManager();
    pluginMgr.initService(theDaemon);
    theDaemon.setPluginManager(pluginMgr);
    crawlManager = new TestableCrawlManagerImpl(pluginMgr);
    theDaemon.setCrawlManager(crawlManager);
    statusSource = (CrawlManager.StatusSource)crawlManager;
    crawlManager.initService(theDaemon);
    callbacks =new HashMap<>();
    testHandler = new CrawlEventHandler.Base() {
      @Override
      protected void handleNewContentCompleted(CrawlEvent event) {
        log.debug2(" new content completed was called");
        Map<String, Object> extraData = event.getExtraData();
        if (extraData != null && extraData.containsKey(KEY_COOKIE)) {
          Object cookie = extraData.get(KEY_COOKIE);
          TestCrawlCB cb = callbacks.remove(cookie);
          if(cb != null) {
            log.debug2("Found callback for cookie: " + cookie);
            cb.signalCrawlAttemptCompleted(event.isSuccessful(),cookie, null);
          }

        }
      }   @Override
      protected void handleRepairCompleted(CrawlEvent event) {
        log.debug2(" repair completed was called");
        Map<String, Object> extraData = event.getExtraData();
        if (extraData != null && extraData.containsKey(KEY_COOKIE)) {
          Object cookie = extraData.get(KEY_COOKIE);
          TestCrawlCB cb = callbacks.get(cookie);
          log.debug2("Found callback for cookie: " + cookie);
          if(cb != null) {
            cb.signalCrawlAttemptCompleted(event.isSuccessful(),cookie, null);
          }
        }
      }
    };
    crawlManager.registerCrawlEventHandler(testHandler);
  }

  void setUpMockAu() {
    mau = new MockArchivalUnit();
    mau.setPlugin(plugin);
    mau.setCrawlRule(rule);
    mau.setStartUrls(ListUtil.list("blah"));
    mau.setCrawlWindow(new MockCrawlWindow(true));

    PluginTestUtil.registerArchivalUnit(plugin, mau);

    cus = mau.makeCachedUrlSet(new SingleNodeCachedUrlSetSpec(GENERIC_URL));
    crawler = new MockCrawler();
    crawlManager.setTestCrawler(crawler);

    AuTestUtil.setUpMockAus(mau);

  }

  public void tearDown() throws Exception {
    TimeBase.setReal();
    for (Iterator iter = semsToGive.iterator(); iter.hasNext(); ) {
      SimpleBinarySemaphore sem = (SimpleBinarySemaphore)iter.next();
      sem.give();
    }
    crawlManager.unregisterCrawlEventHandler(testHandler);
    crawlManager.stopService();
    theDaemon.stopDaemon();
    super.tearDown();
  }


  MockArchivalUnit newMockArchivalUnit(String auid) {
    MockArchivalUnit mau = new MockArchivalUnit(plugin, auid);
    AuTestUtil.setUpMockAus(mau);
    return mau;
  }

  SimpleBinarySemaphore semToGive(SimpleBinarySemaphore sem) {
    semsToGive.add(sem);
    return sem;
  }

  String didntMsg(String what, long time) {
    return "Crawl didn't " + what + " in " +
        TimeUtil.timeIntervalToString(time);
  }

  protected void waitForCrawlToFinish(SimpleBinarySemaphore sem) {
    if (!sem.take(TIMEOUT_SHOULDNT)) {
      PlatformUtil.getInstance().threadDump(true);
      TimerUtil.guaranteedSleep(1000);
      fail(didntMsg("finish", TIMEOUT_SHOULDNT));
    }
  }

  MockArchivalUnit[] makeMockAus(int n) {
    MockArchivalUnit[] res = new MockArchivalUnit[n];
    for (int ix = 0; ix < n; ix++) {
      res[ix] = newMockArchivalUnit("mau" + ix);
    }
    return res;
  }

  public static class TestsWithAutoStart extends TestCrawlManagerImpl {
    public void setUp() throws Exception {
      super.setUp();
      setUpMockAu();
      theDaemon.setAusStarted(true);
      cprops.put(CrawlManagerImpl.PARAM_USE_ODC, "false");
      ConfigurationUtil.addFromProps(cprops);
      crawlManager.startService();
    }

    public void testNullAuForIsCrawlingAu()  throws Exception {
      try {
        TestCrawlCB cb = new TestCrawlCB(new SimpleBinarySemaphore());
        String cookie = randomUUID().toString();
        callbacks.put(cookie, cb);
        crawlManager.startNewContentCrawl(null, cookie);
        fail("Didn't throw an IllegalArgumentException on a null AU");
      } catch (IllegalArgumentException iae) {
      }
    }

    //   public void testNewCrawlDeadlineIsMax() {
    //     SimpleBinarySemaphore sem = new SimpleBinarySemaphore();

    //     crawlManager.startNewContentCrawl(mau, new TestCrawlCB(sem), null, null);

    //     waitForCrawlToFinish(sem);
    //     assertEquals(Deadline.MAX, crawler.getDeadline());
    //   }

    public void testStoppingCrawlAbortsNewContentCrawl() throws Exception {
      SimpleBinarySemaphore finishedSem = new SimpleBinarySemaphore();
      //start the crawler, but use a crawler that will hang

      TestCrawlCB cb = new TestCrawlCB(finishedSem);
      SimpleBinarySemaphore sem1 = new SimpleBinarySemaphore();
      SimpleBinarySemaphore sem2 = new SimpleBinarySemaphore();
      //gives sem1 when doCrawl is entered, then takes sem2
      MockCrawler crawler =
          new HangingCrawler("testStoppingCrawlAbortsNewContentCrawl",
              sem1, sem2);
      semToGive(sem2);
      crawlManager.setTestCrawler(crawler);

      assertFalse(sem1.take(0));
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      crawlManager.startNewContentCrawl(mau, cookie);
      assertTrue(didntMsg("start", TIMEOUT_SHOULDNT),
          sem1.take(TIMEOUT_SHOULDNT));
      //we know that doCrawl started

      // stopping AU should run AuEventHandler, which should cancel crawl
      pluginMgr.stopAu(mau, AuEvent.forAu(mau, AuEvent.Type.Delete));

      assertTrue(crawler.wasAborted());
    }

    public void testStoppingCrawlDoesntAbortCompletedCrawl() throws Exception {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();

      String cookie = randomUUID().toString();
      callbacks.put(cookie, new TestCrawlCB(sem));

      crawlManager.startNewContentCrawl(mau, cookie);

      waitForCrawlToFinish(sem);
      assertTrue("doCrawl() not called", crawler.doCrawlCalled());

      crawlManager.auEventDeleted(AuEvent.forAu(mau, AuEvent.Type.Delete), mau);

      assertFalse(crawler.wasAborted());
    }

    public void testStoppingCrawlAbortsRepairCrawl() throws Exception {
      String url1 = "http://www.example.com/index1.html";
      String url2 = "http://www.example.com/index2.html";
      List urls = ListUtil.list(url1, url2);

      SimpleBinarySemaphore finishedSem = new SimpleBinarySemaphore();
      //start the crawler, but use a crawler that will hang

      String cookie = randomUUID().toString();
      callbacks.put(cookie, new TestCrawlCB(finishedSem));
      SimpleBinarySemaphore sem1 = new SimpleBinarySemaphore();
      SimpleBinarySemaphore sem2 = new SimpleBinarySemaphore();
      //gives sem1 when doCrawl is entered, then takes sem2
      MockCrawler crawler =
          new HangingCrawler("testStoppingCrawlAbortsRepairCrawl",
              sem1, sem2);
      crawler.setType(Crawler.Type.REPAIR);
      semToGive(sem2);
      crawlManager.setTestCrawler(crawler);

      crawlManager.startRepair(mau, urls, cookie);
      assertTrue(didntMsg("start", TIMEOUT_SHOULDNT),
          sem1.take(TIMEOUT_SHOULDNT));
      //we know that doCrawl started

      crawlManager.auEventDeleted(AuEvent.forAu(mau, AuEvent.Type.Delete), mau);

      assertTrue(crawler.wasAborted());
    }


    class AbortRecordingCrawler extends MockCrawler {
      List<ArchivalUnit> abortedCrawls = new ArrayList<ArchivalUnit>();

      public AbortRecordingCrawler(List<ArchivalUnit> abortedCrawlsList) {
        super();
        this.abortedCrawls = abortedCrawlsList;
        this.setType(Type.NEW_CONTENT);
      }

      public AbortRecordingCrawler(List<ArchivalUnit> abortedCrawlsList,
          ArchivalUnit au) {
        super(au);
        this.abortedCrawls = abortedCrawlsList;
        this.setType(Type.NEW_CONTENT);

      }

      public void abortCrawl() {
        abortedCrawls.add(getAu());
        super.abortCrawl();
        crawlManager.removeFromRunningCrawls(this);
      }

      List<ArchivalUnit> getAbortedCrawls() {
        return abortedCrawls;
      }

    }

    public void testAbortCrawl() throws Exception {
      theDaemon.setAusStarted(true);
      List<ArchivalUnit> abortLst = new ArrayList<ArchivalUnit>();
      MockArchivalUnit[] aus = makeMockAus(5);

      for (ArchivalUnit au : aus) {
        MockCrawler mc = new AbortRecordingCrawler(abortLst, au);
        crawlManager.addToRunningCrawls(au, mc);
      }
      assertEmpty(abortLst);
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AUID_MAP,
          "(4|5),3");
      assertEmpty(abortLst);
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AUID_MAP,
          "(4|5),-25000");
      assertSameElements(ListUtil.list(aus[4]), abortLst);
      crawlManager.addToRunningCrawls(aus[4],
          new AbortRecordingCrawler(abortLst,
              aus[4]));
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AUID_MAP,
          "(3|4|5),-25000");
      // CrawlManagerImpl processes running crawls in indeterminate order
      assertSameElements(ListUtil.list(aus[4], aus[3], aus[4]), abortLst);
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AUID_MAP,
          "(3|4|5),-25000;(xx|yy),-26000");
      assertSameElements(ListUtil.list(aus[4], aus[3], aus[4]), abortLst);
    }

    private void setNewContentRateLimit(String rate, String startRate,
        String pluginRate) {
      cprops.put(CrawlManagerImpl.PARAM_MAX_NEW_CONTENT_RATE, rate);
      cprops.put(CrawlManagerImpl.PARAM_NEW_CONTENT_START_RATE, startRate);
      cprops.put(CrawlManagerImpl.PARAM_MAX_PLUGIN_REGISTRY_NEW_CONTENT_RATE,
          pluginRate);
      ConfigurationUtil.addFromProps(cprops);
    }

    private void setRepairRateLimit(int events, long interval) {
      cprops.put(CrawlManagerImpl.PARAM_MAX_REPAIR_RATE, events+"/"+interval);
      ConfigurationUtil.addFromProps(cprops);
    }

    private void assertDoesCrawlNew(MockCrawler crawler) {
      crawler.setDoCrawlCalled(false);
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      crawlManager.startNewContentCrawl(mau,cookie);
      waitForCrawlToFinish(sem);
      assertTrue("doCrawl() not called at time " + TimeBase.nowMs(),
          crawler.doCrawlCalled());
    }

    private void assertDoesCrawlNew() {
      assertDoesCrawlNew(crawler);
    }

    private void assertDoesNotCrawlNew() {
      crawler.setDoCrawlCalled(false);
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      crawlManager.startNewContentCrawl(mau, cookie);
      waitForCrawlToFinish(sem);
      assertFalse("doCrawl() called at time " + TimeBase.nowMs(),
          crawler.doCrawlCalled());
    }

    private void assertDoesCrawlRepair() {
      crawler.setDoCrawlCalled(false);
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      String cookie = randomUUID().toString();
      callbacks.put(cookie, new TestCrawlCB(sem));

      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL), cookie);
      waitForCrawlToFinish(sem);
      assertTrue("doCrawl() not called at time " + TimeBase.nowMs(),
          crawler.doCrawlCalled());
    }

    private void assertDoesNotCrawlRepair() {
      crawler.setDoCrawlCalled(false);
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      String cookie = randomUUID().toString();
      callbacks.put(cookie, new TestCrawlCB(sem));
      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL),cookie);
      waitForCrawlToFinish(sem);
      assertFalse("doCrawl() called at time " + TimeBase.nowMs(),
          crawler.doCrawlCalled());
    }

    void assertEligible(ArchivalUnit au)
        throws CrawlManagerImpl.NotEligibleException {
      assertTrue(crawlManager.isEligibleForNewContentCrawl(au));
      crawlManager.checkEligibleForNewContentCrawl(au);
    }

    void assertNotEligible(String expectedExceptionMessageRE,
        ArchivalUnit au) {
      assertNotEligible(CrawlManagerImpl.NotEligibleException.class,
          expectedExceptionMessageRE,
          au);
    }

    void assertNotEligible(Class expectedExceptionClass,
        String expectedExceptionMessageRE,
        ArchivalUnit au) {
      assertFalse(crawlManager.isEligibleForNewContentCrawl(au));
      try {
        crawlManager.checkEligibleForNewContentCrawl(au);
      } catch (Exception e) {
        assertClass(expectedExceptionClass, e);
        assertMatchesRE(expectedExceptionMessageRE, e.getMessage());
      }
    }

    void assertQueueable(ArchivalUnit au)
        throws CrawlManagerImpl.NotEligibleException {
      crawlManager.checkEligibleToQueueNewContentCrawl(au);
    }

    void assertNotQueueable(String expectedExceptionMessageRE,
        ArchivalUnit au) {
      assertNotQueueable(CrawlManagerImpl.NotEligibleException.class,
          expectedExceptionMessageRE,
          au);
    }

    void assertNotQueueable(Class expectedExceptionClass,
        String expectedExceptionMessageRE,
        ArchivalUnit au) {
      try {
        crawlManager.checkEligibleToQueueNewContentCrawl(au);
      } catch (Exception e) {
        assertClass(expectedExceptionClass, e);
        assertMatchesRE(expectedExceptionMessageRE, e.getMessage());
      }
    }

    void makeRateLimiterReturn(RateLimiter limiter, boolean val) {
      if (val) {
        while (!limiter.isEventOk()) {
          limiter.unevent();
        }
      } else {
        while (limiter.isEventOk()) {
          limiter.event();
        }
      }
    }

    public void testIsEligibleForNewContentCrawl() throws Exception {
      assertQueueable(mau);
      assertEligible(mau);
      crawlManager.setRunningNCCrawl(mau, true);
      assertNotQueueable("is crawling now", mau);
      assertNotEligible("is crawling now", mau);
      crawlManager.setRunningNCCrawl(mau, false);
      assertQueueable(mau);
      assertEligible(mau);
      mau.setCrawlWindow(new CrawlWindows.Never());
      assertQueueable(mau);
      assertNotEligible("window.*closed", mau);
      mau.setCrawlWindow(new CrawlWindows.Always());
      assertQueueable(mau);
      assertEligible(mau);

      RateLimiter limit = crawlManager.getNewContentRateLimiter(mau);
      makeRateLimiterReturn(limit, false);
      assertNotQueueable(CrawlManagerImpl.NotEligibleException.RateLimiter.class,
          "Exceeds crawl-start rate", mau);
      assertNotEligible(CrawlManagerImpl.NotEligibleException.RateLimiter.class,
          "Exceeds crawl-start rate", mau);
      makeRateLimiterReturn(limit, true);
      assertQueueable(mau);
      assertEligible(mau);
    }

    public void testDoesNCCrawl() {
      assertDoesCrawlNew();
    }

    public void testDoesntNCCrawlWhenOutsideWindow() {
      mau.setCrawlWindow(new ClosedCrawlWindow());
      assertDoesNotCrawlNew();
    }

    public void testNewContentRateLimiter() {
      TimeBase.setSimulated(100);
      setNewContentRateLimit("4/100", "unlimited", "1/100000");
      for (int ix = 1; ix <= 4; ix++) {
        assertDoesCrawlNew();
        TimeBase.step(10);
      }
      assertDoesNotCrawlNew();
      TimeBase.step(10);
      assertDoesNotCrawlNew();
      TimeBase.step(50);
      assertDoesCrawlNew();
      TimeBase.step(500);
      // ensure RateLimiter changes when config does
      setNewContentRateLimit("2/5", "unlimited", "1/100000");
      assertDoesCrawlNew();
      assertDoesCrawlNew();
      assertDoesNotCrawlNew();
      TimeBase.step(5);
      assertDoesCrawlNew();
      assertDoesCrawlNew();
    }

    public void testPluginRegistryNewContentRateLimiter() {
      crawlManager.setInternalAu(true);
      TimeBase.setSimulated(100);
      setNewContentRateLimit("1/1000000", "unlimited", "3/100");
      for (int ix = 1; ix <= 3; ix++) {
        assertDoesCrawlNew();
        TimeBase.step(10);
      }
      assertDoesNotCrawlNew();
      TimeBase.step(10);
      assertDoesNotCrawlNew();
      TimeBase.step(60);
      assertDoesCrawlNew();
      TimeBase.step(500);
      // ensure RateLimiter changes when config does
      setNewContentRateLimit("1/1000000", "unlimited", "2/5");
      assertDoesCrawlNew();
      assertDoesCrawlNew();
      assertDoesNotCrawlNew();
      TimeBase.step(5);
      assertDoesCrawlNew();
      assertDoesCrawlNew();
    }

    public void testNewContentRateLimiterWindowClose() {
      TimeBase.setSimulated(100);
      mau.setStartUrls(ListUtil.list("two"));
      mau.setCrawlRule(new MockCrawlRule());
      setNewContentRateLimit("1/10", "unlimited", "1/100000");
      assertDoesCrawlNew();
      assertDoesNotCrawlNew();
      TimeBase.step(10);
      assertDoesCrawlNew();
      assertDoesNotCrawlNew();
      TimeBase.step(10);

      MockCrawler c2 = new CloseWindowCrawler();
      crawlManager.setTestCrawler(mau, c2);
      assertDoesCrawlNew(c2);
    }

    public void testRepairRateLimiter() {
      TimeBase.setSimulated(100);
      setRepairRateLimit(6, 200);
      for (int ix = 1; ix <= 6; ix++) {
        assertDoesCrawlRepair();
        TimeBase.step(10);
      }
      assertDoesNotCrawlRepair();
      TimeBase.step(10);
      assertDoesNotCrawlRepair();
      TimeBase.step(130);
      assertDoesCrawlRepair();
      TimeBase.step(500);
      // ensure RateLimiter changes when config does
      setRepairRateLimit(2, 5);
      assertDoesCrawlRepair();
      assertDoesCrawlRepair();
      assertDoesNotCrawlRepair();
      TimeBase.step(5);
      assertDoesCrawlRepair();
    }

    public void testBasicNewContentCrawl() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      String cookie = randomUUID().toString();
      callbacks.put(cookie, new TestCrawlCB(sem));

      crawlManager.startNewContentCrawl(mau, cookie);

      waitForCrawlToFinish(sem);
      assertTrue("doCrawl() not called", crawler.doCrawlCalled());
    }

    //If the AU throws, the crawler should trap it and call the callbacks
    public void testCallbacksCalledWhenPassedThrowingAU() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      ThrowingAU au = new ThrowingAU();
      try {
	crawlManager.startNewContentCrawl(au, cookie);
	fail("Should have thrown ExpectedRuntimeException");
      } catch (ExpectedRuntimeException ere) {
	assertTrue(cb.wasTriggered());
      }
    }

    public void testRepairCrawlFreesActivityLockWhenDone() {
      String url1 = "http://www.example.com/index1.html";
      String url2 = "http://www.example.com/index2.html";
      List urls = ListUtil.list(url1, url2);

      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      CachedUrlSet cus1 =
          mau.makeCachedUrlSet(new SingleNodeCachedUrlSetSpec(url1));
      CachedUrlSet cus2 =
          mau.makeCachedUrlSet(new SingleNodeCachedUrlSetSpec(url2));

      crawlManager.startRepair(mau, urls, cookie);

      waitForCrawlToFinish(sem);
    }

    List<AuEvent.ContentChangeInfo> changeEvents = new ArrayList();
    SimpleBinarySemaphore eventSem = new SimpleBinarySemaphore();

    class MyAuEventHandler extends AuEventHandler.Base {
      @Override public void auContentChanged(AuEvent event, ArchivalUnit au,
          AuEvent.ContentChangeInfo info) {
        changeEvents.add(info);
        eventSem.give();
      }
    }

    public void testAuEventCrawl() {
      AuEventCrawler auc = new AuEventCrawler(mau);
      crawler = auc;
      auc.setCrawlSuccessful(true);
      auc.setMimes(ListUtil.list(ListUtil.list("text1", "text/plain"),
          ListUtil.list("html1", "text/html"),
          ListUtil.list("html2", "text/html;charset=foo"),
          ListUtil.list("pdf1", "application/pdf")));
      crawlManager.setTestCrawler(auc);

      pluginMgr.registerAuEventHandler(new MyAuEventHandler());
      crawlManager.startNewContentCrawl(mau, null);
      waitForCrawlToFinish(eventSem);
      assertEquals(1, changeEvents.size());
      AuEvent.ContentChangeInfo ci = changeEvents.get(0);
      assertEquals(AuEvent.ContentChangeInfo.Type.Crawl, ci.getType());
      assertTrue(ci.isComplete());
      Map expMime = MapUtil.map("text/plain", 1,
          "text/html", 2,
          "application/pdf", 1);
      assertEquals(4, ci.getNumUrls());
      assertEquals(expMime, ci.getMimeCounts());
      assertFalse(ci.hasUrls());
      assertNull(ci.getUrls());
    }

    public void testAuEventCrawlAborted() {
      AuEventCrawler auc = new AuEventCrawler(mau);
      crawler = auc;
      auc.setCrawlSuccessful(false);
      auc.setMimes(ListUtil.list(ListUtil.list("text1", "text/plain"),
          ListUtil.list("html1", "text/html"),
          ListUtil.list("html2", "text/html;charset=foo"),
          ListUtil.list("pdf1", "application/pdf")));
      crawlManager.setTestCrawler(auc);

      pluginMgr.registerAuEventHandler(new MyAuEventHandler());
      crawlManager.startNewContentCrawl(mau, null);
      waitForCrawlToFinish(eventSem);
      assertEquals(1, changeEvents.size());
      AuEvent.ContentChangeInfo ci = changeEvents.get(0);
      assertEquals(AuEvent.ContentChangeInfo.Type.Crawl, ci.getType());
      assertFalse(ci.isComplete());
      Map expMime = MapUtil.map("text/plain", 1,
          "text/html", 2,
          "application/pdf", 1);
      assertEquals(4, ci.getNumUrls());
      assertEquals(expMime, ci.getMimeCounts());
      assertFalse(ci.hasUrls());
      assertNull(ci.getUrls());
    }

    public void testAuEventRepairCrawl() {
      AuEventCrawler auc = new AuEventCrawler(mau);
      crawler = auc;
      auc.setCrawlSuccessful(true);
      auc.setIsWholeAU(false);
      auc.setMimes(ListUtil.list(ListUtil.list("text1", "text/plain"),
          ListUtil.list("html1", "text/html"),
          ListUtil.list("html2", "text/html;charset=foo"),
          ListUtil.list("pdf1", "application/pdf")));
      crawlManager.setTestCrawler(auc);

      pluginMgr.registerAuEventHandler(new MyAuEventHandler());
      crawlManager.startRepair(mau, ListUtil.list("foo"), null);
      waitForCrawlToFinish(eventSem);
      assertEquals(1, changeEvents.size());
      AuEvent.ContentChangeInfo ci = changeEvents.get(0);
      assertEquals(AuEvent.ContentChangeInfo.Type.Repair, ci.getType());
      assertTrue(ci.isComplete());
      Map expMime = MapUtil.map("text/plain", 1,
          "text/html", 2,
          "application/pdf", 1);
      assertEquals(4, ci.getNumUrls());
      assertEquals(expMime, ci.getMimeCounts());
      assertTrue(ci.hasUrls());
      assertSameElements(ListUtil.list("text1", "html1", "html2", "pdf1"),
          ci.getUrls());
    }

    public void testNewContentCallbackTriggered() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();

      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);

      crawlManager.startNewContentCrawl(mau,cookie);

      waitForCrawlToFinish(sem);
      assertTrue("Callback wasn't triggered", cb.wasTriggered());
    }


    public void testNewContentCrawlCallbackReturnsCookie() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);

      crawlManager.startNewContentCrawl(mau, cookie);

      waitForCrawlToFinish(sem);
      assertEquals(cookie, (String)cb.getCookie());
    }

    public void testNewContentCrawlCallbackReturnsNullCookie() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      String cookie = null;
      TestCrawlCB cb = new TestCrawlCB(sem);
      CrawlEventHandler handler = new CrawlEventHandler.Base() {
        @Override
        protected void handleNewContentCompleted(CrawlEvent event) {
          Map<String, Object> extraData = event.getExtraData();
          Object retCookie = null;
          if (extraData != null && extraData.containsKey(KEY_COOKIE)) {
            retCookie= extraData.get(KEY_COOKIE);
          }
          cb.signalCrawlAttemptCompleted(event.isSuccessful(),retCookie,null);
        }
      };
      crawlManager.registerCrawlEventHandler(handler);
      crawlManager.startNewContentCrawl(mau, cookie);

      waitForCrawlToFinish(sem);
      assertEquals(cookie, (String)cb.getCookie());
      crawlManager.unregisterCrawlEventHandler(handler);
    }

    public void testNewContentSendsCrawlEvent()
    {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      crawlManager.startNewContentCrawl(mau, cookie);

      waitForCrawlToFinish(sem);
      assertEquals(cookie, (String)cb.getCookie());
    }

    public void testKicksOffNewThread() {
      SimpleBinarySemaphore finishedSem = new SimpleBinarySemaphore();
      //start the crawler, but use a crawler that will hang
      //if we aren't running it in another thread we'll hang too

      TestCrawlCB cb = new TestCrawlCB(finishedSem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      SimpleBinarySemaphore sem1 = new SimpleBinarySemaphore();
      SimpleBinarySemaphore sem2 = new SimpleBinarySemaphore();
      //gives sem1 when doCrawl is entered, then takes sem2
      MockCrawler crawler = new HangingCrawler("testKicksOffNewThread",
          sem1, sem2);
      semToGive(sem2);
      crawler.setType(Crawler.Type.NEW_CONTENT);
      crawlManager.setTestCrawler(crawler);

      crawlManager.startNewContentCrawl(mau, cookie);
      assertTrue(didntMsg("start", TIMEOUT_SHOULDNT),
          sem1.take(TIMEOUT_SHOULDNT));
      //we know that doCrawl started

      //if the callback was triggered, the crawl completed
      assertFalse("Callback was triggered", cb.wasTriggered());
      sem2.give();
      waitForCrawlToFinish(finishedSem);
    }

    public void testScheduleRepairNullAu() {
      try{
        String cookie = randomUUID().toString();
        callbacks.put(cookie, new TestCrawlCB());
        crawlManager.startRepair(null, ListUtil.list("http://www.example.com"), cookie);
        fail("Didn't throw IllegalArgumentException on null AU");
      } catch (IllegalArgumentException iae) {
      }
    }

    public void testScheduleRepairNullUrls() {
      try{
        String cookie = randomUUID().toString();
        callbacks.put(cookie, new TestCrawlCB());
        crawlManager.startRepair(mau, (Collection)null,cookie);
        fail("Didn't throw IllegalArgumentException on null URL list");
      } catch (IllegalArgumentException iae) {
      }
    }

    public void testBasicRepairCrawl() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);

      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL),cookie);

      waitForCrawlToFinish(sem);
      assertTrue("doCrawl() not called", crawler.doCrawlCalled());
    }

    public void testRepairCallbackTriggered() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);

      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL), cookie);

      waitForCrawlToFinish(sem);
      assertTrue("Callback wasn't triggered", cb.wasTriggered());
    }

    public void testRepairCallbackGetsCookie() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);

      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL), cookie);

      waitForCrawlToFinish(sem);
      assertEquals(cookie, cb.getCookie());
    }

    //StatusSource tests

    public void testGetCrawlStatusListReturnsEmptyListIfNoCrawls() {
      List list = statusSource.getStatus().getCrawlerStatusList();
      assertEquals(0, list.size());
    }


    public void testGetCrawlsOneRepairCrawl() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL), cookie);
      List actual = statusSource.getStatus().getCrawlerStatusList();
      List expected = ListUtil.list(crawler.getCrawlerStatus());

      assertEquals(expected, actual);
      waitForCrawlToFinish(sem);
    }

    public void testGetCrawlsOneNCCrawl() {
      SimpleBinarySemaphore sem = new SimpleBinarySemaphore();
      TestCrawlCB cb = new TestCrawlCB(sem);
      String cookie = randomUUID().toString();
      callbacks.put(cookie, cb);
      crawlManager.startNewContentCrawl(mau, cookie);
      List actual = statusSource.getStatus().getCrawlerStatusList();
      List expected = ListUtil.list(crawler.getCrawlerStatus());
      assertEquals(expected, actual);
      waitForCrawlToFinish(sem);
    }

    public void testGetCrawlsMulti() {
      SimpleBinarySemaphore sem1 = new SimpleBinarySemaphore();
      SimpleBinarySemaphore sem2 = new SimpleBinarySemaphore();
      String cookie1 = randomUUID().toString();
      String cookie2 = randomUUID().toString();
      callbacks.put(cookie1, new TestCrawlCB(sem1));
      callbacks.put(cookie2, new TestCrawlCB(sem2));
      crawlManager.startRepair(mau, ListUtil.list(GENERIC_URL), cookie1);

      MockCrawler crawler2 = new MockCrawler();
      crawlManager.setTestCrawler(crawler2);
      crawlManager.startNewContentCrawl(mau, cookie2);

      // The two status objects will have been added to the status map in
      // the order created above, but if one of them gets referenced before
      // this test it might be moved to the most-recently-used position of
      // the LRUMap, so the order here is unpredictable
      List actual = statusSource.getStatus().getCrawlerStatusList();
      Set expected = SetUtil.set(crawler.getCrawlerStatus(), crawler2.getCrawlerStatus());
      assertEquals(expected, SetUtil.theSet(actual));
      waitForCrawlToFinish(sem1);
      waitForCrawlToFinish(sem2);
    }

    public void testGloballyPermittedHosts() {
      assertFalse(crawlManager.isGloballyPermittedHost("foo27.com"));
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_PERMITTED_HOSTS,
          "foo[0-9]+\\.com");
      assertTrue(crawlManager.isGloballyPermittedHost("foo27.com"));
      assertFalse(crawlManager.isGloballyPermittedHost("bar42.edu"));

      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_PERMITTED_HOSTS,
          "^foo[0-9]+\\.com$;^bar..\\.edu$");
      assertTrue(crawlManager.isGloballyPermittedHost("foo27.com"));
      assertTrue(crawlManager.isGloballyPermittedHost("bar42.edu"));
      assertFalse(crawlManager.isGloballyPermittedHost("foo27.com;bar42.edu"));
    }

    public void testAllowedPluginPermittedHosts() {
      assertFalse(crawlManager.isAllowedPluginPermittedHost("foo27.com"));
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_PERMITTED_HOSTS,
          "foo[0-9]+\\.com");
      assertFalse(crawlManager.isAllowedPluginPermittedHost("foo27.com"));
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_ALLOWED_PLUGIN_PERMITTED_HOSTS,
          "foo[0-9]+\\.com");
      assertTrue(crawlManager.isAllowedPluginPermittedHost("foo27.com"));
      ConfigurationUtil.removeKey(CrawlManagerImpl.PARAM_PERMITTED_HOSTS);
      assertTrue(crawlManager.isAllowedPluginPermittedHost("foo27.com"));

      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_ALLOWED_PLUGIN_PERMITTED_HOSTS,
          "^foo[0-9]+\\.com$;^baz..\\.edu$");
      assertTrue(crawlManager.isAllowedPluginPermittedHost("foo27.com"));
      assertTrue(crawlManager.isAllowedPluginPermittedHost("baz42.edu"));
      assertFalse(crawlManager.isAllowedPluginPermittedHost("foo27.com;baz42.edu"));
    }

    public void testDeleteRemovesFromHighPriorityQueue() {
      crawlManager.disableCrawlStarter();
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_USE_ODC, "true");
      MockArchivalUnit mau1 = newMockArchivalUnit("foo1");
      PluginTestUtil.registerArchivalUnit(plugin, mau1);
      CrawlReq req = new CrawlReq(mau1, new CrawlerStatus(mau1,
	  mau1.getStartUrls(), null));
      req.setPriority(8);
      req.setRefetchDepth(1232);
      crawlManager.enqueueHighPriorityCrawl(req);
      //startNewContentCrawl
      List<ArchivalUnit> quas = crawlManager.getHighPriorityAus();
      assertEquals(ListUtil.list(mau1), quas);
      crawlManager.auEventDeleted(AuEvent.forAu(mau1, AuEvent.Type.Delete),
          mau1);
      assertEmpty(crawlManager.getHighPriorityAus());
      crawlManager.auEventCreated(AuEvent.forAu(mau1, AuEvent.Type.Create),
          mau1);
      assertEmpty(crawlManager.getHighPriorityAus());
      crawlManager.enqueueHighPriorityCrawl(req);
      assertEquals(ListUtil.list(mau1), crawlManager.getHighPriorityAus());
      crawlManager.auEventDeleted(AuEvent.forAu(mau1, AuEvent.Type.RestartDelete),
          mau1);
      assertEmpty(crawlManager.getHighPriorityAus());
      crawlManager.auEventCreated(AuEvent.forAu(mau1, AuEvent.Type.Create),
          mau1);
      assertEquals(ListUtil.list(mau1), crawlManager.getHighPriorityAus());
    }

  }

  public static class TestsWithoutAutoStart extends TestCrawlManagerImpl {

    public void setUp() throws Exception {
      super.setUp();
    }

    public void testNoQueuePoolSize(int max) {
      setUpMockAu();
      cprops.put(CrawlManagerImpl.PARAM_CRAWLER_QUEUE_ENABLED, "false");
      cprops.put(CrawlManagerImpl.PARAM_USE_ODC, "false");
      cprops.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_MAX,
          Integer.toString(max));
      cprops.put(CrawlManagerImpl.PARAM_MAX_NEW_CONTENT_RATE, (max*2)+"/1h");
      cprops.put(CrawlManagerImpl.PARAM_NEW_CONTENT_START_RATE, "unlimited");
      ConfigurationUtil.addFromProps(cprops);
      crawlManager.startService();
      SimpleBinarySemaphore[] startSem = new SimpleBinarySemaphore[max];
      SimpleBinarySemaphore[] endSem = new SimpleBinarySemaphore[max];
      SimpleBinarySemaphore[] finishedSem = new SimpleBinarySemaphore[max];
      HangingCrawler[] crawler = new HangingCrawler[max];
      TestCrawlCB[] cb = new TestCrawlCB[max];
      for (int ix = 0; ix < max; ix++) {
        finishedSem[ix] = new SimpleBinarySemaphore();
        //start a crawler that hangs until we post its semaphore
        cb[ix] = new TestCrawlCB(finishedSem[ix]);
        String cookie = randomUUID().toString();
        callbacks.put(cookie, cb[ix]);
        startSem[ix] = new SimpleBinarySemaphore();
        endSem[ix] = new SimpleBinarySemaphore();
        //gives sem1 when doCrawl is entered, then takes sem2
        crawler[ix] = new HangingCrawler("testNoQueuePoolSize " + ix,
            startSem[ix], endSem[ix]);
        MockArchivalUnit au = newMockArchivalUnit("mau" + ix);
        au.setCrawlRule(rule);
        au.setStartUrls(ListUtil.list("blah"));
        PluginTestUtil.registerArchivalUnit(plugin, au);
        setupAuToCrawl(au, crawler[ix]);
        semToGive(endSem[ix]);

        crawlManager.startNewContentCrawl(au,cookie);
      }
      for (int ix = 0; ix < max; ix++) {
        assertTrue(didntMsg("start("+ix+")", TIMEOUT_SHOULDNT),
            startSem[ix].take(TIMEOUT_SHOULDNT));
        //we know that doCrawl started
      }
      MockCrawler crawlerN =
          new HangingCrawler("testNoQueuePoolSize ix+1",
              new SimpleBinarySemaphore(),
              semToGive(new SimpleBinarySemaphore()));
      crawlManager.setTestCrawler(crawlerN);
      TestCrawlCB onecb = new TestCrawlCB();
      String cookie = randomUUID().toString();
      callbacks.put(cookie, onecb);
      log.info("Pool is blocked exception expected");
      crawlManager.startNewContentCrawl(mau, cookie);
      assertTrue("Callback for non schedulable crawl wasn't triggered",
          onecb.wasTriggered());
      assertFalse("Non schedulable crawl succeeded",
          onecb.wasSuccessful());

      for (int ix = 0; ix < max; ix++) {
        //if the callback was triggered, the crawl completed
        assertFalse("Callback was triggered", cb[ix].wasTriggered());
        endSem[ix].give();
        waitForCrawlToFinish(finishedSem[ix]);
      }
    }

    public void testNoQueueBoundedPool() {
      testNoQueuePoolSize(5);
    }

    public void testQueuedPool(int qMax, int poolMax) {
      setUpMockAu();
      int tot = qMax + poolMax;
      cprops.put(CrawlManagerImpl.PARAM_CRAWLER_QUEUE_ENABLED, "true");
      cprops.put(CrawlManagerImpl.PARAM_USE_ODC, "false");
      cprops.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_MAX,
          Integer.toString(poolMax));
      cprops.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_QUEUE_SIZE,
          Integer.toString(qMax));
      cprops.put(CrawlManagerImpl.PARAM_MAX_NEW_CONTENT_RATE, (tot*2)+"/1h");
      int startInterval = 10;
      cprops.put(CrawlManagerImpl.PARAM_NEW_CONTENT_START_RATE,
          "1/" + startInterval);
      ConfigurationUtil.addFromProps(cprops);
      crawlManager.startService();
      HangingCrawler[] crawler = new HangingCrawler[tot];
      SimpleBinarySemaphore[] startSem = new SimpleBinarySemaphore[tot];
      SimpleBinarySemaphore[] endSem = new SimpleBinarySemaphore[tot];
      SimpleBinarySemaphore[] finishedSem = new SimpleBinarySemaphore[tot];
      TestCrawlCB[] cb = new TestCrawlCB[tot];
      long startTime[] = new long[tot];

      // queue enough crawlers to fill the queue and pool
      for (int ix = 0; ix < tot; ix++) {
        finishedSem[ix] = new SimpleBinarySemaphore();
        //start a crawler that hangs until we post its semaphore
        cb[ix] = new TestCrawlCB(finishedSem[ix]);
        String cookie = randomUUID().toString();
        callbacks.put(cookie, cb[ix]);
        startSem[ix] = new SimpleBinarySemaphore();
        endSem[ix] = new SimpleBinarySemaphore();
        //gives sem1 when doCrawl is entered, then takes sem2
        crawler[ix] = new HangingCrawler("testQueuedPool " + ix,
            startSem[ix], endSem[ix]);
        MockArchivalUnit au = newMockArchivalUnit("mau" + ix);
        au.setCrawlRule(rule);
        au.setStartUrls(ListUtil.list("blah"));
        PluginTestUtil.registerArchivalUnit(plugin, au);
        setupAuToCrawl(au, crawler[ix]);
        semToGive(endSem[ix]);

        // queue the crawl directly
        crawlManager.startNewContentCrawl(au,  cookie);
      }
      // wait for the first poolMax crawlers to start.  Keep track of their
      // start times
      for (int ix = 0; ix < tot; ix++) {
        if (ix < poolMax) {
          assertTrue(didntMsg("start("+ix+")", TIMEOUT_SHOULDNT),
              startSem[ix].take(TIMEOUT_SHOULDNT));
          startSem[ix] = null;

          startTime[ix] = crawler[ix].getStartTime();
        } else {
          assertFalse("Wasn't queued "+ix, cb[ix].wasTriggered());
          assertFalse("Shouldn't have started " + ix,
              startSem[ix].take(0));
        }
      }
      // now check that no two start times are closer together than allowed
      // by the start rate limiter.  We don't know what order the crawl
      // threads ran in, so must sort the times first.  Thefirst qMax will
      // be zero because only poolMax actually got started.
      Arrays.sort(startTime);
      long lastTime = 0;
      for (int ix = qMax; ix < tot; ix++) {
        long thisTime =  startTime[ix];
        assertTrue(  "Crawler " + ix + " started early in " +
                (thisTime - lastTime),
            thisTime >= lastTime + startInterval);
        lastTime = thisTime;
      }
      MockCrawler failcrawler =
          new HangingCrawler("testNoQueuePoolSize ix+1",
              new SimpleBinarySemaphore(),
              semToGive(new SimpleBinarySemaphore()));
      crawlManager.setTestCrawler(failcrawler);
      TestCrawlCB onecb = new TestCrawlCB();
      String cookie = randomUUID().toString();
      callbacks.put(cookie, onecb);
      log.info("Pool is blocked exception expected");
      crawlManager.startNewContentCrawl(mau, cookie);
      assertTrue("Callback for non schedulable crawl wasn't triggered",
          onecb.wasTriggered());
      assertFalse("Non schedulable crawl succeeded",
          onecb.wasSuccessful());

      for (int ix = poolMax; ix < tot; ix++) {
        int poke = randomActive(startSem, endSem);
        assertFalse("Shouldnt have finished "+poke, cb[poke].wasTriggered());
        endSem[poke].give();
        waitForCrawlToFinish(finishedSem[poke]);
        endSem[poke] = null;
        assertTrue(didntMsg("start("+ix+")", TIMEOUT_SHOULDNT),
            startSem[ix].take(TIMEOUT_SHOULDNT));
        startSem[ix] = null;
      }
    }

    public void testCrawlStarter(int qMax, int poolMax) {
      assertEmpty(pluginMgr.getAllAus());
      int tot = qMax + poolMax;
      Properties p = new Properties();
      p.put(CrawlManagerImpl.PARAM_CRAWLER_QUEUE_ENABLED,
          "true");
      p.put(CrawlManagerImpl.PARAM_USE_ODC, "false");
      p.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_MAX,
          Integer.toString(poolMax));
      p.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_QUEUE_SIZE,
          Integer.toString(qMax));
      p.put(CrawlManagerImpl.PARAM_MAX_NEW_CONTENT_RATE, (tot*2)+"/1h");
      p.put(CrawlManagerImpl.PARAM_START_CRAWLS_INITIAL_DELAY, "100");
      p.put(CrawlManagerImpl.PARAM_START_CRAWLS_INTERVAL, "1s");
      theDaemon.setAusStarted(true);
      ConfigurationUtil.addFromProps(p);
      crawlManager.ausStartedSem = new OneShotSemaphore();
      crawlManager.startService();
      HangingCrawler[] crawler = new HangingCrawler[tot];
      SimpleBinarySemaphore endSem = new SimpleBinarySemaphore();
      semToGive(endSem);
      crawlManager.recordExecute(true);
      assertEmpty(pluginMgr.getAllAus());
      // Create AUs and build the pieces necessary for the crawl starter to
      // get the list of AUs from the PluginManager.  The crawl starter
      // should then shortly start poolMax of them.  We only check that it
      // called startNewContentCrawl() on each.
      for (int ix = 0; ix < tot-1; ix++) {
        crawler[ix] = new HangingCrawler("testQueuedPool " + ix,
            null, endSem);
        MockArchivalUnit au = newMockArchivalUnit("mau" + ix);
        au.setCrawlRule(rule);
        au.setStartUrls(ListUtil.list("blah"));
        setupAuToCrawl(au, crawler[ix]);
        PluginTestUtil.registerArchivalUnit(plugin, au);
      }
      // Last one is a RegistryArchivalUnit crawl
      RegistryArchivalUnit rau = makeRegistryAu();
      crawler[tot-1] = new HangingCrawler("testQueuedPool(reg au) " + (tot-1),
          null, endSem);
      setupAuToCrawl(rau, crawler[tot-1]);
      pluginMgr.addRegistryAu(rau);

      // now let the crawl starter proceed
      crawlManager.ausStartedSem.fill();
      // Ensure they all got queued
      List exe = Collections.EMPTY_LIST;
      Interrupter intr = interruptMeIn(TIMEOUT_SHOULDNT, true);
      while (true) {
        exe = crawlManager.getExecuted();
        if (exe.size() == tot) {
          break;
        }
        crawlManager.waitExecuted();
        assertFalse("Only " + exe.size() + " of " + tot +
                " expected crawls were started by the crawl starter",
            intr.did());
      }
      intr.cancel();
      assertEquals(SetUtil.fromArray(crawler),
          SetUtil.theSet(crawlersOf(exe)));
      // The registry au crawl should always be first
      assertEquals(crawler[tot-1], crawlersOf(exe).get(0));
    }

    void setupAuToCrawl(ArchivalUnit au, MockCrawler crawler) {
      crawlManager.setTestCrawler(au, crawler);
    }

    List crawlersOf(List runners) {
      List res = new ArrayList();
      for (Iterator iter = runners.iterator(); iter.hasNext(); ) {
        CrawlManagerImpl.CrawlRunner runner =
            (CrawlManagerImpl.CrawlRunner)iter.next();
        res.add(runner.getCrawler());
      }
      return res;
    }

    static LockssRandom random = new LockssRandom();

    int randomActive(SimpleBinarySemaphore[] startSem,
        SimpleBinarySemaphore[] endSem) {
      while (true) {
        int x = random.nextInt(startSem.length);
        if (startSem[x] == null && endSem[x] != null) return x;
      }
    }

    public void testQueuedPool() {
      testQueuedPool(10, 3);
    }

    public void testCrawlStarter() {
      testCrawlStarter(10, 3);
    }


    // ODC tests

    CrawlReq[] makeReqs(int n) {
      CrawlReq[] res = new CrawlReq[n];
      for (int ix = 0; ix < n; ix++) {
        MockArchivalUnit mau = newMockArchivalUnit(String.format("mau%2d", ix));
        res[ix] =
            new CrawlReq(mau, new CrawlerStatus(mau, mau.getStartUrls(), null));
      }
      return res;
    }

    void setReq(CrawlReq req,
        int pri, int crawlResult,
        long crawlAttempt, long crawlFinish) {
      setReq(req, pri, crawlResult, crawlAttempt, crawlFinish, null);
    }

    void setReq(CrawlReq req,
        int pri, int crawlResult,
        long crawlAttempt, long crawlFinish,
        String limiterKey) {
      req.priority = pri;
      setAu((MockArchivalUnit)req.getAu(),
          crawlResult, crawlAttempt, crawlFinish, limiterKey);
    }

    void setAu(MockArchivalUnit mau,
        int crawlResult, long crawlAttempt, long crawlFinish) {
      setAu(mau, crawlResult, crawlAttempt, crawlFinish, null);
    }

    void setAu(MockArchivalUnit mau,
        int crawlResult, long crawlAttempt, long crawlFinish,
        String limiterKey) {
      MockAuState aus = (MockAuState)AuUtil.getAuState(mau);
      aus.setLastCrawlTime(crawlFinish);
      aus.setLastCrawlAttempt(crawlAttempt);
      aus.setLastCrawlResult(crawlResult, "foo");
      mau.setFetchRateLimiterKey(limiterKey);
    }

    CrawlManagerImpl.CrawlPriorityComparator cmprtr() {
      return crawlManager.cmprtr();
    }

    void assertCompareLess(CrawlReq r1, CrawlReq r2) {
      assertTrue("Expected " + r1 + " less than " + r2 + " but wasn't",
          cmprtr().compare(r1, r2) < 0);
    }

    public void testCrawlPriorityComparator(CrawlReq[] reqs) {
      for (int ix = 0; ix <= reqs.length - 2; ix++) {
        assertCompareLess(reqs[ix], reqs[ix+1]);
      }
      List lst = ListUtil.fromArray(reqs);
      for (int ix = 0; ix <= 5; ix++) {
        TreeSet sorted = new TreeSet(cmprtr());
        sorted.addAll(CollectionUtil.randomPermutation(lst));
        sorted.add(reqs[0]);
        assertIsomorphic(reqs, sorted);
      }
    }

    public void testCrawlPriorityComparator1() {
      ConfigurationUtil.setFromArgs(CrawlManagerImpl.PARAM_RESTART_AFTER_CRASH,
          "true");
      CrawlReq[] reqs = makeReqs(11);
      setReq(reqs[0], 1, Crawler.STATUS_WINDOW_CLOSED, 9999, 9999);
      setReq(reqs[1], 1, 0, 5000, 5000);
      setReq(reqs[2], 0, Crawler.STATUS_WINDOW_CLOSED, -1, 2000);
      setReq(reqs[3], 0, Crawler.STATUS_WINDOW_CLOSED, 1000, 1000);
      setReq(reqs[4], 0, Crawler.STATUS_RUNNING_AT_CRASH, 1000, 1000);
      setReq(reqs[5], 0, 0, -1, 500);
      setReq(reqs[6], 0, 0, 123, -1);
      setReq(reqs[7], 0, 0, 123, 456);
      setReq(reqs[8], 0, Crawler.STATUS_WINDOW_CLOSED, -1, 2000);
      setReq(reqs[9], 0, 0, -1, 500);
      setReq(reqs[10], 1, 0, 5000, 5000);
      for (int ix=8; ix<=10; ix++) {
        reqs[ix].auDeleted();
      }
      assertFalse(reqs[8].isActive());
      assertTrue(reqs[7].isActive());
      testCrawlPriorityComparator(reqs);
    }

    public void testCrawlPriorityComparator2() {
      ConfigurationUtil.setFromArgs(CrawlManagerImpl.PARAM_RESTART_AFTER_CRASH,
          "false");
      CrawlReq[] reqs = makeReqs(8);
      setReq(reqs[0], 1, Crawler.STATUS_WINDOW_CLOSED, 9999, 9999);
      setReq(reqs[1], 1, 0, 5000, 5000);
      setReq(reqs[2], 0, Crawler.STATUS_WINDOW_CLOSED, -1, 2000);
      setReq(reqs[3], 0, Crawler.STATUS_WINDOW_CLOSED, 1000, 1000);
      setReq(reqs[4], 0, 0, -1, 500);
      setReq(reqs[5], 0, 0, 123, -1);
      setReq(reqs[6], 0, 0, 123, 456);
      setReq(reqs[7], 0, Crawler.STATUS_RUNNING_AT_CRASH, 1000, 1000);
      testCrawlPriorityComparator(reqs);
    }

    void setAuCreationTime(CrawlReq req, long time) {
      MockAuState aus = (MockAuState)AuUtil.getAuState(req.getAu());
      aus.setAuCreationTime(time);
    }

    public void testCrawlPriorityComparatorCreationOrder() {
      ConfigurationUtil.setFromArgs(CrawlManagerImpl.PARAM_RESTART_AFTER_CRASH,
          "false",
          CrawlManagerImpl.PARAM_CRAWL_ORDER,
          "CreationDate");
      CrawlReq[] reqs = makeReqs(8);
      setReq(reqs[0], 0, Crawler.STATUS_WINDOW_CLOSED, 1001, 1001);
      setReq(reqs[1], 0, Crawler.STATUS_RUNNING_AT_CRASH, 1000, 1000);
      setReq(reqs[2], 0, 0, 123, 456);
      setReq(reqs[3], 0, 0, 123, -1);
      setReq(reqs[4], 0, 0, -1, 500);
      setReq(reqs[5], 0, 0, -1, 2000);
      setReq(reqs[6], 0, 0, 5000, 5000);
      setReq(reqs[7], 0, 0, 9999, 9999);
      for (int ix = 0; ix < reqs.length; ix++) {
        setAuCreationTime(reqs[ix], 9990 + ix);
      }
      testCrawlPriorityComparator(reqs);
    }

    void registerAus(MockArchivalUnit[] aus) {
      List lst = ListUtil.fromArray(aus);
      List<MockArchivalUnit> rand = CollectionUtil.randomPermutation(lst);
      for (MockArchivalUnit mau : rand) {
        PluginTestUtil.registerArchivalUnit(plugin, mau);
      }
    }

    CrawlRateLimiter getCrl(Crawler c) {
      return crawlManager.getCrawlRateLimiter(c);
    }

    public void testOdcQueue() throws Exception {
      Properties p = new Properties();
      p.put(CrawlManagerImpl.PARAM_START_CRAWLS_INTERVAL, "-1");
      p.put(CrawlManagerImpl.PARAM_SHARED_QUEUE_MAX, "4");
      p.put(CrawlManagerImpl.PARAM_UNSHARED_QUEUE_MAX, "3");
      p.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_MAX, "3");
      p.put(CrawlManagerImpl.PARAM_FAVOR_UNSHARED_RATE_THREADS, "1");
      p.put(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AUID_MAP, "auNever,-10000");
      theDaemon.setAusStarted(true);
      ConfigurationUtil.addFromProps(p);
      crawlManager.startService();

      MockArchivalUnit[] aus = makeMockAus(15);
      registerAus(aus);
      MockArchivalUnit auPri = newMockArchivalUnit("auPri");
      setAu(auPri, 0, 9999, 9999);
      MockArchivalUnit auNever = newMockArchivalUnit("auNever");
      setAu(auNever, 0, 1, 2);
      registerAus(new MockArchivalUnit[] { auNever });
      setAu(aus[0], Crawler.STATUS_WINDOW_CLOSED, -1, 2000);
      setAu(aus[1], Crawler.STATUS_WINDOW_CLOSED, 1000, 1000);
      setAu(aus[2], 0, -1, 1000);
      setAu(aus[3], 0, 123, -1);
      setAu(aus[4], 0, 123, 456);

      setAu(aus[5], Crawler.STATUS_WINDOW_CLOSED, -1, 2001, "foo");
      setAu(aus[6], Crawler.STATUS_WINDOW_CLOSED, 1001, 1001, "foo");
      setAu(aus[7], 0, -1, 1001, "foo");
      setAu(aus[8], 0, 124, -1, "foo");		// repair
      setAu(aus[9], 0, 124, 457, "foo");	// repair

      setAu(aus[10], Crawler.STATUS_WINDOW_CLOSED, -1, 2002, "bar");
      setAu(aus[11], Crawler.STATUS_WINDOW_CLOSED, 1002, 1002, "bar");
      setAu(aus[12], 0, -1, 1002, "bar");
      setAu(aus[13], 0, 125, -1, "bar");
      setAu(aus[14], 0, 125, 458, "bar");	// repair

      assertEquals(0, crawlManager.rebuildCount);
      assertEquals(aus[5], crawlManager.nextReq().getAu());
      assertEquals(1, crawlManager.rebuildCount);
      MockCrawler cr5 = crawlManager.addToRunningRateKeys(aus[5]);

      // Should fail to assign a crl to another nc crawl in pool foo
      try {
        crawlManager.addToRunningRateKeys(aus[8]);
        fail("Attempt to add new content crawler in full pool should throw");
      } catch (IllegalStateException e) {
      }
      // Repair crawl in pool foo should get same crl as nc crawl
      MockCrawler cr8 = crawlManager.addToRunningRateKeys(aus[8], false);
      CrawlRateLimiter crl = getCrl(cr8);
      assertNotNull(crl);
      assertSame(crl, getCrl(cr5));
      crawlManager.delFromRunningRateKeys(aus[8]);

      // Add a repair in pool bar
      MockCrawler cr14 = crawlManager.addToRunningRateKeys(aus[14], false);

      assertEquals(aus[10], crawlManager.nextReq().getAu());
      MockCrawler cr10 = crawlManager.addToRunningRateKeys(aus[10]);
      assertNotNull(getCrl(cr10));
      assertSame(getCrl(cr10), getCrl(cr14));

      assertEquals(aus[0], crawlManager.nextReq().getAu());
      assertEquals(aus[1], crawlManager.nextReq().getAu());
      crawlManager.delFromRunningRateKeys(aus[10]);
      assertEquals(aus[11], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[11]);
      aus[5].setShouldCrawlForNewContent(false);
      aus[10].setShouldCrawlForNewContent(false);
      aus[0].setShouldCrawlForNewContent(false);
      aus[1].setShouldCrawlForNewContent(false);
      aus[11].setShouldCrawlForNewContent(false);
      assertEquals(aus[2], crawlManager.nextReq().getAu());
      crawlManager.delFromRunningRateKeys(aus[5]);
      assertEquals(aus[6], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[6]);
      aus[2].setShouldCrawlForNewContent(false);
      aus[6].setShouldCrawlForNewContent(false);
      assertEquals(1, crawlManager.rebuildCount);
      assertEquals(aus[3], crawlManager.nextReq().getAu());
      assertEquals(2, crawlManager.rebuildCount);
      assertEquals(aus[4], crawlManager.nextReq().getAu());
      aus[3].setShouldCrawlForNewContent(false);
      aus[4].setShouldCrawlForNewContent(false);
      assertEquals(null, crawlManager.nextReq());
      crawlManager.delFromRunningRateKeys(aus[11]);
      assertEquals(aus[12], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[12]);
      assertEquals(null, crawlManager.nextReq());
      crawlManager.delFromRunningRateKeys(aus[12]);
      crawlManager.delFromRunningRateKeys(aus[6]);
      PluginTestUtil.registerArchivalUnit(plugin, auPri);
      assertEquals(aus[7], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[7]);
      aus[12].setShouldCrawlForNewContent(false);
      aus[7].setShouldCrawlForNewContent(false);
      auPri.setShouldCrawlForNewContent(false);
      crawlManager.startNewContentCrawl(auPri, 1, null);
      assertEquals(auPri, crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(auPri);
      auPri.setShouldCrawlForNewContent(false);
      assertEquals(aus[13], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[13]);
      assertEquals(null, crawlManager.nextReq());
    }

    public void testOdcQueueWithConcurrentPool() throws Exception {
      Properties p = new Properties();
      p.put(CrawlManagerImpl.PARAM_START_CRAWLS_INTERVAL, "-1");
      p.put(CrawlManagerImpl.PARAM_SHARED_QUEUE_MAX, "4");
      p.put(CrawlManagerImpl.PARAM_UNSHARED_QUEUE_MAX, "3");
      p.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_MAX, "3");
      p.put(CrawlManagerImpl.PARAM_FAVOR_UNSHARED_RATE_THREADS, "1");
      p.put(CrawlManagerImpl.PARAM_CONCURRENT_CRAWL_LIMIT_MAP, "foo,2;bar,3");
      theDaemon.setAusStarted(true);
      ConfigurationUtil.addFromProps(p);
      crawlManager.startService();

      MockArchivalUnit[] aus = makeMockAus(15);
      registerAus(aus);
      setAu(aus[0], 0, 0, 2000);
      setAu(aus[1], 0, 0, 4000);
      setAu(aus[2], 0, 0, 6000);
      setAu(aus[3], 0, 0, 8000);
      setAu(aus[4], 0, 0, 10000);

      setAu(aus[5], 0, 0, 2002, "foo");
      setAu(aus[6], 0, 0, 2004, "foo");
      setAu(aus[7], 0, 0, 2006, "foo");
      setAu(aus[8], 0, 0, 4002, "foo");	// repair
      setAu(aus[9], 0, 0, 4004, "foo");	// repair

      setAu(aus[10], 0, 0, 2001, "bar");
      setAu(aus[11], 0, 0, 2003, "bar");
      setAu(aus[12], 0, 0, 2005, "bar");
      setAu(aus[13], 0, 0, 4001, "bar");
      setAu(aus[14], 0, 0, 4003, "bar"); // repair

      assertFalse(crawlManager.isWorthRebuildingQueue());
      assertEquals(aus[10], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[10]);
      aus[10].setShouldCrawlForNewContent(false);
      assertTrue(crawlManager.isWorthRebuildingQueue());
      assertEquals(aus[5], crawlManager.nextReq().getAu());
      MockCrawler cr5 = crawlManager.addToRunningRateKeys(aus[5]);

      assertEquals(aus[0], crawlManager.nextReq().getAu());
      aus[0].setShouldCrawlForNewContent(false);
      assertEquals(aus[11], crawlManager.nextReq().getAu());
      MockCrawler cr11 = crawlManager.addToRunningRateKeys(aus[11]);
      aus[11].setShouldCrawlForNewContent(false);
      assertNotSame(getCrl(cr5), getCrl(cr11));

      // Repair crawl in foo should get different crl
      MockCrawler cr8 = crawlManager.addToRunningRateKeys(aus[8], false);
      assertNotSame(getCrl(cr5), getCrl(cr8));

      // Next nc in foo should get same crl as repair
      assertEquals(aus[6], crawlManager.nextReq().getAu());
      MockCrawler cr6 = crawlManager.addToRunningRateKeys(aus[6]);
      aus[6].setShouldCrawlForNewContent(false);
      assertSame(getCrl(cr8), getCrl(cr6));

      assertEquals(aus[12], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[12]);
      aus[12].setShouldCrawlForNewContent(false);
      assertEquals(aus[1], crawlManager.nextReq().getAu());
      aus[1].setShouldCrawlForNewContent(false);
      crawlManager.delFromRunningRateKeys(aus[5]);
      assertEquals(aus[7], crawlManager.nextReq().getAu());
      crawlManager.addToRunningRateKeys(aus[7]);
      aus[7].setShouldCrawlForNewContent(false);
      assertEquals(aus[2], crawlManager.nextReq().getAu());
      aus[2].setShouldCrawlForNewContent(false);
      assertEquals(aus[3], crawlManager.nextReq().getAu());
      aus[3].setShouldCrawlForNewContent(false);
      crawlManager.delFromRunningRateKeys(aus[10]);
      assertEquals(aus[13], crawlManager.nextReq().getAu());
      assertEquals(aus[14], crawlManager.nextReq().getAu());
      assertEquals(aus[4], crawlManager.nextReq().getAu());
      assertNull(crawlManager.nextReq());
      assertFalse(crawlManager.isWorthRebuildingQueue());

    }

    public void testCrawlPriorityAuidPatterns() {
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AUID_MAP,
          "foo(4|5),3;bar,5;baz,-1");
      MockArchivalUnit mau1 = new MockArchivalUnit(new MockPlugin(theDaemon));
      mau1.setAuId("other");
      CrawlReq req = new CrawlReq(mau1, new CrawlerStatus(mau1,
	  mau1.getStartUrls(), null));
      crawlManager.setReqPriority(req);
      assertEquals(0, req.getPriority());
      mau1.setAuId("foo4");
      crawlManager.setReqPriority(req);
      assertEquals(3, req.getPriority());
      mau1.setAuId("foo5");
      crawlManager.setReqPriority(req);
      assertEquals(3, req.getPriority());
      mau1.setAuId("x~ybar~z");
      crawlManager.setReqPriority(req);
      assertEquals(5, req.getPriority());
      mau1.setAuId("bazab");
      crawlManager.setReqPriority(req);
      assertEquals(-1, req.getPriority());

      // Remove param, ensure priority map gets removed
      ConfigurationUtil.resetConfig();
      mau1.setAuId("foo4");
      req.setPriority(-3);
      crawlManager.setReqPriority(req);
      assertEquals(-3, req.getPriority());

    }

    public void testCrawlPriorityAuPatterns() throws Exception {
      MockArchivalUnit mau1 = new MockArchivalUnit(new MockPlugin(theDaemon));
      mau1.setAuId("auid1");
      MockArchivalUnit mau2 = new MockArchivalUnit(new MockPlugin(theDaemon));
      mau2.setAuId("auid2");
      MockArchivalUnit mau3 = new MockArchivalUnit(new MockPlugin(theDaemon));
      mau3.setAuId("auid3");

      Tdb tdb = new Tdb();

      Properties tprops = new Properties();
      tprops.put("journalTitle", "jtitle");
      tprops.put("plugin", "Plug1");
      tprops.put("param.1.key", "volume");

      tprops.put("title", "title 1");
      tprops.put("param.1.value", "vol_1");
      tprops.put("attributes.year", "2001");
      mau1.setTdbAu(tdb.addTdbAuFromProperties(tprops));

      tprops.put("title", "title 2");
      tprops.put("param.1.value", "vol_2");
      tprops.put("attributes.year", "2002");
      mau2.setTdbAu(tdb.addTdbAuFromProperties(tprops));

      tprops.put("title", "title 3");
      tprops.put("param.1.value", "vol_3");
      tprops.put("attributes.year", "2003");
      mau3.setTdbAu(tdb.addTdbAuFromProperties(tprops));

      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AU_MAP,
          "[RE:isMatchRe(tdbAu/params/volume,'vol_1')],6;" +
              "[RE:isMatchRe(tdbAu/params/volume,'vol_(1|2)')],3;");

      CrawlReq req1 = new CrawlReq(mau1,
	  new CrawlerStatus(mau1, mau1.getStartUrls(), null));
      CrawlReq req2 = new CrawlReq(mau2,
	  new CrawlerStatus(mau2, mau2.getStartUrls(), null));
      CrawlReq req3 = new CrawlReq(mau3,
	  new CrawlerStatus(mau3, mau3.getStartUrls(), null));
      crawlManager.setReqPriority(req1);
      crawlManager.setReqPriority(req2);
      crawlManager.setReqPriority(req3);
      assertEquals(6, req1.getPriority());
      assertEquals(3, req2.getPriority());
      assertEquals(0, req3.getPriority());

      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_PRIORITY_AU_MAP,
          "[tdbAu/year <= '2002'],10;" +
              "[tdbAu/year >= '2002'],-20000;");

      crawlManager.setReqPriority(req1);
      crawlManager.setReqPriority(req2);
      crawlManager.setReqPriority(req3);
      assertEquals(10, req1.getPriority());
      assertEquals(10, req2.getPriority());
      assertEquals(-20000, req3.getPriority());

      // Remove param, ensure priority map gets removed
      ConfigurationUtil.resetConfig();
      req1.setPriority(0);
      crawlManager.setReqPriority(req1);
      assertEquals(0, req1.getPriority());
    }

    public void testCrawlPoolSizeMap() {
      // map not configured
      assertEquals(1, crawlManager.getCrawlPoolSize("nopool"));
      assertEquals(1, crawlManager.getCrawlPoolSize("foopool"));
      ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CONCURRENT_CRAWL_LIMIT_MAP,
          "foopool,2;barpool,4");
      assertEquals(1, crawlManager.getCrawlPoolSize("nopool"));
      assertEquals(2, crawlManager.getCrawlPoolSize("foopool"));
      assertEquals(4, crawlManager.getCrawlPoolSize("barpool"));

      // Remove param, ensure map reverts to empty
      ConfigurationUtil.resetConfig();
      assertEquals(1, crawlManager.getCrawlPoolSize("nopool"));
      assertEquals(1, crawlManager.getCrawlPoolSize("foopool"));
    }

    public void testOdcCrawlStarter() {
      int num = 15;
      int tot = num+1;
      int rauix = tot-1;
      int nthreads = 3;
      Properties p = new Properties();
      p.put(CrawlManagerImpl.PARAM_USE_ODC, "true");
      p.put(CrawlManagerImpl.PARAM_CRAWLER_THREAD_POOL_MAX, ""+nthreads);
      p.put(CrawlManagerImpl.PARAM_MAX_NEW_CONTENT_RATE, "1/1w");
      p.put(CrawlManagerImpl.PARAM_NEW_CONTENT_START_RATE, (tot*2)+"/1h");
      p.put(CrawlManagerImpl.PARAM_START_CRAWLS_INITIAL_DELAY, "10");
      p.put(CrawlManagerImpl.PARAM_START_CRAWLS_INTERVAL, "10");
      p.put(CrawlManagerImpl.PARAM_QUEUE_RECALC_AFTER_NEW_AU, "200");
      p.put(CrawlManagerImpl.PARAM_QUEUE_EMPTY_SLEEP, "500");

      p.put(CrawlManagerImpl.PARAM_SHARED_QUEUE_MAX, "10");
      p.put(CrawlManagerImpl.PARAM_UNSHARED_QUEUE_MAX, "10");
      p.put(CrawlManagerImpl.PARAM_FAVOR_UNSHARED_RATE_THREADS, "1");

      ConfigurationUtil.addFromProps(p);
      crawlManager.ausStartedSem = new OneShotSemaphore();
      crawlManager.startService();

//       Uncomment to stress startup logic
//       TimerUtil.guaranteedSleep(1000);

      MockArchivalUnit[] aus = makeMockAus(num);
      RegistryArchivalUnit rau = makeRegistryAu();

      setAu(aus[0], Crawler.STATUS_WINDOW_CLOSED, -1, 2000);
      setAu(aus[1], Crawler.STATUS_WINDOW_CLOSED, 1000, 1000);
      setAu(aus[2], 0, -1, -2);
      setAu(aus[3], 0, 123, -1);
      setAu(aus[4], 0, 123, 456);

      setAu(aus[5], Crawler.STATUS_WINDOW_CLOSED, -1, 2001, "foo");
      setAu(aus[6], Crawler.STATUS_WINDOW_CLOSED, 1001, 1001, "foo");
      setAu(aus[7], 0, -1, 1001, "foo");
      setAu(aus[8], 0, 124, -1, "foo");
      setAu(aus[9], 0, 124, 457, "foo");

      setAu(aus[10], Crawler.STATUS_WINDOW_CLOSED, -1, 2002, "bar");
      setAu(aus[11], Crawler.STATUS_WINDOW_CLOSED, 1002, 1002, "bar");
      setAu(aus[12], 0, -1, 1002, "bar");
      setAu(aus[13], 0, 125, -1, "bar");
      setAu(aus[14], 0, 125, 458, "bar");

      assertEmpty(pluginMgr.getAllAus());
      registerAus(aus);
      pluginMgr.addRegistryAu(rau);

      HangingCrawler[] crawler = new HangingCrawler[tot];
      TestCrawlCB[] cb = new TestCrawlCB[tot];
      SimpleBinarySemaphore[] startSem = new SimpleBinarySemaphore[tot];
      SimpleBinarySemaphore[] endSem = new SimpleBinarySemaphore[tot];
      SimpleBinarySemaphore[] finishedSem = new SimpleBinarySemaphore[tot];
      long startTime[] = new long[tot];
      crawlManager.recordExecute(true);

      // Create AUs and build the pieces necessary for the crawl starter to
      // get the list of AUs from the PluginManager.  The crawl starter
      // should then shortly start poolMax of them.  We only check that it
      // called startNewContentCrawl() on each.
      for (int ix = 0; ix < tot; ix++) {
        startSem[ix] = new SimpleBinarySemaphore();
        endSem[ix] = new SimpleBinarySemaphore();
        finishedSem[ix] = new SimpleBinarySemaphore();
        if (ix == rauix) {
          // Last one is a RegistryArchivalUnit crawl
          crawler[ix] = new HangingCrawler("testOdcCrawlStarter(reg au) " + ix,
              startSem[ix], endSem[ix]);
          setupAuToCrawl(rau, crawler[ix]);
        } else {
          crawler[ix] = new HangingCrawler("testOdcCrawlStarter " + ix,
              startSem[ix], endSem[ix]);
          aus[ix].setCrawlRule(rule);
          aus[ix].setStartUrls(ListUtil.list("blah"));
          setupAuToCrawl(aus[ix], crawler[ix]);
        }
        semToGive(endSem[ix]);
      }

      theDaemon.setAusStarted(true);
      // now let the crawl starter proceed
      crawlManager.ausStartedSem.fill();
      // PluginTestUtil.registerArchivalUnit() doesn't cause Create AuEvent
      // to be signalled so must ensure CrawlManagerImpl rebuilds queue
      // promptly
      crawlManager.rebuildQueueSoon();
      // Ensure they all got queued
      List exe = Collections.EMPTY_LIST;
      Interrupter intr = interruptMeIn(TIMEOUT_SHOULDNT * 2, true);

      int pokeOrder[] =
          { -1, -1, -1, 0, 10, 1, 5, 6, 7, 8, 9, 3,  4, 11, 12, 13, 14};
      int expStartOrder[] =
          {  5, 10,  0, 1, 11, 2, 6, 7, 8, 9, 3, 4, -1, 12, 13, 14, -1};

      // wait for first nthreads to start
      // check correct three (unordered)
      // repeat until done
      //    let one finish
      //    correct next one starts

      List expExec = new ArrayList();
      List expRun = new ArrayList();
      List expEnd = new ArrayList();

      for (int ix = 0; ix < tot; ix++) {
        int poke = pokeOrder[ix];
        if (poke >= 0) {
          endSem[poke].give();
        }
        int wait = expStartOrder[ix];
        if (wait >= 0) {
          assertTrue(didntMsg("start("+ix+") = " + wait, TIMEOUT_SHOULDNT),
              startSem[wait].take(TIMEOUT_SHOULDNT));
          expExec.add(crawler[wait]);
        }
        if (ix >= nthreads - 1) {
          while (exe.size() < expExec.size()) {
            crawlManager.waitExecuted();
            assertFalse("Expected " + expExec +
                    ", but timed out and was " + exe,
                intr.did());
            exe = crawlManager.getExecuted();
          }
          assertEquals(expExec, crawlersOf(exe));
        }
      }
    }


  }

  RegistryPlugin regplugin;

  RegistryArchivalUnit makeRegistryAu() {
    if (regplugin == null) {
      regplugin = new RegistryPlugin();
      regplugin.initPlugin(theDaemon);
    }
    RegistryArchivalUnit res = new MyRegistryArchivalUnit(regplugin);
    AuTestUtil.setUpMockAus(mau);

    return res;
  }

  class MyRegistryArchivalUnit extends RegistryArchivalUnit {
    MyRegistryArchivalUnit(RegistryPlugin plugin) {
      super(plugin);
      try {
        setConfiguration(ConfigurationUtil.fromArgs(ConfigParamDescr.BASE_URL.getKey(), "http://foo.bar/"));
      } catch (ArchivalUnit.ConfigurationException e) {
        throw new RuntimeException("setConfiguration()", e);
      }
    }
  }

  class MyPluginManager extends PluginManager {
    private List registryAus;
    public Collection getAllRegistryAus() {
      if (registryAus == null) {
        return super.getAllRegistryAus();
      } else {
        return registryAus;
      }
    }
    public void addRegistryAu(ArchivalUnit au) {
      if (registryAus == null) {
        registryAus = new ArrayList();
      }
      registryAus.add(au);
    }

    @Override
    protected void raiseAlert(Alert alert, String msg) {
    }
  }

  private static class TestCrawlCB  {
    SimpleBinarySemaphore sem;
    boolean called = false;
    Object cookie;
    boolean success;

    public TestCrawlCB() {
      this(null);
    }

    public TestCrawlCB(SimpleBinarySemaphore sem) {
      this.sem = sem;
    }

    public void signalCrawlAttemptCompleted(boolean success,
        Object cookie,
        CrawlerStatus status) {
      log.info("entering signal crawl attempt complete");
      this.success = success;
      called = true;
      this.cookie = cookie;
      if (sem != null) {
        sem.give();
      }
      log.info("exiting signal crawl attempt complte.");
    }

    public boolean wasSuccessful() {
      return success;
    }

//    public void signalCrawlSuspended(Object cookie) {
//      this.cookie = cookie;
//    }

    public Object getCookie() {
      return cookie;
    }

    public boolean wasTriggered() {
      return called;
    }
  }

  private static class TestableCrawlManagerImpl extends CrawlManagerImpl {
    private MockCrawler mockCrawler;
    private HashMap auCrawlers = new HashMap();
    private boolean recordExecute = false;
    private List executed = new ArrayList();
    private SimpleBinarySemaphore executedSem = new SimpleBinarySemaphore();
    private OneShotSemaphore ausStartedSem;
    private boolean isInternalAu = false;
    private Map<ArchivalUnit,Crawler> auCrawlerMap =
        new HashMap<ArchivalUnit,Crawler>();

    TestableCrawlManagerImpl(PluginManager pmgr) {
      pluginMgr = pmgr;
    }

    protected Crawler makeFollowLinkCrawler(ArchivalUnit au,
	      CrawlerStatus crawlerStatus) {
      MockCrawler crawler = getCrawler(au);
      crawler.setAu(au);
      crawler.setUrls(au.getStartUrls());
      crawler.setFollowLinks(true);
      crawler.setType(Crawler.Type.NEW_CONTENT);
      crawler.setIsWholeAU(true);
      crawlerStatus.setStartUrls(au.getStartUrls());
      crawlerStatus.setType(Crawler.Type.NEW_CONTENT.name());
      crawler.setCrawlerStatus(crawlerStatus);
      return crawler;
    }

    protected Crawler makeRepairCrawler(ArchivalUnit au,
        Collection repairUrls) {
      MockCrawler crawler = getCrawler(au);
      crawler.setAu(au);
      crawler.setUrls(repairUrls);
      crawler.setFollowLinks(false);
      crawler.setType(Crawler.Type.REPAIR);
      crawler.setIsWholeAU(false);
      return crawler;
    }

    protected void execute(Runnable run) throws InterruptedException {
      if (recordExecute) {
        executed.add(run);
        executedSem.give();
      }
      super.execute(run);
    }

    @Override
    void waitUntilAusStarted() throws InterruptedException {
      if (ausStartedSem != null) {
        ausStartedSem.waitFull(Deadline.MAX);
      } else {
        super.waitUntilAusStarted();
      }
    }

    @Override
    boolean areAusStarted() {
      if (ausStartedSem != null) {
        return ausStartedSem.isFull();
      } else {
        return super.areAusStarted();
      }
    }

    MockCrawler addToRunningRateKeys(ArchivalUnit au) {
      return addToRunningRateKeys(au, true);
    }

    MockCrawler addToRunningRateKeys(ArchivalUnit au, boolean isWholeAU) {
      MockCrawler mc = new MockCrawler();
      mc.setAu(au);
      mc.setIsWholeAU(isWholeAU);
      return addToRunningRateKeys(au, mc);
    }

    MockCrawler addToRunningRateKeys(ArchivalUnit au, MockCrawler mc) {
      mc.setAu(au);
      addToRunningCrawls(au, mc);
      auCrawlerMap.put(au, mc);
      highPriorityCrawlRequests.remove(au.getAuId());
      return mc;
    }

    void delFromRunningRateKeys(ArchivalUnit au) {
      Crawler crawler = auCrawlerMap.get(au);
      if (crawler != null) {
        removeFromRunningCrawls(crawler);
        auCrawlerMap.remove(au);
      }
    }

    int rebuildCount = 0;
    void rebuildCrawlQueue() {
      rebuildCount++;
      super.rebuildCrawlQueue();
    }

    public void recordExecute(boolean val) {
      recordExecute = val;
    }

    public void waitExecuted() {
      executedSem.take();
    }

    public List getExecuted() {
      return executed;
    }

    private void setTestCrawler(MockCrawler crawler) {
      this.mockCrawler = crawler;
    }

    private MockCrawler getCrawler(ArchivalUnit au) {
      MockCrawler crawler = (MockCrawler)auCrawlers.get(au);
      if (crawler != null) {
        return crawler;
      }
      return mockCrawler;
    }

    private void setTestCrawler(ArchivalUnit au, MockCrawler crawler) {
      auCrawlers.put(au, crawler);
    }

    protected void instrumentBeforeStartRateLimiterEvent(Crawler crawler) {
      if (crawler instanceof MockCrawler) {
        ((MockCrawler)crawler).setStartTime(TimeBase.nowMs());
      }
    }

    CrawlPriorityComparator cmprtr() {
      return new CrawlPriorityComparator();
    }

    void setInternalAu(boolean val) {
      isInternalAu = val;
    }

    @Override
    protected boolean isInternalAu(ArchivalUnit au) {
      return isInternalAu || super.isInternalAu(au);
    }

  }

  private static class ThrowingCrawler extends MockCrawler {
    RuntimeException e = null;
    public ThrowingCrawler(RuntimeException e) {
      this.e = e;
      setType(Type.NEW_CONTENT);
    }

    public boolean doCrawl() {
      if (false) return super.doCrawl();
      throw e;
    }
  }

  private static class HangingCrawler extends MockCrawler {
    String name;
    SimpleBinarySemaphore sem1;
    SimpleBinarySemaphore sem2;

    public HangingCrawler(String name,
        SimpleBinarySemaphore sem1,
        SimpleBinarySemaphore sem2) {
      this.name = name;
      this.sem1 = sem1;
      this.sem2 = sem2;
      setType(Type.NEW_CONTENT);
    }

    public boolean doCrawl() {
      if (false) return super.doCrawl();
      if (sem1 != null) sem1.give();
      if (sem2 != null) sem2.take();
      return false;
    }

    public String toString() {
      return "HangingCrawler " + name;
    }
  }

  private static class SignalDoCrawlCrawler extends MockCrawler {
    SimpleBinarySemaphore sem;
    public SignalDoCrawlCrawler(SimpleBinarySemaphore sem) {
      this.sem = sem;
      setType(Type.NEW_CONTENT);
    }

    public boolean doCrawl() {
      if (false) return super.doCrawl();
      sem.give();
      return true;
    }
  }

  private static class CloseWindowCrawler extends MockCrawler {
    public CloseWindowCrawler() {
      setType(Type.NEW_CONTENT);
    }

    public boolean doCrawl() {
      super.doCrawl();
      MockArchivalUnit au = (MockArchivalUnit)getAu();
      au.setCrawlWindow(new ClosedCrawlWindow());
      return false;
    }
  }

  private static class ClosedCrawlWindow implements CrawlWindow {
    public boolean canCrawl() {
      return false;
    }
    public boolean canCrawl(Date x) {
      return false;
    }
  }

  private static class AuEventCrawler extends MockCrawler {
    List<List<String>> urls = Collections.EMPTY_LIST;

    public AuEventCrawler(ArchivalUnit au) {
      setStatus(new CrawlerStatus(au, ListUtil.list("foo"), "New content"));
      setType(Type.NEW_CONTENT);
    }

    void setMimes(List<List<String>> urls) {
      this.urls = urls;
    }

    public boolean doCrawl() {
      for (List<String> pair : urls) {
        getCrawlerStatus().signalUrlFetched(pair.get(0));
        String mime = HeaderUtil.getMimeTypeFromContentType(pair.get(1));
        getCrawlerStatus().signalMimeTypeOfUrl(mime, pair.get(0));
      }
      if (super.doCrawl()) {
        getCrawlerStatus().setCrawlStatus(Crawler.STATUS_SUCCESSFUL);
        return true;
      } else {
        getCrawlerStatus().setCrawlStatus(Crawler.STATUS_ERROR);
        return false;
      }
    }
  }


  private static class ThrowingAU extends NullPlugin.ArchivalUnit {
    public CachedUrlSet makeCachedUrlSet(CachedUrlSetSpec spec) {
      throw new ExpectedRuntimeException("I throw");
    }

    public CachedUrl makeCachedUrl(CachedUrlSet owner, String url) {
      throw new ExpectedRuntimeException("I throw");
    }

    public UrlCacher makeUrlCacher(CachedUrlSet owner, String url) {
      throw new ExpectedRuntimeException("I throw");
    }

    public CachedUrlSet getAuCachedUrlSet() {
      throw new ExpectedRuntimeException("I throw");
    }

    public boolean shouldBeCached(String url) {
      throw new ExpectedRuntimeException("I throw");
    }

    public Collection getUrlStems() {
      throw new ExpectedRuntimeException("I throw");
    }

    public Plugin getPlugin() {
      throw new ExpectedRuntimeException("I throw");
    }
  }

  public static Test suite() {
    return variantSuites(new Class[] {TestsWithAutoStart.class,
        TestsWithoutAutoStart.class});
  }

  public static void main(String[] argv) {
    String[] testCaseList = { TestCrawlManagerImpl.class.getName()};
    junit.textui.TestRunner.main(testCaseList);
  }
}
