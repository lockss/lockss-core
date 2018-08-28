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

package org.lockss.plugin.base;

import java.io.*;
import java.net.MalformedURLException;
import java.util.*;

import org.lockss.crawler.*;
import org.lockss.crawler.PermissionRecord.PermissionStatus;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.repository.OldLockssRepositoryImpl;
import org.lockss.state.AuState;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.net.IPAddr;

public class TestSimpleUrlConsumer extends LockssTestCase {

  protected static Logger logger = Logger.getLogger();


  private MockLockssDaemon theDaemon;
  private MockPlugin plugin;
  private MockHistoryRepository histRepo = new MockHistoryRepository();
  private MockArchivalUnit mau;
  private MockAuState aus;
  private MySimpleUrlConsumerFactory ucfact = new MySimpleUrlConsumerFactory();

  private static final String TEST_URL = "http://www.example.com/testDir/leaf1";

  public void setUp() throws Exception {
    super.setUp();

    String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.setFromArgs(OldLockssRepositoryImpl.PARAM_CACHE_LOCATION,
				  tempDirPath);
    theDaemon = getMockLockssDaemon();

    mau = new MockArchivalUnit();

    plugin = new MockPlugin();
    plugin.initPlugin(theDaemon);
    mau.setPlugin(plugin);

    histRepo = new MockHistoryRepository();
    theDaemon.setHistoryRepository(histRepo, mau);
    aus = new MockAuState(mau);
    histRepo.setAuState(aus);
  }

  // Ensure SimpleUrlConsumer uses the CrawlFacade's makeUrlCacher()
  public void testUrlCacherCreation() throws IOException {
    mau.addUrl(TEST_URL);
    TestableBaseCrawler crawler = new TestableBaseCrawler(mau, aus);
    Crawler.CrawlerFacade crawlFacade =
      new BaseCrawler.BaseCrawlerFacade(crawler);
    LockssWatchdog wdog = new MockLockssWatchdog();
    crawler.setWatchdog(wdog);
    FetchedUrlData fud = 
      new FetchedUrlData(TEST_URL, TEST_URL,
			 new StringInputStream("test stream"),
			 new CIProperties(), null, null/*this*/);
    MySimpleUrlConsumer con =
      (MySimpleUrlConsumer)ucfact.createUrlConsumer(crawlFacade, fud);
    con.consume();
    UrlCacher uc = con.getUrlCacher();
    assertSame(wdog, uc.getWatchdog());
  }


  private class MyMockLockssUrlConnection extends MockLockssUrlConnection {
    String proxyHost = null;
    int proxyPort = -1;
    IPAddr localAddr = null;
    String username;
    String password;
    String cpolicy;
    String headerCharset;
    List<List<String>> cookies = new ArrayList<List<String>>();

    public MyMockLockssUrlConnection() throws IOException {
      super();
    }

    public MyMockLockssUrlConnection(String url) throws IOException {
      super(url);
    }

    public void setProxy(String host, int port) {
      proxyHost = host;
      proxyPort = port;
    }

    public void setLocalAddress(IPAddr addr) {
      localAddr = addr;
    }

    public void setCredentials(String username, String password) {
      this.username = username;
      this.password = password;
    }

    public void setCookiePolicy(String policy) {
      cpolicy = policy;
    }

    String getCookiePolicy() {
      return cpolicy;
    }

    public void addCookie(String domain, String path,
			  String name, String value) {
      cookies.add(ListUtil.list(domain, path, name, value));
    }

    public List<List<String>> getCookies() {
      return cookies;
    }

    public void setHeaderCharset(String charset) {
      headerCharset = charset;
    }

  }

  private class ThrowingMockLockssUrlConnection extends MockLockssUrlConnection {
    IOException ex;

    public ThrowingMockLockssUrlConnection(IOException ex) throws IOException {
      super();
      this.ex = ex;
    }

    public void execute() throws IOException {
      throw ex;
    }
  }

  class MyStringInputStreamMarkNotSupported extends MyStringInputStream {
    public MyStringInputStreamMarkNotSupported(String str) {
      super(str);
    }

    public boolean markSupported() {
      return false;
    }
  }

  class MyStringInputStream extends StringInputStream {
    private boolean resetWasCalled = false;
    private boolean markWasCalled = false;
    private boolean closeWasCalled = false;
    private IOException resetEx;

    private int buffSize = -1;

    public MyStringInputStream(String str) {
      super(str);
    }

    /**
     * @param str String to read from
     * @param resetEx IOException to throw when reset is called
     *
     * Same as one arg constructor, but can provide an exception that is thrown
     * when reset is called
     */
    public MyStringInputStream(String str, IOException resetEx) {
      super(str);
      this.resetEx = resetEx;
    }

    public void reset() throws IOException {
      resetWasCalled = true;
      if (resetEx != null) {
        throw resetEx;
      }
      super.reset();
    }

    public boolean resetWasCalled() {
      return resetWasCalled;
    }

    public void mark(int buffSize) {
      markWasCalled = true;
      this.buffSize = buffSize;
      super.mark(buffSize);
    }

    public boolean markWasCalled() {
      return markWasCalled;
    }

    public int getMarkBufferSize() {
      return this.buffSize;
    }

    public void close() throws IOException {
      Exception ex = new Exception("Blah");
      logger.debug3("Close called on " + this, ex);
      closeWasCalled = true;
      super.close();
    }

    public boolean closeWasCalled() {
      return closeWasCalled;
    }

  }

  private static class MockPermissionMap extends PermissionMap {
    public MockPermissionMap() {
      super(new MockCrawler().new MockCrawlerFacade(),
	    new ArrayList(), new ArrayList(), null);
    }

    protected void putStatus(String permissionUrl, PermissionStatus status)
            throws MalformedURLException {
      super.createRecord(permissionUrl).setStatus(status);
    }

  }

  private class TestableBaseCrawler extends BaseCrawler {

    protected TestableBaseCrawler(ArchivalUnit au, AuState aus) {
      super(au, aus);
      crawlStatus = new MockCrawlStatus();
    }

    public Crawler.Type getType() {
      throw new UnsupportedOperationException("not implemented");
    }

    public String getTypeString() {
      return "Testable";
    }

    public boolean isWholeAU() {
      return true;
    }

    protected boolean doCrawl0() {
      return true;
    }
  }

  static class MySimpleUrlConsumer extends SimpleUrlConsumer {
    public MySimpleUrlConsumer(Crawler.CrawlerFacade crawlFacade,
			       FetchedUrlData fud){
      super(crawlFacade, fud);
    }

    UrlCacher getUrlCacher() {
      return cacher;
    }

  }

  static class MySimpleUrlConsumerFactory implements UrlConsumerFactory {
    public UrlConsumer createUrlConsumer(Crawler.CrawlerFacade crawlFacade,
					 FetchedUrlData fud) {
      return new MySimpleUrlConsumer(crawlFacade, fud);
    }
  }
}
