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
package org.lockss.ws.status;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.lockss.config.ConfigManager;
import org.lockss.crawler.*;
import org.lockss.config.Configuration;
import org.lockss.daemon.PluginException;
import org.lockss.extractor.ArticleMetadata;
import org.lockss.extractor.ArticleMetadataExtractor;
import org.lockss.extractor.MetadataTarget;
import org.lockss.plugin.*;
import org.lockss.plugin.simulated.SimulatedArchivalUnit;
import org.lockss.plugin.simulated.SimulatedContentGenerator;
import org.lockss.plugin.simulated.SimulatedDefinablePlugin;
import org.lockss.protocol.IdentityManager;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.ws.entities.CrawlWsResult;

/**
 * Test class for org.lockss.ws.status.DaemonStatusService
 * 
 * @author Fernando Garcia-Loygorri
 */
public class TestDaemonStatusService extends LockssTestCase {
  static Logger log = Logger.getLogger();
  static String TEST_LOCAL_IP = "127.1.2.3";

  private MockLockssDaemon theDaemon;
  private PluginManager pluginManager;
  private DaemonStatusServiceImpl service;
  private String tempDirPath;
  private SimulatedArchivalUnit sau0, sau1;
  private Plugin m_plug;

  public void setUp() throws Exception {
    super.setUp();

    tempDirPath = setUpDiskSpace();

    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_IP,
				  TEST_LOCAL_IP,
				  CrawlManagerImpl.PARAM_CRAWLER_ENABLED,
				  "true");

    theDaemon = getMockLockssDaemon();
    theDaemon.setDaemonInited(true);

    pluginManager = theDaemon.getPluginManager();
    pluginManager.startService();

    sau0 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin0.class,
	simAuConfig(tempDirPath + "0"));

    PluginTestUtil.crawlSimAu(sau0);

    sau1 = PluginTestUtil.createAndStartSimAu(MySimulatedPlugin1.class,
	simAuConfig(tempDirPath + "/1"));

    PluginTestUtil.crawlSimAu(sau1);

    theDaemon.getRemoteApi().startService();

    service = new DaemonStatusServiceImpl();

    m_plug = new MockPlugin(theDaemon);

  }

  CrawlManager startCrawlManager() {
    CrawlManagerImpl mgr = new CrawlManagerImpl();
    theDaemon.setCrawlManager(mgr);
    mgr.initService(theDaemon);
    mgr.startService();
    return mgr;
  }

  MockArchivalUnit newMockArchivalUnit(String auid) {
    MockArchivalUnit mau = new MockArchivalUnit(m_plug, auid);
    AuTestUtil.setUpMockAus(mau);
    PluginTestUtil.registerArchivalUnit(m_plug, mau);
    return mau;
  }

  public void testQueryCrawls() throws Exception {
    CrawlManager crawlMgr = startCrawlManager();
    ConfigurationUtil.addFromArgs(CrawlManagerImpl.PARAM_CRAWL_STARTER_ENABLED,
				  "false",
				  CrawlManagerImpl.PARAM_USE_ODC,
				  "true"); 

    ArchivalUnit sau2 =
      PluginTestUtil.createAndStartSimAu(MySimulatedPlugin1.class,
					 simAuConfig(tempDirPath + "/2"));
    CrawlReq req1 = new CrawlReq(sau1);
    req1.setPriority(8);
    req1.setRefetchDepth(1232);
    crawlMgr.startNewContentCrawl(req1);

    CrawlReq req2 = new CrawlReq(sau2);
    req2.setPriority(9);
    req2.setRefetchDepth(1231);
    crawlMgr.startNewContentCrawl(req2);


    String query = "select *";
    List<CrawlWsResult> crawls = service.queryCrawls(query);
    assertEquals(2, crawls.size());
    CrawlWsResult r1 = crawls.get(0);
    CrawlWsResult r2 = crawls.get(1);
    if (sau2.getAuId().equals(r1.getAuId())) {
      r1 = crawls.get(1);
      r2 = crawls.get(0);
    }      
    assertEquals(sau1.getAuId(), r1.getAuId());
    assertEquals(1232, (int)r1.getRefetchDepth());
    assertEquals(8, (int)r1.getPriority());
    assertEquals("Pending", r1.getCrawlStatus());

    assertEquals(sau2.getAuId(), r2.getAuId());
    assertEquals(1231, (int)r2.getRefetchDepth());
    assertEquals(9, (int)r2.getPriority());
    assertEquals("Pending", r2.getCrawlStatus());

    pluginManager.stopAu(sau1, AuEvent.forAu(sau1, AuEvent.Type.RestartDelete));
    pluginManager.stopAu(sau2, AuEvent.forAu(sau2, AuEvent.Type.Deactivate));

    List<CrawlWsResult> crawls2 = service.queryCrawls(query);
    assertEquals(1, crawls2.size());
    CrawlWsResult s1 = crawls2.get(0);
    assertEquals(sau1.getAuId(), s1.getAuId());
    assertEquals(1232, (int)s1.getRefetchDepth());
    assertEquals(8, (int)s1.getPriority());
    assertEquals("Inactive", s1.getCrawlStatus());
  }

  private Configuration simAuConfig(String rootPath) {
    Configuration conf = ConfigManager.newConfiguration();
    conf.put("root", rootPath);
    conf.put("depth", "2");
    conf.put("branch", "1");
    conf.put("numFiles", "3");
    conf.put("fileTypes", "" + (SimulatedContentGenerator.FILE_TYPE_PDF +
                                SimulatedContentGenerator.FILE_TYPE_HTML));
    conf.put("binFileSize", "7");
    return conf;
  }

  private static class MySubTreeArticleIteratorFactory
      implements ArticleIteratorFactory {
    String pat;
    public MySubTreeArticleIteratorFactory(String pat) {
      this.pat = pat;
    }
    
    /**
     * Create an Iterator that iterates through the AU's articles, pointing
     * to the appropriate CachedUrl of type mimeType for each, or to the
     * plugin's choice of CachedUrl if mimeType is null
     * @param au the ArchivalUnit to iterate through
     * @return the ArticleIterator
     */
    @Override
    public Iterator<ArticleFiles> createArticleIterator(
        ArchivalUnit au, MetadataTarget target) throws PluginException {
      Iterator<ArticleFiles> ret;
      SubTreeArticleIterator.Spec spec = 
        new SubTreeArticleIterator.Spec().setTarget(target);
      
      if (pat != null) {
       spec.setPattern(pat);
      }
      
      ret = new SubTreeArticleIterator(au, spec);
      log.debug(  "creating article iterator for au " + au.getName() 
                    + " hasNext: " + ret.hasNext());
      return ret;
    }
  }

  private static class MySimulatedPlugin extends SimulatedDefinablePlugin {
    ArticleMetadataExtractor simulatedArticleMetadataExtractor = null;
    int version = 2;
    /**
     * Returns the article iterator factory for the mime type, if any
     * @return the ArticleIteratorFactory
     */
    @Override
    public ArticleIteratorFactory getArticleIteratorFactory() {
      return new MySubTreeArticleIteratorFactory(null);
    }
    @Override
    public ArticleMetadataExtractor 
      getArticleMetadataExtractor(MetadataTarget target, ArchivalUnit au) {
      return simulatedArticleMetadataExtractor;
    }

    @Override
    public String getFeatureVersion(Plugin.Feature feat) {
      if (Feature.Metadata == feat) {
	return feat + "_" + version;
      } else {
	return null;
      }
    }
  }

  public static class MySimulatedPlugin0 extends MySimulatedPlugin {
    public MySimulatedPlugin0() {
      simulatedArticleMetadataExtractor = new ArticleMetadataExtractor() {
        public void extract(MetadataTarget target, ArticleFiles af,
            Emitter emitter) throws IOException, PluginException {
          ArticleMetadata md = new ArticleMetadata();
          emitter.emitMetadata(af, md);
        }
      };
    }
    public ExternalizableMap getDefinitionMap() {
      ExternalizableMap map = new ExternalizableMap();
      map.putString("au_start_url", "\"%splugin0/%s\", base_url, volume");
      return map;
    }
  }

  public static class MySimulatedPlugin1 extends MySimulatedPlugin {
    public MySimulatedPlugin1() {
      simulatedArticleMetadataExtractor = new ArticleMetadataExtractor() {
        public void extract(MetadataTarget target, ArticleFiles af,
            Emitter emitter) throws IOException, PluginException {
          ArticleMetadata md = new ArticleMetadata();
          emitter.emitMetadata(af, md);
        }
      };
    }
    public ExternalizableMap getDefinitionMap() {
      ExternalizableMap map = new ExternalizableMap();
      map.putString("key1", "value1");
      map.putString("key2", "value2");
      return map;
    }
  }
}
