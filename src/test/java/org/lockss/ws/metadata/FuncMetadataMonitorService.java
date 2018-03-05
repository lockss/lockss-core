/*

 Copyright (c) 2015-2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.ws.metadata;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import org.lockss.config.ConfigManager;
import org.lockss.config.Configuration;
import org.lockss.metadata.MetadataManager;
import org.lockss.metadata.TestMetadataManager.MySimulatedPlugin0;
import org.lockss.plugin.PluginManager;
import org.lockss.plugin.PluginTestUtil;
import org.lockss.plugin.simulated.SimulatedArchivalUnit;
import org.lockss.plugin.simulated.SimulatedContentGenerator;
import org.lockss.servlet.AdminServletManager;
import org.lockss.servlet.ServletManager;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;

/**
 * Functional test class for org.lockss.ws.metadata.MetadataMonitorService.
 * 
 * @author Fernando Garcia-Loygorri
 */
public class FuncMetadataMonitorService extends LockssTestCase {
  private static final String USER_NAME = "lockss-u";
  private static final String PASSWORD = "lockss-p";
  private static final String PASSWORD_SHA1 =
      "SHA1:ac4fc8fa9930a24c8d002d541c37ca993e1bc40f";
  private static final String TARGET_NAMESPACE =
      "http://metadata.ws.lockss.org/";
  private static final String SERVICE_NAME =
      "MetadataMonitorServiceImplService";

  private MetadataMonitorService proxy;
  
  /** number of articles deleted by the MetadataManager */
  Integer[] articlesDeleted = new Integer[] {0};

  public void setUp() throws Exception {
    super.setUp();

    String tempDirPath = setUpDiskSpace();

    int port = TcpTestUtil.findUnboundTcpPort();
    ConfigurationUtil.addFromArgs(AdminServletManager.PARAM_PORT, "" + port,
	ServletManager.PARAM_PLATFORM_USERNAME, USER_NAME,
	ServletManager.PARAM_PLATFORM_PASSWORD, PASSWORD_SHA1);

    MockLockssDaemon theDaemon = getMockLockssDaemon();
    theDaemon.getAlertManager();
    PluginManager pluginManager = theDaemon.getPluginManager();
    pluginManager.setLoadablePluginsReady(true);
    theDaemon.setDaemonInited(true);
    theDaemon.getCrawlManager();

    SimulatedArchivalUnit sau = PluginTestUtil
	.createAndStartSimAu(MySimulatedPlugin0.class,
	    simAuConfig(tempDirPath + "/0"));
    PluginTestUtil.crawlSimAu(sau);

    getTestDbManager(tempDirPath);
    MetadataManager metadataManager = new MetadataManager();

    theDaemon.setMetadataManager(metadataManager);
    metadataManager.initService(theDaemon);
    metadataManager.startService();
    theDaemon.getServletManager().startService();

    theDaemon.setAusStarted(true);
    
    int expectedAuCount = 1;
    assertEquals(expectedAuCount, pluginManager.getAllAus().size());

    // The client authentication.
    Authenticator.setDefault(new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
	return new PasswordAuthentication(USER_NAME, PASSWORD.toCharArray());
      }
    });

    String addressLocation = "http://localhost:" + port
	+ "/ws/MetadataMonitorService?wsdl";

    Service service = Service.create(new URL(addressLocation), new QName(
	TARGET_NAMESPACE, SERVICE_NAME));

    proxy = service.getPort(MetadataMonitorService.class);
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

  /**
   * Tests the metadata monitor.
   */
  public void testMetadataMonitor() throws Exception {
    List<String> publisherNames = proxy.getPublisherNames();
    assertEquals(1, publisherNames.size());
    assertEquals("Publisher 0", publisherNames.get(0));
    assertNull(proxy.getPublishersWithMultipleDoiPrefixes());
    assertNull(proxy.getDoiPrefixesWithMultiplePublishers());
    assertNull(proxy.getAuNamesWithMultipleDoiPrefixes());
    assertNull(proxy.getPublicationsWithMoreThan2Isbns());
    assertNull(proxy.getPublicationsWithMoreThan2Issns());
    assertNull(proxy.getIsbnsWithMultiplePublications());
    assertNull(proxy.getIssnsWithMultiplePublications());
    assertNull(proxy.getBooksWithIssns());
    assertNull(proxy.getPeriodicalsWithIsbns());
    assertNull(proxy.getUnknownProviderAuIds());
    assertNull(proxy.getMismatchedParentJournalArticles());
    assertNull(proxy.getMismatchedParentBookChapters());
    assertNull(proxy.getMismatchedParentBookVolumes());
    assertNull(proxy.getAuNamesWithMultiplePublishers());
    assertNull(proxy.getUnnamedItems());
    assertNull(proxy.getPublicationsWithMultiplePids());
    assertNull(proxy.getNoDoiItems());
    assertNull(proxy.getNoAccessUrlItems());
    assertNull(proxy.getNoItemsAuIds());
  }
}
