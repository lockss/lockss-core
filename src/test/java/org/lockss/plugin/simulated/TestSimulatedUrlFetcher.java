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

package org.lockss.plugin.simulated;

import java.io.*;
import java.util.Properties;

import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.test.*;
import org.lockss.test.MockCrawler.MockCrawlerFacade;
import org.lockss.util.*;

/**
 * This is the test class for org.lockss.plugin.simulated.SimulatedUrlCacher
 *
 * @author  Emil Aalto
 * @version 0.0
 */


public class TestSimulatedUrlFetcher extends LockssTestCase {
  private MockLockssDaemon theDaemon;
  private SimulatedArchivalUnit sau;
  private String tempDirPath;
  MockCrawlerFacade mcf;

  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tempDirPath);

//     useOldRepo();
    theDaemon = getMockLockssDaemon();
    theDaemon.getPluginManager();
    Plugin simPlugin = PluginTestUtil.findPlugin(SimulatedPlugin.class);

    String tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    sau = new SimulatedArchivalUnit(simPlugin);
    Configuration auConfig = ConfigurationUtil.fromArgs("root", tempDirPath);
    sau.setConfiguration(auConfig);
    mcf = new MockCrawler().new MockCrawlerFacade();
    mcf.setAu(sau);
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void testHtmlProperties() throws Exception {
    String testStr = "http://www.example.com/index.html";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, "");
    Properties prop = suf.getUncachedProperties();
    assertEquals("text/html",
		 prop.getProperty(CachedUrl.PROPERTY_CONTENT_TYPE));
    assertEquals(testStr, prop.getProperty(CachedUrl.PROPERTY_ORIG_URL));
  }

  public void testTextProperties() throws Exception {
    String testStr = "http://www.example.com/file.txt";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, "");
    Properties prop = suf.getUncachedProperties();
    assertEquals("text/plain",
		 prop.getProperty(CachedUrl.PROPERTY_CONTENT_TYPE));
    assertEquals(testStr, prop.getProperty(CachedUrl.PROPERTY_ORIG_URL));
  }

  public void testPdfProperties() throws Exception {
    String testStr = "http://www.example.com/file.pdf";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, "");
    Properties prop = suf.getUncachedProperties();
    assertEquals("application/pdf",
		 prop.getProperty(CachedUrl.PROPERTY_CONTENT_TYPE));
    assertEquals(testStr, prop.getProperty(CachedUrl.PROPERTY_ORIG_URL));
  }

  public void testJpegProperties() throws Exception {
    String testStr = "http://www.example.com/image.jpg";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, "");
    Properties prop = suf.getUncachedProperties();
    assertEquals("image/jpeg",
		 prop.getProperty(CachedUrl.PROPERTY_CONTENT_TYPE));
    assertEquals(testStr, prop.getProperty(CachedUrl.PROPERTY_ORIG_URL));
  }

  public void testNoBranchContent() throws Exception {
    File branchFile = new File(tempDirPath, "simcontent/branch1");
    branchFile.mkdirs();

    String testStr = "http://www.example.com/branch1";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, tempDirPath);
    assertNull(suf.getUncachedInputStream());
  }

  public void testNoBranchContentWithRedirToAutoIndex() throws Exception {
    Configuration auConfig = sau.getConfiguration();
    auConfig.put("redirectDirToIndex", "true");
    auConfig.put("autoGenIndexHtml", "true");
    sau.setConfiguration(auConfig);

    File branchFile = new File(tempDirPath, "simcontent/branch1");
    branchFile.mkdirs();

    String testStr = "http://www.example.com/branch1";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, tempDirPath);
    String cont = StringUtil.fromInputStream(suf.getUncachedInputStream());
    assertMatchesRE("<A HREF=\"branch1/index.html\">", cont);
  }



  public void testBranchContent() throws Exception {
    File branchFile = new File(tempDirPath,
                               "simcontent/branch1");
    branchFile.mkdirs();
    File contentFile = new File(branchFile, "branch_content");
    FileOutputStream fos = new FileOutputStream(contentFile);
    StringInputStream sis = new StringInputStream("test stream");
    StreamUtil.copy(sis, fos);
    fos.close();
    sis.close();

    String testStr = "http://www.example.com/branch1";
    SimulatedUrlFetcher suf = new SimulatedUrlFetcher(mcf, testStr, tempDirPath);
    InputStream is = suf.getUncachedInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(11);
    StreamUtil.copy(is, baos);
    is.close();
    assertEquals("test stream", baos.toString());
  }

}
