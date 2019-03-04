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

import org.lockss.config.*;
import org.lockss.plugin.*;
import org.lockss.test.*;
import org.lockss.util.*;

/**
 * This is the test class for org.lockss.plugin.simulated.SimulatedUrlCacher
 *
 * @author  Emil Aalto
 * @version 0.0
 */


public class TestSimulatedUrlCacher extends LockssTestCase {
  private MockLockssDaemon theDaemon;
  private SimulatedArchivalUnit sau;
  private String tempDirPath;

  public void setUp() throws Exception {
    super.setUp();
    tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.setFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
				  tempDirPath);

//     useOldRepo();
    theDaemon = getMockLockssDaemon();
    theDaemon.getPluginManager();
    
    Plugin simPlugin = PluginTestUtil.findPlugin(SimulatedPlugin.class);

    Configuration auConfig = ConfigurationUtil.fromArgs("root", tempDirPath);
    sau = (SimulatedArchivalUnit) PluginTestUtil.createAndStartAu(simPlugin, auConfig);

  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void testBranchContent() throws Exception {
    File branchFile = new File(tempDirPath,
                               "simcontent/branch1");
    branchFile.mkdirs();
    StringInputStream sis = new StringInputStream("test stream");

    String testStr = "http://www.example.com/branch1";
    UrlData ud = new UrlData(sis, new CIProperties(), testStr);
    SimulatedUrlCacher suc = new SimulatedUrlCacher(sau, ud, tempDirPath);
    suc.storeContent();
    InputStream is = suc.getCachedUrl().getUnfilteredInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(11);
    StreamUtil.copy(is, baos);
    is.close();
    assertEquals("test stream", baos.toString());
  }

}
