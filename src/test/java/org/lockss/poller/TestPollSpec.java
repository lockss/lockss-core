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

package org.lockss.poller;

import java.util.*;
import junit.framework.TestCase;
import org.lockss.daemon.*;
import org.lockss.plugin.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;
import org.lockss.test.*;
import org.lockss.protocol.*;
import java.net.*;
import java.io.*;

/**
 * This is the test class for org.lockss.poller.PollSpec
 */

public class TestPollSpec extends LockssTestCase {
  private MockLockssDaemon theDaemon;

  File tempDir;

  public void setUp() throws Exception {
    super.setUp();

    tempDir = getTempDir();
    String tempDirPath = tempDir.getAbsolutePath() + File.separator;
    Properties p = new Properties();
    p.setProperty(IdentityManager.PARAM_IDDB_DIR, tempDirPath + "iddb");
    p.setProperty(IdentityManager.PARAM_LOCAL_IP, "127.0.0.1");
    ConfigurationUtil.setCurrentConfigFromProps(p);
    useOldRepo();

    theDaemon = getMockLockssDaemon();
    theDaemon.getIdentityManager();
    theDaemon.getPluginManager();
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }

  public void testIll() {
    MockArchivalUnit au = new MockArchivalUnit();
    CachedUrlSet cus =
      new MockCachedUrlSet(au, PrunedCachedUrlSetSpec.includeMatchingSubTrees("foo", "foo"));
    try {
      new PollSpec(cus, Poll.V3_POLL);
      fail("Shouldn't be able to make PollSpec from PrunedCachedUrlSetSpec");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testFromCus() {
    String auid = "aaai1";
    String url = "http://foo.bar/";
    String lower = "abc";
    String upper = "xyz";
    String plugVer = "ver42";
    String pollVer = "p24";
    MockArchivalUnit au = new MockArchivalUnit();
    au.setAuId(auid);
    MockPlugin mp = new MockPlugin();
    mp.setVersion(plugVer);
    au.setPlugin(mp);

    CachedUrlSet cus =
      new MockCachedUrlSet(au, new RangeCachedUrlSetSpec(url, lower, upper));
    PollSpec ps = new PollSpec(cus, Poll.V1_CONTENT_POLL);
    assertEquals(auid, ps.getAuId());
    assertEquals(url, ps.getUrl());
    assertEquals(lower, ps.getLwrBound());
    assertEquals(upper, ps.getUprBound());
    assertEquals(plugVer, ps.getPluginVersion());

    mp.setFeatureVersionMap(MapUtil.map(Plugin.Feature.Poll, pollVer));

    ps = new PollSpec(cus, Poll.V1_CONTENT_POLL);
    assertEquals(auid, ps.getAuId());
    assertEquals(url, ps.getUrl());
    assertEquals(lower, ps.getLwrBound());
    assertEquals(upper, ps.getUprBound());
    assertEquals(pollVer, ps.getPluginVersion());
  }

  public void testFromV3LcapMessage() throws Exception {
    byte[] testbytes = {0,1,2,3,4,5,6,8,10};
    String auid = "aaai1";
    MockArchivalUnit au = new MockArchivalUnit();
    au.setAuId(auid);
    MockPlugin plug = new MockPlugin();
    plug.setVersion("oddVer");
    assertEquals("oddVer", plug.getVersion());
    au.setPlugin(plug);
    PeerIdentity id = null;
    try {
      id = theDaemon.getIdentityManager().stringToPeerIdentity("127.0.0.1");
    }
    catch (IdentityManager.MalformedIdentityKeyException ex) {
      fail("can't open test host");
    }
    V3LcapMessage msg = new V3LcapMessage(au.getAuId(),
					  "pollkey",
					  plug.getVersion(),
                                          PollUtil.makeHashNonce(20),
                                          PollUtil.makeHashNonce(20),
                                          V3LcapMessage.MSG_POLL,
                                          TimeBase.nowMs() + Constants.WEEK,
					  id, tempDir, theDaemon);
    PollSpec ps = new PollSpec(msg);

    assertEquals(Poll.V3_PROTOCOL, ps.getProtocolVersion());
    assertEquals(auid, ps.getAuId());
    assertEquals("lockssau:", ps.getUrl());
    assertEquals(plug.getVersion(), ps.getPluginVersion());
  }

  public static void main(String[] argv) {
    String[] testCaseList = {TestPollSpec.class.getName()};
    junit.textui.TestRunner.main(testCaseList);
  }
}
