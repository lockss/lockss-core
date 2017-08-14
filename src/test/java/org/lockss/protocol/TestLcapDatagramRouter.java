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

package org.lockss.protocol;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.lockss.poller.TestPoll;
import org.lockss.test.*;
import org.lockss.util.*;

/** JUnitTest case for class: org.lockss.protocol.Message */
public class TestLcapDatagramRouter extends LockssTestCase {
  static Logger log = Logger.getLogger("TestLcapDatagramRouter");

  private LcapDatagramRouter rtr;

  private final static String urlstr = "http://www.example.com";
  private final static byte[] testbytes = {1,2,3,4,5,6,7,8,9,10};
  private final static String lwrbnd = "test1.doc";
  private final static String uprbnd = "test3.doc";
  private final static String archivalID = "TestAU_1.0";

  private MockLockssDaemon daemon;
  private IdentityManager idmgr;
  protected IPAddr testaddr;
  protected PeerIdentity testID;
  protected V1LcapMessage testmsg;
  LockssDatagram dg;
  LockssReceivedDatagram rdg;
  private ArrayList testentries;

  public void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();
    setConfig();
    idmgr = daemon.getIdentityManager();
    // this causes error messages, but need to start comm so it gets idmgr.
    daemon.getDatagramCommManager().startService();
    rtr = daemon.getDatagramRouterManager();
    rtr.startService();
    TimeBase.setSimulated(20000);
  }

  public void tearDown() throws Exception {
    LcapDatagramComm comm = daemon.getDatagramCommManager();
    if (comm != null) {
      comm.stopService();
    }
    super.tearDown();
  }

  public void testIsEligibleToForward() throws Exception {
    //     LcapMessage msg = createTestMsg("127.0.0.1", 3);
    createTestMsg("1.2.3.4", 3);
    TimeBase.step(100000);
    assertTrue(rtr.isEligibleToForward(rdg, testmsg));
    createTestMsg("1.2.3.4", 0);
    TimeBase.step(100000);
    assertFalse(rtr.isEligibleToForward(rdg, testmsg));
    createTestMsg("127.0.0.1", 3);
    TimeBase.step(100000);
    assertFalse(rtr.isEligibleToForward(rdg, testmsg));
  }

  void setConfig() {
    Properties p = new Properties();
    p.put(LcapDatagramRouter.PARAM_FWD_MSG_RATE, "100/1");
    p.put(LcapDatagramRouter.PARAM_BEACON_INTERVAL, "1m");
    p.put(LcapDatagramRouter.PARAM_INITIAL_HOPCOUNT, "3");
    p.put(LcapDatagramRouter.PARAM_PROB_PARTNER_ADD, "100");
    p.put(IdentityManager.PARAM_LOCAL_IP, "127.0.0.1");
    ConfigurationUtil.setCurrentConfigFromProps(p);
  }

  V1LcapMessage createTestMsg(String originator, int hopCount)
      throws IOException {
    try {
      testaddr = IPAddr.getByName(originator);
      testID =
	idmgr.stringToPeerIdentity(originator);
    }
    catch (UnknownHostException ex) {
      fail("can't open test host");
    }
    try {
      testmsg = new V1LcapMessage();
    }
    catch (IOException ex) {
      fail("can't create test message");
    }
    // assign the data
    testmsg.m_targetUrl = urlstr;
    testmsg.m_lwrBound = lwrbnd;
    testmsg.m_uprBound = uprbnd;

    testmsg.m_originatorID = testID;
    testmsg.m_hashAlgorithm = LcapMessage.getDefaultHashAlgorithm();
    testmsg.m_startTime = 123456789;
    testmsg.m_stopTime = 987654321;
    testmsg.m_multicast = false;
    testmsg.m_hopCount = (byte)hopCount;

    // testmsg.m_ttl = 5;
    testmsg.m_challenge = testbytes;
    testmsg.m_verifier = testbytes;
    testmsg.m_hashed = testbytes;
    testmsg.m_opcode = V1LcapMessage.CONTENT_POLL_REQ;
    testmsg.m_entries = testentries = TestPoll.makeEntries(1, 25);
    testmsg.m_archivalID = archivalID;
    dg = new LockssDatagram(LockssDatagram.PROTOCOL_LCAP, testmsg.encodeMsg());
    rdg = new LockssReceivedDatagram(dg.makeSendPacket(testaddr, 0));
    rdg.setMulticast(true);
    return testmsg;
  }

}

