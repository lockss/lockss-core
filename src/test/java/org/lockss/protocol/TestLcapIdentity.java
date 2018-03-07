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

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.lockss.poller.*;
import org.lockss.test.*;
import org.lockss.util.*;
import org.mortbay.util.B64Code;

/** JUnitTest case for class: org.lockss.protocol.Identity */
public class TestLcapIdentity extends LockssTestCase {

  static final String fakeIdString = "127.0.0.1";
  static final String urlstr = "http://www.test.org";
  static final String lwrbnd = "test1.doc";
  static final String uprbnd = "test3.doc";
  static final String archivalid = "testarchive 1.0";
  static final byte[] testbytes = {1,2,3,4,5,6,7,8,9,10};
  LcapIdentity fakeId = null;
  IPAddr testAddress;
  int testReputation;
  PeerIdentity testID;
  LcapMessage testMsg= null;
  private MockLockssDaemon daemon;
  private IdentityManager idmgr;
  private File tempDir;

  /** setUp method for test case */
  protected void setUp() throws Exception {
    super.setUp();
    daemon = getMockLockssDaemon();
    tempDir = getTempDir();
    String host = "1.2.3.4";
    String prop = "org.lockss.localIPAddress="+host;
    ConfigurationUtil.
      setCurrentConfigFromUrlList(ListUtil.list(FileTestUtil.urlOfString(prop)));
    idmgr = daemon.getIdentityManager();
    testID = idmgr.stringToPeerIdentity(fakeIdString);
    try {
      fakeId = new LcapIdentity(testID, fakeIdString);
      testAddress = IPAddr.getByName(fakeIdString);
    }
    catch (UnknownHostException ex) {
      fail("can't open test host");
    }
    testReputation = IdentityManager.INITIAL_REPUTATION;
    PollSpec spec = new MockPollSpec(archivalid, urlstr, lwrbnd, uprbnd,
				     Poll.V1_CONTENT_POLL);
      testMsg =
        new V3LcapMessage("ArchivalID_2", "key", "Plug42",
            testbytes,
            testbytes,
            V3LcapMessage.MSG_REPAIR_REQ,
            987654321, testID, tempDir, daemon);

  }

  /** test for method toString(..) */
  public void testToString() {
    String s = fakeId.toString();
    assertTrue(s.equals((String)fakeId.m_idKey));
  }

  /** test for method rememberActive(..) */
  public void testRememberActive() {
    // XXX: Stubbed for V3
  }

  /** test for method rememberValidOriginator(..) */
  public void testRememberValidOriginator() {
    long val_originator = fakeId.m_origPackets;
    fakeId.rememberValidOriginator(testMsg);
    assertEquals(val_originator + 1, fakeId.m_origPackets);
  }

  /** test for method rememberValidForward(..) */
  public void testRememberValidForward() {
    long val_forward = fakeId.m_forwPackets;
    fakeId.rememberValidForward(testMsg);
    assertEquals(val_forward + 1, fakeId.m_forwPackets);
  }

  /** test for method rememberDuplicate(..) */
  public void testRememberDuplicate() {
    long duplicates = fakeId.m_duplPackets;
    fakeId.rememberDuplicate(testMsg);
    assertEquals(duplicates + 1, fakeId.m_duplPackets);
  }

  public void testSerializationRoundtrip() throws Exception {
    ObjectSerializer serializer = new XStreamSerializer();
    ObjectSerializer deserializer = new XStreamSerializer(daemon);

    File temp1 = File.createTempFile("tmp", ".xml");
    temp1.deleteOnExit();
    PeerIdentity pidv1 = new PeerIdentity("12.34.56.78");
    LcapIdentity lid1 = new LcapIdentity(pidv1, "12.34.56.78");
    serializer.serialize(temp1, lid1);
    LcapIdentity back1 = (LcapIdentity)deserializer.deserialize(temp1);
    assertTrue(lid1.isEqual(back1));
    assertEquals(lid1.m_address.getAddress(),
                 back1.m_address.getAddress());

    File temp3 = File.createTempFile("tmp", ".xml");
    temp3.deleteOnExit();
    PeerIdentity pidv3 =
      new PeerIdentity(IDUtil.ipAddrToKey("87.65.43.21", "999"));
    LcapIdentity lid3 =
      new LcapIdentity(pidv3, IDUtil.ipAddrToKey("87.65.43.21", "999"));
    serializer.serialize(temp3, lid3);
    LcapIdentity back3 = (LcapIdentity)deserializer.deserialize(temp3);
    assertTrue(lid3.isEqual(back3));
    assertEquals(lid3.m_address.getAddress(),
                 back3.m_address.getAddress());
  }

  public void testFindLcapIdentity() throws Exception {
    PeerIdentity pidv1 = new PeerIdentity("12.34.56.78");
    LcapIdentity lid1 = idmgr.findLcapIdentity(pidv1, "12.34.56.78");
    LcapIdentity lid2 = idmgr.findLcapIdentity(pidv1, "12.34.56.78");
    assertSame(lid1, lid2);
  }

}
