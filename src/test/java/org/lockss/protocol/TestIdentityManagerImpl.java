/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

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

import java.io.*;
import java.util.*;

import org.lockss.config.ConfigManager;
import org.lockss.daemon.status.StatusTable;
import org.lockss.hasher.LocalHashResult;
import org.lockss.plugin.*;
import org.lockss.poller.Poll;
import org.lockss.state.AuState;
import org.lockss.test.*;
import org.lockss.util.*;
import org.lockss.util.net.IPAddr;
import org.lockss.util.time.TimeBase;

import junit.framework.Test;

/** Test cases for org.lockss.protocol.IdentityManager that assume the
 * IdentityManager has been initialized.  See TestIdentityManagerInit for
 * more IdentityManager tests. */
public abstract class TestIdentityManagerImpl extends LockssTestCase {

  /**
   * <p>A version of {@link TestIdentityManagerImpl} that forces the
   * serialization compatibility mode to
   * {@link CXSerializer#XSTREAM_MODE}.</p>
   * @author Thib Guicherd-Callin
   */
  public static class WithXStream extends TestIdentityManagerImpl {
    public void setUp() throws Exception {
      super.setUp();
      ConfigurationUtil.addFromArgs(CXSerializer.PARAM_COMPATIBILITY_MODE,
                                    Integer.toString(CXSerializer.XSTREAM_MODE));
    }
  }

  public static Test suite() {
    return variantSuites(TestIdentityManagerImpl.class);
  }

  Object testIdKey;

  private MockLockssDaemon theDaemon;
  private TestableIdentityManager idmgr;
  String tempDirPath;

  PeerIdentity peer1;
  PeerIdentity peer2;
  PeerIdentity peer3;
  PeerIdentity peer4;
  MockArchivalUnit mau;

  private static final String LOCAL_IP = "127.1.2.3";
  private static final String IP_2 = "127.6.5.4";
  private static final String LOCAL_V3_ID = "TCP:[127.1.2.3]:3141";
  private static final int LOCAL_PORT_NUM = 3141;
//   private static final String LOCAL_PORT = "3141";
  private static final String LOCAL_PORT = Integer.toString(LOCAL_PORT_NUM);

  public void setUp() throws Exception {
    super.setUp();

    theDaemon = getMockLockssDaemon();
    mau = newMockArchivalUnit();
    tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    ConfigurationUtil.setCurrentConfigFromProps(commonConfig());

    idmgr = new TestableIdentityManager();
    idmgr.initService(theDaemon);
    theDaemon.setIdentityManager(idmgr);
    idmgr.startService();
    MockPlugin plug = new MockPlugin(theDaemon);
    mau.setPlugin(plug);
  }

  public void tearDown() throws Exception {
    if (idmgr != null) {
      idmgr.stopService();
      idmgr = null;
    }
    super.tearDown();
  }

  MockArchivalUnit newMockArchivalUnit() {
    MockArchivalUnit res = new MockArchivalUnit();
    AuTestUtil.setUpMockAus(res);
    return res;
  }

  Properties commonConfig() {
    Properties p = new Properties();
    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);
    p.setProperty(IdentityManager.PARAM_IDDB_DIR, tempDirPath + "iddb");
    p.setProperty(IdentityManager.PARAM_LOCAL_V3_IDENTITY, LOCAL_V3_ID);
    p.setProperty(IdentityManager.PARAM_IDDB_DIR, tempDirPath);
    return p;
  }

  void setupPeer123() throws IdentityManager.MalformedIdentityKeyException {
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");
    peer2 = idmgr.stringToPeerIdentity("127.0.0.2");
    peer3 = idmgr.stringToPeerIdentity("127.0.0.3");
    peer4 = idmgr.stringToPeerIdentity("tcp:[127.0.0.4]:4444");
  }

  void setupV3Peer123() throws IdentityManager.MalformedIdentityKeyException {
    peer1 = idmgr.stringToPeerIdentity("tcp:[127.0.0.1]:1111");
    peer2 = idmgr.stringToPeerIdentity("tcp:[127.0.0.2]:2222");
    peer3 = idmgr.stringToPeerIdentity("tcp:[127.0.0.3]:3333");
    peer4 = idmgr.stringToPeerIdentity("tcp:[127.0.0.4]:4444");
  }

  public void testSetupLocalIdentitiesV3Normal()
      throws IdentityManager.MalformedIdentityKeyException {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_PORT,
                                  LOCAL_PORT);
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_INITIAL_PEERS,
                                  LOCAL_V3_ID);
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.initService(theDaemon);
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    assertTrue("Peer ID is not a local identity.", pid1.isLocalIdentity());
    String key = IDUtil.ipAddrToKey(LOCAL_IP, LOCAL_PORT_NUM);
    PeerIdentity pid2 = mgr.stringToPeerIdentity(key);
    assertSame(pid1, pid2);
    PeerAddress pa = pid1.getPeerAddress();
    assertTrue(pa instanceof PeerAddress.Tcp);
    assertEquals(LOCAL_IP, ((PeerAddress.Tcp)pa).getIPAddr().getHostAddress());
    assertEquals(LOCAL_PORT_NUM, ((PeerAddress.Tcp)pa).getPort());
    assertEquals(ListUtil.list(pid1), mgr.getLocalPeerIdentities());
  }
  
  public void testSetupLocalIdentitiesV3Only()
      throws IdentityManager.MalformedIdentityKeyException {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_PORT,
                                  LOCAL_PORT);
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_INITIAL_PEERS,
                                  LOCAL_V3_ID);
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.initService(theDaemon);
    assertNull(mgr.localPeerIdentities[Poll.V1_PROTOCOL]);
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    assertTrue("Peer ID is not a local identity.", pid1.isLocalIdentity());
    String key = IDUtil.ipAddrToKey(LOCAL_IP, LOCAL_PORT_NUM);
    PeerIdentity pid2 = mgr.stringToPeerIdentity(key);
    assertSame(pid1, pid2);
    PeerAddress pa = pid1.getPeerAddress();
    assertTrue(pa instanceof PeerAddress.Tcp);
    assertEquals(LOCAL_IP, ((PeerAddress.Tcp)pa).getIPAddr().getHostAddress());
    assertEquals(LOCAL_PORT_NUM, ((PeerAddress.Tcp)pa).getPort());
    assertEquals(ListUtil.list(pid1), mgr.getLocalPeerIdentities());
  }
  
  public void testSetupLocalIdentitiesV3FromLocalV3IdentityParam() 
      throws Exception {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_IDENTITY,
                                  LOCAL_V3_ID);
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_INITIAL_PEERS,
                                  LOCAL_V3_ID);
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.initService(theDaemon);
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    assertTrue("Peer ID is not a local identity.", pid1.isLocalIdentity());
    String key = IDUtil.ipAddrToKey(LOCAL_IP, LOCAL_PORT_NUM);
    PeerIdentity pid2 = mgr.stringToPeerIdentity(key);
    assertSame(pid1, pid2);
    PeerAddress pa = pid1.getPeerAddress();
    assertTrue(pa instanceof PeerAddress.Tcp);
    assertEquals(LOCAL_IP, ((PeerAddress.Tcp)pa).getIPAddr().getHostAddress());
    assertEquals(LOCAL_PORT_NUM, ((PeerAddress.Tcp)pa).getPort());
  }

  public void testSetupLocalIdentitiesV3Override()
      throws IdentityManager.MalformedIdentityKeyException {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_PORT,
                                  LOCAL_PORT,
                                  IdentityManager.PARAM_LOCAL_V3_IDENTITY,
                                  IDUtil.ipAddrToKey(IP_2,
						     (LOCAL_PORT_NUM + 123)));
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.setupLocalIdentities();
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    PeerAddress pa = pid1.getPeerAddress();
    assertTrue(pa instanceof PeerAddress.Tcp);
    assertEquals(IP_2, ((PeerAddress.Tcp)pa).getIPAddr().getHostAddress());
    assertEquals(LOCAL_PORT_NUM + 123, ((PeerAddress.Tcp)pa).getPort());
  }

  /** test for method stringToPeerIdentity **/
  public void testStringToPeerIdentity() throws Exception {
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");
    assertNotNull(peer1);
    assertSame(peer1, idmgr.stringToPeerIdentity("127.0.0.1"));
    peer2 = idmgr.stringToPeerIdentity("127.0.0.2");
    assertNotNull(peer2);
    assertNotEquals(peer1, peer2);
    peer3 = idmgr.stringToPeerIdentity(IDUtil.ipAddrToKey("127.0.0.2", "300"));
    assertNotNull(peer3);
    assertNotEquals(peer3, peer2);
    assertNotEquals(peer3, peer1);
  }

  /** test for method ipAddrToPeerIdentity **/
  public void testIPAddrToPeerIdentity() throws Exception {
    IPAddr ip1 = IPAddr.getByName("127.0.0.1");
    IPAddr ip2 = IPAddr.getByName("127.0.0.2");
    IPAddr ip3 = IPAddr.getByName("127.0.0.3");

    peer1 = idmgr.ipAddrToPeerIdentity(ip1);
    assertNotNull(peer1);
    assertSame(peer1, idmgr.ipAddrToPeerIdentity(ip1));
    assertSame(peer1, idmgr.stringToPeerIdentity("127.0.0.1"));
    peer2 = idmgr.ipAddrToPeerIdentity(ip2);
    assertNotNull(peer2);
    assertNotEquals(peer1, peer2);
    assertSame(peer2, idmgr.stringToPeerIdentity("127.0.0.2"));
    peer3 = idmgr.ipAddrToPeerIdentity(ip2, 300);
    assertNotNull(peer3);
    assertNotEquals(peer3, peer2);
    assertNotEquals(peer3, peer1);
    assertSame(peer3,
	       idmgr.stringToPeerIdentity(IDUtil.ipAddrToKey("127.0.0.2",
							     "300")));
  }

  public void testGetLocalIdentityIll() {
    try {
      peer1 = idmgr.getLocalPeerIdentity(Poll.MAX_PROTOCOL + 32);
      fail("getLocalPeerIdentity(" + (Poll.MAX_PROTOCOL + 32) +
	   ") should throw");
    } catch (IllegalArgumentException e) {
    }
  }

  // -----------------------------------------------------

  public void testStoreIdentities() throws Exception {
    setupPeer123();
    idmgr.storeIdentities();
  }

  public void testLoadIdentities() throws Exception {
    // Store
    setupPeer123();
    idmgr.storeIdentities();

    // Load
    MockLockssDaemon otherDaemon = getMockLockssDaemon();
    IdentityManagerImpl im = new IdentityManagerImpl();
    im.initService(otherDaemon);
    otherDaemon.setIdentityManager(im);
    im.reloadIdentities();
    im.findPeerIdentity("127.0.0.2");
  }

  public void testSignalAgreedThrowsOnNullAu() throws Exception {
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");
    try {
      idmgr.signalAgreed(peer1, null);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testSignalAgreedThrowsOnNullId() throws Exception {
    try {
      idmgr.signalAgreed(null, mau);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testSignalDisagreedThrowsOnNullAu() throws Exception {
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");
    try {
      idmgr.signalDisagreed(peer1, null);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testSignalDisagreedThrowsOnNullId() throws Exception {
    try {
      idmgr.signalDisagreed(null, mau);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testSignalAgreedWithLocalIdentity() throws Exception {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_PORT,
                                  LOCAL_PORT);
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_INITIAL_PEERS,
                                  LOCAL_V3_ID);
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.initService(theDaemon);
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    assertTrue("Peer ID is not a local identity.", pid1.isLocalIdentity());
    assertEmpty(mgr.getAgreed(mau));
    mgr.signalAgreed(pid1,mau);
    assertEquals(1, mgr.getAgreed(mau).size());
    assertNotNull(mgr.getAgreed(mau).get(pid1));
  }

  public void testSignalDisagreedWithLocalIdentity() throws Exception {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_PORT,
                                  LOCAL_PORT);
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_INITIAL_PEERS,
                                  LOCAL_V3_ID);
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.initService(theDaemon);
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    assertTrue("Peer ID is not a local identity.", pid1.isLocalIdentity());
    //    assertEmpty(mgr.getDisagreed(mau));
    mgr.signalDisagreed(pid1,mau);
    //    assertEquals(1, mgr.getDisagreed(mau).size());
    //    assertNotNull(mgr.getDisagreed(mau).get(pid1));
  }

  public void testSignalWithLocalIdentityDoesntRemove() throws Exception {
    TimeBase.setSimulated(10);
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_LOCAL_V3_PORT,
                                  LOCAL_PORT);
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_INITIAL_PEERS,
                                  LOCAL_V3_ID);
    IdentityManagerImpl mgr = new IdentityManagerImpl();
    mgr.initService(theDaemon);
    PeerIdentity pid1 = mgr.localPeerIdentities[Poll.V3_PROTOCOL];
    assertTrue("Peer ID is not a local identity.", pid1.isLocalIdentity());
    //    assertEmpty(mgr.getDisagreed(mau));
    mgr.signalDisagreed(pid1,mau);
    //    assertEquals(1, mgr.getDisagreed(mau).size());
    //    assertNotNull(mgr.getDisagreed(mau).get(pid1));
    assertEmpty(mgr.getAgreed(mau));
    TimeBase.step();
    mgr.signalAgreed(pid1,mau);
    assertEquals(1, mgr.getAgreed(mau).size());
    assertNotNull(mgr.getAgreed(mau).get(pid1));
    //    assertEquals(1, mgr.getDisagreed(mau).size());
    //    assertNotNull(mgr.getDisagreed(mau).get(pid1));
    TimeBase.step();
    mgr.signalDisagreed(pid1,mau);
    assertEquals(1, mgr.getAgreed(mau).size());
    assertNotNull(mgr.getAgreed(mau).get(pid1));
    //    assertEquals(1, mgr.getDisagreed(mau).size());
    //    assertNotNull(mgr.getDisagreed(mau).get(pid1));
  }

  public void testGetAgreeThrowsOnNullAu() throws Exception {
    try {
      idmgr.getAgreed(null);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
    }
  }

  public void testGetAgreedNoneSet() throws Exception {
    assertEmpty(idmgr.getAgreed(mau));
  }

  public void testHasAgreed() throws Exception {
    TimeBase.setSimulated(10);

    setupPeer123();
    assertFalse(idmgr.hasAgreed(peer1, mau));
    assertFalse(idmgr.hasAgreed(peer2, mau));

    idmgr.signalAgreed(peer1, mau);
    assertTrue(idmgr.hasAgreed(peer1, mau));
    assertFalse(idmgr.hasAgreed(peer2, mau));
    TimeBase.step();
    idmgr.signalAgreed(peer2, mau);
    assertTrue(idmgr.hasAgreed(peer1, mau));
    assertTrue(idmgr.hasAgreed(peer2, mau));
  }

  public void testGetAgreed() throws Exception {
    TimeBase.setSimulated(10);
    setupPeer123();

    idmgr.signalAgreed(peer1, mau);
    TimeBase.step();
    idmgr.signalAgreed(peer2, mau);
    Map expected = new HashMap();
    expected.put(peer1, Long.valueOf(10));
    expected.put(peer2, Long.valueOf(11));
    assertEquals(expected, idmgr.getAgreed(mau));
  }
  
  public void testSignalPartialAgreement() throws Exception {
    setupPeer123();

    idmgr.signalPartialAgreement(peer1, mau, 0.85f);
    idmgr.signalPartialAgreement(peer2, mau, 0.95f);
    idmgr.signalPartialAgreement(peer3, mau, 1.00f);
    
    idmgr.signalPartialAgreement(AgreementType.POP, peer1, mau, 0.1f);
    idmgr.signalPartialAgreement(AgreementType.POP_HINT, peer1, mau, 0.2f);
    idmgr.signalPartialAgreement(AgreementType.W_POR, peer1, mau, 0.3f);

    idmgr.signalPartialAgreement(AgreementType.W_POR, peer1, mau, 0.25f);

    assertEquals(0.85f, idmgr.getPercentAgreement(peer1, mau), 0.001f);
    assertEquals(0.95f, idmgr.getPercentAgreement(peer2, mau), 0.001f);
    assertEquals(1.00f, idmgr.getPercentAgreement(peer3, mau), 0.001f);

    assertEquals(0.85f,
		 idmgr.getPercentAgreement(peer1, mau, AgreementType.POR),
		 0.001f);
    assertEquals(0.95f,
		 idmgr.getPercentAgreement(peer2, mau, AgreementType.POR),
		 0.001f);
    assertEquals(-1.0f,
		 idmgr.getPercentAgreement(peer2, mau, AgreementType.W_POR));

    assertEquals(0.1f,
		 idmgr.getPercentAgreement(peer1, mau, AgreementType.POP),
		 0.01f);
    assertEquals(0.2f,
		 idmgr.getPercentAgreement(peer1, mau, AgreementType.POP_HINT),
		 0.01f);
    assertEquals(0.25f,
		 idmgr.getPercentAgreement(peer1, mau, AgreementType.W_POR),
		 0.01f);
    assertEquals(0.3f,
		 idmgr.getHighestPercentAgreement(peer1, mau, AgreementType.W_POR),
		 0.01f);
    assertEquals(-1.0f,
		 idmgr.getPercentAgreement(peer1, mau, AgreementType.W_POP),
		 0.01f);
  }
  
  public void testSignalPartialAgreementDisagreementThreshold()
      throws Exception {
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_MIN_PERCENT_AGREEMENT,
 				  "50");
    TimeBase.setSimulated(10);
    setupPeer123();

    assertFalse(idmgr.hasAgreed(peer1, mau));
    assertFalse(idmgr.hasAgreed(peer2, mau));
    assertFalse(idmgr.hasAgreed(peer3, mau));

    idmgr.signalPartialAgreement(peer1, mau, 0.49f);
    AuState aus = AuUtil.getAuState(mau);
    assertEquals(0, aus.getNumWillingRepairers());
    TimeBase.step();
    idmgr.signalPartialAgreement(peer2, mau, 0.50f);
    assertEquals(1, aus.getNumWillingRepairers());
    TimeBase.step();
    idmgr.signalPartialAgreement(peer3, mau, 0.51f);
    assertEquals(2, aus.getNumWillingRepairers());

    Map expectedDisagree = new HashMap();
    expectedDisagree.put(peer1, Long.valueOf(10));
    
    Map expectedAgree = new HashMap();
    expectedAgree.put(peer2, Long.valueOf(11));
    expectedAgree.put(peer3, Long.valueOf(12));

    assertFalse(idmgr.hasAgreed(peer1, mau));
    assertTrue(idmgr.hasAgreed(peer2, mau));
    assertTrue(idmgr.hasAgreed(peer3, mau));
    
    //    assertEquals(expectedDisagree, idmgr.getDisagreed(mau));
    assertEquals(expectedAgree, idmgr.getAgreed(mau));
  }

  private PeerAgreement expected(float... agreements) {
    PeerAgreement expected = PeerAgreement.NO_AGREEMENT;
    for (float agreement: agreements) {
      expected = expected.signalAgreement(agreement, TimeBase.nowMs());
    }
    return expected;
  }

  public void testSignalLocalHashComplete() throws Exception {
    setupPeer123();

    // Nothing but logging the call at present.
    idmgr.signalLocalHashComplete(new LocalHashResult());
  }

  public void testGetIdentityAgreements() throws Exception {
    TimeBase.setSimulated(10);
    setupPeer123();

    idmgr.signalPartialAgreement(AgreementType.POR, peer1, mau, 0.49f);
    PeerAgreement expected = expected(0.49f);

    Map<PeerIdentity, PeerAgreement> agreementsPOR = 
      idmgr.getAgreements(mau, AgreementType.POR);
    assertSameElements(SetUtil.set(peer1), agreementsPOR.keySet());
    PeerAgreement agreement = agreementsPOR.get(peer1);
    assertEquals(expected, agreement);
  }

  public void testHasAgreeMap() throws Exception {
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");
    final File nonExistingFile = new File(tempDirPath, "nofile");

    assertFalse(idmgr.hasAgreeMap(mau));
    idmgr.signalAgreed(peer1, mau);
    assertTrue(idmgr.hasAgreeMap(mau));

    // Make sure that nothing has created this by accident.
    assertFalse(nonExistingFile.exists());
  }

  // ensure that deactivating and reactivating an AU gets its old data
  // back (i.e., uses auid not au as key in maps
  public void testAgreedUsesAuid() throws Exception {
    TimeBase.setSimulated(10);
    setupPeer123();

    idmgr.signalAgreed(peer1, mau);
    TimeBase.step();
    idmgr.signalAgreed(peer2, mau);
    Map expected = new HashMap();
    expected.put(peer1, Long.valueOf(10));
    expected.put(peer2, Long.valueOf(11));
    assertEquals(expected, idmgr.getAgreed(mau));
    MockArchivalUnit mau2 = newMockArchivalUnit();
    // simulate desctivating and reactivating an AU, which creates a new AU
    // instance with the same auid
    mau2.setAuId(mau.getAuId());
    assertEquals(expected, idmgr.getAgreed(mau2));
  }

  public void testDisagreeDoesntRemove() throws Exception {
    TimeBase.setSimulated(10);
    setupPeer123();

    idmgr.signalAgreed(peer1, mau);
    idmgr.signalAgreed(peer2, mau);

    idmgr.signalDisagreed(peer1, mau);

    Map expected = new HashMap();
    expected.put(peer1, Long.valueOf(10));
    expected.put(peer2, Long.valueOf(10));

    assertEquals(expected, idmgr.getAgreed(mau));
  }

  public void testGetCachesToRepairFromThrowsOnNullAu()
      throws Exception {
    try {
      idmgr.getCachesToRepairFrom(null);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  public void testGetCachesToRepairFromNoneSet() throws Exception {
    assertEmpty(idmgr.getCachesToRepairFrom(mau));
  }

  public void testGetCachesToRepairFrom() throws Exception {
    TimeBase.setSimulated(10);
    setupPeer123();

    idmgr.signalAgreed(peer1, mau);
    idmgr.signalDisagreed(peer2, mau);
    TimeBase.step();
    idmgr.signalDisagreed(peer1, mau);
    idmgr.signalAgreed(peer2, mau);
    idmgr.signalAgreed(peer3, mau);
    List toRepair = idmgr.getCachesToRepairFrom(mau);
    assertSameElements(ListUtil.list(peer1, peer2, peer3), toRepair);
    // List order is unspecified.
  }

  public void testCountCachesToRepairFromThrowsOnNullAu()
      throws Exception {
    try {
      idmgr.countCachesToRepairFrom(null);
      fail("Should have thrown on a null au");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  public void testCountCachesToRepairFromNoneSet() throws Exception {
    assertEquals(0, idmgr.countCachesToRepairFrom(mau));
  }

  public void testCountCachesToRepairFrom() throws Exception {
    setupPeer123();

    idmgr.signalAgreed(peer1, mau);
    idmgr.signalDisagreed(peer2, mau);
    idmgr.signalDisagreed(peer1, mau);
    idmgr.signalAgreed(peer2, mau);
    idmgr.signalAgreed(peer3, mau);
    assertEquals(3, idmgr.countCachesToRepairFrom(mau));
  }

  public void testAgreeUpdatesTime() throws Exception {
    TimeBase.setSimulated(10);
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");


    idmgr.signalAgreed(peer1, mau);
    TimeBase.step(15);
    idmgr.signalAgreed(peer1, mau);

    Map expected = new HashMap();
    expected.put(peer1, Long.valueOf(25));

    assertEquals(expected, idmgr.getAgreed(mau));
  }

  public void testMultipleAus() throws Exception {
    TimeBase.setSimulated(10);
    setupPeer123();

    MockArchivalUnit mau1 = newMockArchivalUnit();
    MockArchivalUnit mau2 = newMockArchivalUnit();
    MockPlugin plug = new MockPlugin(theDaemon);
    mau1.setPlugin(plug);
    mau2.setPlugin(plug);
    log.info("auid1: " + mau1.getAuId());
    log.info("auid2: " + mau2.getAuId());

    idmgr.signalAgreed(peer1, mau1);
    idmgr.signalAgreed(peer2, mau2);

    idmgr.signalDisagreed(peer1, mau2);

    Map expected = new HashMap();
    expected.put(peer2, Long.valueOf(10));

    assertEquals(expected, idmgr.getAgreed(mau2));

    expected = new HashMap();
    expected.put(peer1, Long.valueOf(10));
    assertEquals(expected, idmgr.getAgreed(mau1));
  }

  /**
   * Tests that the IP address info fed to the IdentityManagerStatus object
   * looks like an IP address (x.x.x.x)
   */

  public void NoTestStatusInterface() throws Exception {
    peer1 = idmgr.stringToPeerIdentity("127.0.0.1");
    peer2 = idmgr.stringToPeerIdentity("127.0.0.2");

    idmgr.signalAgreed(peer1, mau);
    idmgr.signalAgreed(peer2, mau);

    Map<PeerIdentity,PeerIdentityStatus> idMap = idmgr.getIdentityMap();
    Set expectedAddresses = new HashSet();
    expectedAddresses.add("127.0.0.1");
    expectedAddresses.add("127.0.0.2");
    expectedAddresses.add(LOCAL_IP);

    for (PeerIdentityStatus pis : idMap.values()) {
      assertTrue(pis.getPeerIdentity() + " not found in " + expectedAddresses,
		 expectedAddresses.contains(pis.getPeerIdentity().getIdString()));
    }

    assertEquals(expectedAddresses.size(), idMap.size()); //2 above,plus me
    assertEquals(SetUtil.theSet(idMap.values()),
		 SetUtil.theSet(idmgr.getPeerIdentityStatusList()));
  }

  public void NoTestGetTcpPeerIdentities() throws Exception {
    Collection tcpPeers = idmgr.getTcpPeerIdentities();
    assertNotNull(tcpPeers);
    assertEquals(0, tcpPeers.size());
    PeerIdentity id1 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8001");
    PeerIdentity id2 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8002");
    PeerIdentity id3 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8003");
    PeerIdentity id4 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8004");
    idmgr.findPeerIdentity("127.0.0.2");
    idmgr.findPeerIdentity("127.0.0.3");
    idmgr.findPeerIdentity("127.0.0.4");
    tcpPeers = idmgr.getTcpPeerIdentities();
    log.info("tcp peers: " + tcpPeers);
    assertEquals(4, tcpPeers.size());
    Collection expectedPeers =
      ListUtil.list(id1, id2, id3, id4);
    assertTrue(tcpPeers.containsAll(expectedPeers));
    assertTrue(expectedPeers.containsAll(tcpPeers));
  }

  public void NoTestNormalizePeerIdentities() throws Exception {
    PeerIdentity id1 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8001");
    assertSame(id1, idmgr.findPeerIdentity("tcp:[127.0.0.1]:8001"));
    assertSame(id1, idmgr.findPeerIdentity("TCP:[127.0.0.1]:8001"));
    assertSame(id1, idmgr.findPeerIdentity("tcp:[127.00.0.1]:8001"));
    assertSame(id1, idmgr.findPeerIdentity("tcp:[127.0.0.1]:08001"));
    assertSame(id1, idmgr.findPeerIdentity("tcp:[127.0.0.1]:8001"));
    assertNotEquals(id1, idmgr.findPeerIdentity("tcp:[127.0.0.2]:8001"));
    assertNotEquals(id1, idmgr.findPeerIdentity("tcp:[127.0.0.1]:8002"));

    PeerIdentity id2 = idmgr.findPeerIdentity("tcp:[::1]:9729");
    assertSame(id2, idmgr.findPeerIdentity("tcp:[::1]:9729"));
    assertSame(id2, idmgr.findPeerIdentity("TCP:[::1]:9729"));
    assertSame(id2, idmgr.findPeerIdentity("tcp:[0:0:0:0:0:0:0:1]:9729"));
    assertSame(id2, idmgr.findPeerIdentity("tcp:[0:0:00:0000:0:0:0:1]:9729"));
    assertSame(id2, idmgr.findPeerIdentity("TCP:[::1]:09729"));
    assertNotEquals(id2, idmgr.findPeerIdentity("tcp:[::2]:9729"));
    assertNotEquals(id2, idmgr.findPeerIdentity("tcp:[::1]:9720"));
  }

  void assertIsTcpAddr(String expIp, int expPort, PeerAddress pad) {
    PeerAddress.Tcp tpad = (PeerAddress.Tcp)pad;
    assertEquals(expIp, tpad.getIPAddr().toString());
    assertEquals(expPort, tpad.getPort());
  }

  public void NoTestPeerAddressMap() throws Exception {
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_PEER_ADDRESS_MAP,
                                  "tcp:[127.0.0.1]:8001,tcp:[127.0.3.4]:6602;"+
                                  "tcp:[127.0.0.2]:8003,tcp:[127.0.5.4]:7702;");
    PeerIdentity id1 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8001");
    PeerIdentity id2 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8002");
    PeerIdentity id3 = idmgr.findPeerIdentity("tcp:[127.0.0.2]:8003");
    assertIsTcpAddr("127.0.3.4", 6602, id1.getPeerAddress());
    assertIsTcpAddr("127.0.0.1", 8002, id2.getPeerAddress());
    assertIsTcpAddr("127.0.5.4", 7702, id3.getPeerAddress());
  }

  public void NoTestGetUiUrlStem() throws Exception {
    PeerIdentity id1 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8001");
    PeerIdentity id2 = idmgr.findPeerIdentity("tcp:[127.0.0.1]:8002");
    PeerIdentity id3 = idmgr.findPeerIdentity("tcp:[127.0.0.2]:8003");
    assertNull(idmgr.getUiUrlStem(id1));
    assertNull(idmgr.getUiUrlStem(id2));
    assertNull(idmgr.getUiUrlStem(id3));
    assertEquals("http://127.0.0.1:1234", id1.getUiUrlStem(1234));
    assertEquals("http://127.0.0.1:1234", id2.getUiUrlStem(1234));
    assertEquals("http://127.0.0.2:1234", id3.getUiUrlStem(1234));

    String map = "tcp:[127.0.0.1]:8001,http://127.0.0.1:3333;" +
      "tcp:[127.0.0.2]:8003,http://127.0.0.22:4444";
    ConfigurationUtil.addFromArgs(IdentityManagerImpl.PARAM_UI_STEM_MAP, map);
    assertEquals("http://127.0.0.1:3333", id1.getUiUrlStem(1234));
    assertEquals("http://127.0.0.1:1234", id2.getUiUrlStem(1234));
    assertEquals("http://127.0.0.22:4444", id3.getUiUrlStem(1234));
  }


  private class TestableIdentityManager extends IdentityManagerImpl {
    Map identities = null;

    public Map getIdentityMap() {

      return identities;
    }

    protected IdentityManagerStatus makeStatusAccessor() {
      this.identities = pidStatusMap;
      return new MockIdentityManagerStatus();
    }

    void setIdentity(int proto, PeerIdentity pid) {
      localPeerIdentities[proto] = pid;
    }
  }

  private class MockIdentityManagerStatus
    extends IdentityManagerStatus {
    public MockIdentityManagerStatus() {
      super(idmgr);
    }
    public String getDisplayName() {
      throw new UnsupportedOperationException();
    }

    public void populateTable(StatusTable table) {
      throw new UnsupportedOperationException();
    }

    public boolean requiresKey() {
      throw new UnsupportedOperationException();
    }
  }

}
