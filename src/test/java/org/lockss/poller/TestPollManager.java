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

import java.io.*;
import java.util.*;

import org.junit.*;
import org.lockss.config.*;
import org.lockss.daemon.*;
import org.lockss.util.test.FileTestUtil;
import org.lockss.plugin.*;
import org.lockss.poller.v3.*;
import org.lockss.protocol.*;
import org.lockss.repository.RepositoryManager;
import org.lockss.test.*;
import org.lockss.state.*;
import org.lockss.util.*;
import org.lockss.util.time.TimeBase;

/** JUnitTest case for class: org.lockss.poller.PollManager */
public class TestPollManager extends LockssTestCase4 {
  private static final Logger log = Logger.getLogger();

  private static String[] rooturls = {"http://www.test.org",
				      "http://www.test1.org",
				      "http://www.test2.org"};

  private static String urlstr = "http://www.test3.org";
  private static String lwrbnd = "test1.doc";
  private static String uprbnd = "test3.doc";
  private static long testduration = Constants.HOUR;

  protected static MockArchivalUnit testau;
  private MockLockssDaemon theDaemon;
  private RepositoryManager repoMgr;
  private Plugin plugin;


  protected PeerIdentity testID;
  protected V3LcapMessage[] v3Testmsg;
  protected MyPollManager pollmanager;
  protected IdentityManager idmanager;
  private File tempDir;
  private Tdb tdb;

  public void setUp() throws Exception {
    super.setUp();

    String tempDirPath = setUpDiskSpace();
    ConfigurationUtil.addFromArgs(IdentityManager.PARAM_IDDB_DIR, tempDirPath + "iddb",
				  IdentityManager.PARAM_LOCAL_IP, "127.1.2.3");
    plugin = new MockPlugin(getMockLockssDaemon());
    TimeBase.setSimulated();
    initRequiredServices();
    initTestAddr();
    initTestMsg();
    // Many of these tests are prone to timer events running after the test
    // harness has been torn down, leading to spurious NPEs & such in the
    // timer thread.
    setErrorIfTimerThrows(false);
    tdb = new Tdb();
  }


  public void tearDown() throws Exception {
    TimeBase.setReal();
    pollmanager.stopService();
    idmanager.stopService();
    theDaemon.getHashService().stopService();
    theDaemon.getRouterManager().stopService();
    super.tearDown();
  }

  @Test
  public void testConfig() throws Exception {
    assertEquals(ListUtil.list("all"), pollmanager.getAutoPollAuClasses());
    ConfigurationUtil.addFromArgs(PollManager.PARAM_AUTO_POLL_AUS,
				  "Internal;Priority");
    assertEquals(ListUtil.list("internal", "priority"),
		 pollmanager.getAutoPollAuClasses());
  }

  @Test
  public void testGetPollFactoryByVersion() throws Exception {
    PollFactory pfm1 = pollmanager.getPollFactory(-1);
    PollFactory pf0 = pollmanager.getPollFactory(0);
    PollFactory pf1 = pollmanager.getPollFactory(1);
    PollFactory pf2 = pollmanager.getPollFactory(2);
    PollFactory pf3 = pollmanager.getPollFactory(3);
    PollFactory pf4 = pollmanager.getPollFactory(4);
    assertNull(pfm1);
    assertNull(pf0);
    assertNull(pf1);
    assertNull(pf2);
    assertNotNull(pf3);
    assertTrue(pf3 instanceof V3PollFactory);
    assertNull(pf4);
  }

  @Test
  public void testGetPollFactoryByPollSpec() throws Exception {
    CachedUrlSet cus =
      new MockCachedUrlSet(new MockArchivalUnit(plugin),
                           new SingleNodeCachedUrlSetSpec("foo"));
    PollSpec bad1 = new MockPollSpec(cus, -1);
    PollSpec bad2 = new MockPollSpec(cus, 99);
    PollSpec v1 = new MockPollSpec(cus, Poll.V1_CONTENT_POLL);
    PollSpec v3 = new MockPollSpec(cus, Poll.V3_POLL);
    PollFactory pfBad1 = pollmanager.getPollFactory(bad1);
    assertNull(pfBad1);
    PollFactory pfBad2 = pollmanager.getPollFactory(bad2);
    assertNull(pfBad2);
    PollFactory pfV1 = pollmanager.getPollFactory(v1);
    PollFactory pfV3 = pollmanager.getPollFactory(v3);
    assertNull(pfV1);
    assertNotNull(pfV3);
    assertTrue(pfV3 instanceof V3PollFactory);
  }

  // Start by testing the local mock poll factory

  /** Test for getPollsForAu(String auId) */
  @Test
  public void testGetV3PollStatus() throws Exception {
    String auId = testau.getAuId();
    PollManager.V3PollStatusAccessor accessor = 
      pollmanager.getV3Status();
    
    assertEquals(0, accessor.getNumPolls(auId));
    assertEquals(0.0, accessor.getAgreement(auId), 0.001);
    assertEquals(-1, accessor.getLastPollTime(auId));

    addCompletedV3Poll(100000L, 0.99f);
    assertEquals(1, accessor.getNumPolls(auId));
    assertEquals(0.99, accessor.getAgreement(auId), 0.001);
    assertEquals(100000L, accessor.getLastPollTime(auId));
    
    addCompletedV3Poll(987654321L, 1.0f);
    assertEquals(2, accessor.getNumPolls(auId));
    assertEquals(1.0, accessor.getAgreement(auId), 0.001);
    assertEquals(987654321L, accessor.getLastPollTime(auId));
    
    addCompletedV3Poll(1000L, 0.25f);
    assertEquals(3, accessor.getNumPolls(auId));
    assertEquals(0.25, accessor.getAgreement(auId), 0.001);
    assertEquals(1000L, accessor.getLastPollTime(auId));
  }
  
  private void addCompletedV3Poll(long timestamp, 
                                  float agreement) throws Exception {
    PollSpec spec = new MockPollSpec(testau, rooturls[0], lwrbnd, uprbnd,
                                     Poll.V3_POLL);
    V3Poller poll = new V3Poller(spec, theDaemon, testID, "akeyforthispoll",
                                 1234567, "SHA-1");
    pollmanager.addPoll(poll);
    poll.stopPoll();
    PollManager.V3PollStatusAccessor v3status =
      pollmanager.getV3Status();
    v3status.incrementNumPolls(testau.getAuId());
    v3status.setAgreement(testau.getAuId(), agreement);
    v3status.setLastPollTime(testau.getAuId(), timestamp);
  }
  
  private Poll makeTestV3Voter() throws Exception {
    PollSpec spec = new MockPollSpec(testau, rooturls[0], lwrbnd, uprbnd,
                                     Poll.V3_POLL);

    V3LcapMessage pollMsg = 
      new V3LcapMessage(testau.getAuId(), "akeyforthispoll", "3",
                        ByteArray.makeRandomBytes(20),
                        ByteArray.makeRandomBytes(20),
                        V3LcapMessage.MSG_POLL,
                        TimeBase.nowMs() + 50000, 
                        testID, tempDir, theDaemon);
    
    pollMsg.setVoteDuration(20000);
      
    return new V3Voter(theDaemon, pollMsg);
  }
  
  MockArchivalUnit[] makeMockAus(int n) {
    MockArchivalUnit[] res = new MockArchivalUnit[n];
    for (int ix = 0; ix < n; ix++) {
      res[ix] = newMockArchivalUnit("mau" + ix);
      res[ix].setName("Mock " + ix);
    }
    return res;
  }

  MockArchivalUnit newMockArchivalUnit(String auid) {
    MockArchivalUnit mau = new MockArchivalUnit(plugin, auid);
    return mau;
  }

  void setAu(MockArchivalUnit mau,
	     String year,
	     long lastPollStart, long lastTopLevelPoll, int lastPollResult,
	     long pollDuration, double agreement) 
      throws Exception {
    MockAuState aus = AuTestUtil.setUpMockAus(mau);
    aus.setLastCrawlTime(100);
    aus.setLastPollStart(lastPollStart);
    aus.setLastToplevalPoll(lastTopLevelPoll);
    aus.setLastPollResult(lastPollResult);
    aus.setPollDuration(pollDuration);
    aus.setV3Agreement(agreement);

    Properties tprops = new Properties();
    tprops.put("title", "It's " + mau.getName());
    tprops.put("journalTitle", "jtitle " + mau.getName());
    tprops.put("plugin", "Plug1");
    tprops.put("pluginVersion", "4");
    tprops.put("param.1.key", "volume");
    tprops.put("param.1.value", "vol_" + mau.getName());
    tprops.put("param.2.key", "year");
    tprops.put("param.2.value", "2010");
    tprops.put("attributes.year", year);

    TdbAu tau = tdb.addTdbAuFromProperties(tprops);
    mau.setTdbAu(tau);
  }	   

  void registerAus(MockArchivalUnit[] aus) {
    List lst = ListUtil.fromArray(aus);
    List<MockArchivalUnit> rand = CollectionUtil.randomPermutation(lst);
    for (MockArchivalUnit mau : rand) {
      PluginTestUtil.registerArchivalUnit(plugin, mau);
    }
  }    

  static final int C = V3Poller.POLLER_STATUS_COMPLETE;
  static final int NC = V3Poller.POLLER_STATUS_NO_QUORUM;

  @Test
  public void testPollQueue() throws Exception {
    testau.setShouldCallTopLevelPoll(false);

    Properties p = new Properties();
    p.put(PollManager.PARAM_REBUILD_POLL_QUEUE_INTERVAL, "");
    p.put(PollManager.PARAM_POLL_QUEUE_MAX, "8");
    p.put(PollManager.PARAM_POLL_INTERVAL_AGREEMENT_CURVE,
	  "[50,75],[50,500]");
    p.put(PollManager.PARAM_POLL_INTERVAL_AGREEMENT_LAST_RESULT, "1;6");
    p.put(PollManager.PARAM_TOPLEVEL_POLL_INTERVAL, "300");
    p.put(PollManager.PARAM_MIN_POLL_ATTEMPT_INTERVAL, "1");
    p.put(PollManager.PARAM_MIN_TIME_BETWEEN_ANY_POLL, "1");

    ConfigurationUtil.addFromProps(p);
    theDaemon.setAusStarted(true);
    TimeBase.setSimulated(1000);

    MockArchivalUnit[] aus = makeMockAus(16);
    registerAus(aus);

    //    setAu(mau, lastStart, lastComplete, lastResult, duration, agmnt);
    setAu(aus[0], "2000", 900, 950,  C, 5, .9);
    setAu(aus[1], "2001", 900, 500, NC, 5, .9);
    setAu(aus[2], "2002", 900, 950,  C, 5, .2);
    setAu(aus[3], "2003", 900, 500, NC, 5, .2);

    setAu(aus[4], "2004", 850, 950,  C, 10, .9);
    setAu(aus[5], "2005", 850, 500, NC, 10, .9);
    setAu(aus[6], "2006", 850, 950,  C, 10, .2);
    setAu(aus[7], "2007", 850, 500, NC, 10, .2);

    setAu(aus[ 8], "2008", 650, 750,  C, 10, .9);
    setAu(aus[ 9], "2009", 650, 400, NC, 10, .9);
    setAu(aus[10], "2010", 650, 750,  C, 10, .2);
    setAu(aus[11], "2011", 650, 400, NC, 10, .2);

    setAu(aus[12], "2012", 350, 450,  C, 10, .9);
    setAu(aus[13], "2013", 350, 100, NC, 10, .9);
    setAu(aus[14], "2014", 350, 450,  C, 10, .2);
    setAu(aus[15], "2015", 350, 100, NC, 10, .2);

    String p1 = "TCP:[127.0.0.1]:12";
    String p2 = "TCP:[127.0.0.2]:12";
    String p3 = "TCP:[127.0.0.3]:12";
    String atRiskString =
      aus[0].getAuId() + "," + p1 + "," + p2 + "," + p3 + ";" +
      aus[7].getAuId() + "," + p1 + ";" +
      aus[12].getAuId() + "," + p1 + "," + p2;

    pollmanager.pollQueue.rebuildPollQueue();

    List exp = ListUtil.list(aus[14], aus[10], aus[13], aus[15],
			     aus[11], aus[9], aus[1], aus[3],
			     aus[5], aus[7], aus[12]);
    assertEquals(exp, weightOrder());
    List<ArchivalUnit> queue = pollmanager.pollQueue.getPendingQueueAus();
    assertEquals(8, queue.size());
    assertTrue(queue+"", exp.containsAll(queue));

    p.put(V3Poller.PARAM_AT_RISK_AU_INSTANCES, atRiskString);
    p.put(PollManager.PARAM_POLL_WEIGHT_AT_RISK_PEERS_CURVE,
	  "[0,1],[1,2],[2,4]");
    ConfigurationUtil.addFromProps(p);

    pollmanager.pollQueue.rebuildPollQueue();

    List exp2 = ListUtil.list(aus[14], aus[12], aus[7], aus[10],
			      aus[13], aus[15], aus[11], aus[9],
			      aus[1], aus[3], aus[5]);
    assertEquals(exp2, weightOrder());

    p.put(PollManager.PARAM_POLL_INTERVAL_AT_RISK_PEERS_CURVE,
	  "[0,-1],[2,-1],[3,1]");
    ConfigurationUtil.addFromProps(p);

    pollmanager.pollQueue.rebuildPollQueue();

    List exp3 = ListUtil.list(aus[0], aus[14], aus[12], aus[7], aus[10],
			      aus[13], aus[15], aus[11], aus[9],
			      aus[1], aus[3], aus[5]);
    assertEquals(exp3, weightOrder());

    // enqueue a high priority poll, ensure it's now first
    PollSpec spec = new PollSpec(aus[2].getAuCachedUrlSet(), Poll.V3_POLL);
    pollmanager.enqueueHighPriorityPoll(aus[2], spec);
    pollmanager.pollQueue.rebuildPollQueue();
    assertEquals(aus[2], pollmanager.pollQueue.getPendingQueueAus().get(0));

    // Add an auid->priority map moving mau11 and mau5 to the front.
    ConfigurationUtil.addFromArgs(PollManager.PARAM_POLL_PRIORITY_AUID_MAP,
				  "mau5,50.0;mau11,100");

    pollmanager.pollQueue.rebuildPollQueue();
    List exp4 = ListUtil.list(aus[11], aus[5], aus[0], aus[14], aus[12],
			      aus[7], aus[10], aus[13], aus[15], aus[9],
			      aus[1], aus[3]);
    assertEquals(exp4, weightOrder());

    // Add a au xpath->priority map.
    ConfigurationUtil.addFromArgs(PollManager.PARAM_POLL_PRIORITY_AU_MAP,
				  "[tdbAu/attrs/year='2001'],2.0;" +
				  "[tdbAu/attrs/year='2005'],2.0");
    
    pollmanager.pollQueue.rebuildPollQueue();
    List exp5 = ListUtil.list(aus[11], aus[5], aus[0], aus[14], aus[12],
			      aus[1],
			      aus[7], aus[10], aus[13], aus[15], aus[9],
			      aus[3]);
    assertEquals(exp5, weightOrder());

    // Add a more extreme au xpath->priority map.
    ConfigurationUtil.addFromArgs(PollManager.PARAM_POLL_PRIORITY_AU_MAP,
				  "[tdbAu/attrs/year='2001'],3.0;" +
				  "[tdbAu/attrs/year='2005'],1.3");
    
    pollmanager.pollQueue.rebuildPollQueue();
    List exp6 = ListUtil.list(aus[11], aus[5], aus[0], aus[14], aus[1],
			      aus[12],
			      aus[7], aus[10], aus[13], aus[15], aus[9],
			      aus[3]);
    assertEquals(exp6, weightOrder());
}

  List<ArchivalUnit> weightOrder() {
    final Map<ArchivalUnit,PollManager.PollWeight> weightMap =
      pollmanager.getWeightMap();
    assertNotNull(weightMap);
    ArrayList<ArchivalUnit> queued = new ArrayList(weightMap.keySet());
    //log.debug("weightMap: " + weightMap.toString());
    Collections.sort(queued, new Comparator<ArchivalUnit>() {
	public int compare(ArchivalUnit au1,
			   ArchivalUnit au2) {
	  int res = - weightMap.get(au1).value().compareTo(weightMap.get(au2).value()); 
	  if (res == 0) {
	    res = au1.getAuId().compareTo(au2.getAuId());
	  }
	  return res;
	}});
    return queued;
  }

  List<ArchivalUnit> ausOfReqs(List<PollManager.PollReq> reqs) {
    List<ArchivalUnit> res = new ArrayList();
    for (PollManager.PollReq req : reqs) {
      res.add(req.getAu());
    }
    return res;
  }

  @Test
  public void testAtRiskMap() throws Exception {
    String p1 = "TCP:[127.0.0.1]:12";
    String p2 = "TCP:[127.0.0.2]:12";
    String p3 = "TCP:[127.0.0.3]:12";
    String p4 = "TCP:[127.0.0.4]:12";
    String p5 = "TCP:[127.0.0.5]:12";
  
    PeerIdentity peer1 = idmanager.stringToPeerIdentity(p1);
    PeerIdentity peer2 = idmanager.stringToPeerIdentity(p2);
    PeerIdentity peer3 = idmanager.stringToPeerIdentity(p3);
    PeerIdentity peer4 = idmanager.stringToPeerIdentity(p4);
    PeerIdentity peer5 = idmanager.stringToPeerIdentity(p5);

    String auid1 = "org|lockss|plugin|absinthe|AbsinthePlugin&base_url~http%3A%2F%2Fabsinthe-literary-review%2Ecom%2F&year~2003";
    String auid2 = "org|lockss|plugin|absinthe|AbsinthePlugin&base_url~http%3A%2F%2Fabsinthe-literary-review%2Ecom%2F&year~2004";
    MockArchivalUnit mau1 = new MockArchivalUnit(auid1);
    MockArchivalUnit mau2 = new MockArchivalUnit(auid2);

    String atRiskString =
      auid1 + "," + p1 + "," + p2 + "," + p5 + ";" +
      auid2 + "," + p3 + "," + p2 + "," + p4;

    ConfigurationUtil.addFromArgs(V3Poller.PARAM_AT_RISK_AU_INSTANCES,
				  atRiskString);
    assertEquals(SetUtil.set(peer1, peer2, peer5),
		 pollmanager.getPeersWithAuAtRisk(mau1));
    assertEquals(SetUtil.set(peer2, peer3, peer4),
		 pollmanager.getPeersWithAuAtRisk(mau2));
  }

  @Test
  public void testGetNoAuSet() throws Exception {
    MockPlugin plugin = new MockPlugin(theDaemon);
    String auid1 = "auid111";
    MockArchivalUnit mau1 = new MockArchivalUnit(plugin, auid1);
    String auid2 = "auid222";
    MockArchivalUnit mau2 = new MockArchivalUnit(plugin, auid2);

    DatedPeerIdSet s1 = pollmanager.getNoAuPeerSet(mau1);
    DatedPeerIdSet s2 = pollmanager.getNoAuPeerSet(mau2);
    DatedPeerIdSet s3 = pollmanager.getNoAuPeerSet(mau1);
    assertNotSame(s1, s2);
    assertSame(s1, s3);
  }

  @Test
  public void testAgeNoAuSet() throws Exception {
    String p1 = "TCP:[127.0.0.1]:12";
    String p2 = "TCP:[127.0.0.2]:12";
    PeerIdentity peer1 = idmanager.stringToPeerIdentity(p1);
    PeerIdentity peer2 = idmanager.stringToPeerIdentity(p2);
    List<PeerIdentity> both = ListUtil.list(peer1, peer2);

    ConfigurationUtil.addFromArgs(PollManager.PARAM_NO_AU_RESET_INTERVAL_CURVE,
 				  "[2000,500],[10000,500],[10000,5000]");

    TimeBase.setSimulated(1000);
    String auid = "auid111";
    MockPlugin plugin = new MockPlugin(theDaemon);
    MockArchivalUnit mau = new MockArchivalUnit(plugin, auid);
    MockAuState maus = AuTestUtil.setUpMockAus(mau);
    File file = FileTestUtil.tempFile("noau");
    DatedPeerIdSet noAuSet = new DatedPeerIdSetImpl(idmanager);
    assertTrue(noAuSet.isEmpty());
    assertTrue(noAuSet.getDate() < 0);
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.isEmpty());
    assertTrue(noAuSet.getDate() < 0);
    maus.setAuCreationTime(1000);
    noAuSet.addAll(both);
    noAuSet.setDate(TimeBase.nowMs());
    assertTrue(noAuSet.containsAll(both));

    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.containsAll(both));

    TimeBase.step(1000);
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.isEmpty());
    noAuSet.addAll(both);
    noAuSet.setDate(TimeBase.nowMs());
    assertTrue(noAuSet.containsAll(both));
    TimeBase.step(499);
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.containsAll(both));
    TimeBase.step(1);
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.isEmpty());
    TimeBase.step(12000);
    noAuSet.addAll(both);
    noAuSet.setDate(TimeBase.nowMs());
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.containsAll(both));
    TimeBase.step(4999);
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.containsAll(both));
    TimeBase.step(1);
    pollmanager.ageNoAuSet(mau, noAuSet);
    assertTrue(noAuSet.isEmpty());
  }

  //  Local mock classes

  // MyPollManager allows us to override the PollFactory
  // used for a particular protocol,  and to override the
  // sendMessage() method.
  static class MyPollManager extends PollManager {
    LcapMessage msgSent = null;
    Map weightMap;

    public void setPollFactory(int i, PollFactory fact) {
      pf[i] = fact;
    }

    @Override
      protected List<ArchivalUnit> weightedRandomSelection(Map<ArchivalUnit, PollManager.PollWeight> weightMap, int n) {
      //log.debug("weightMap: " + weightMap);
      this.weightMap = weightMap;
      return super.weightedRandomSelection(weightMap, n);
    }

    Map getWeightMap() {
      return weightMap;
    }
  }

  private void initRequiredServices() {
    theDaemon = getMockLockssDaemon();
    pollmanager = new MyPollManager();
    pollmanager.initService(theDaemon);
    theDaemon.setPollManager(pollmanager);
    idmanager = theDaemon.getIdentityManager();

    theDaemon.getPluginManager();
    testau =
      (MockArchivalUnit)PollTestPlugin.PTArchivalUnit.createFromListOfRootUrls(rooturls);
    testau.setPlugin(new MockPlugin(theDaemon));
    PluginTestUtil.registerArchivalUnit(testau);

    repoMgr = theDaemon.getRepositoryManager();
    repoMgr.startService();

    Properties p = new Properties();
    addRequiredConfig(p);
    ConfigurationUtil.setCurrentConfigFromProps(p);

    theDaemon.getSchedService().startService();
    theDaemon.getHashService().startService();
    theDaemon.getRouterManager().startService();

    pollmanager.startService();
    idmanager.startService();
  }

  private void addRequiredConfig(Properties p) {
    String tempDirPath = null;
    try {
      tempDirPath = getTempDir().getAbsolutePath() + File.separator;
    }
    catch (IOException ex) {
      fail("unable to create a temporary directory");
    }
    p.setProperty(IdentityManager.PARAM_IDDB_DIR, tempDirPath + "iddb");
    p.setProperty(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST, tempDirPath);
    p.setProperty(IdentityManager.PARAM_LOCAL_IP, "127.0.0.1");
  }

  private void initTestAddr() {
    try {
      testID = theDaemon.getIdentityManager().stringToPeerIdentity("127.0.0.1");
    }
    catch (IdentityManager.MalformedIdentityKeyException ex) {
      fail("can't open test host");
    }
  }

  private void initTestMsg() throws Exception {
 // V3 Messages.
    v3Testmsg = new V3LcapMessage[1];
//    PollSpec v3Spec = new MockPollSpec(testau, rooturls[0], null, null,
//                                       Poll.V3_POLL);
    v3Testmsg[0] = new V3LcapMessage(testau.getAuId(), "testpollid", "2",
                                     ByteArray.makeRandomBytes(20),
                                     ByteArray.makeRandomBytes(20),
                                     V3LcapMessage.MSG_POLL,
                                     12345678, testID, tempDir, theDaemon);
    v3Testmsg[0].setArchivalId(testau.getAuId());
  }

}
