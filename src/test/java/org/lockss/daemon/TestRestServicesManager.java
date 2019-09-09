/*

 Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.daemon;

import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockserver.junit.*;
import org.mockserver.client.*;
import static org.mockserver.model.HttpRequest.*;
import static org.mockserver.model.HttpResponse.*;
import org.mockserver.model.Header;
import org.apache.commons.collections4.*;
import org.apache.commons.collections4.multimap.*;
import org.apache.commons.lang3.tuple.*;

import org.lockss.laaws.status.model.ApiStatus;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.rs.exception.*;
import org.lockss.util.*;
import org.lockss.util.time.*;
import org.lockss.util.time.Deadline;
import org.lockss.test.*;
import static  org.lockss.daemon.RestServicesManager.ServiceStatus;

/**
 * Test class for RestServicesManager.
 */
// Use MockServer, which starts a real server, rather than Spring's
// MockRestServiceServer because we want to test timeouts, etc., which the
// latter ignores.
public class TestRestServicesManager extends LockssTestCase4 {
  private static L4JLogger log = L4JLogger.getLogger();

  // injected by the MockServerRule
  private MockServerClient msClient;

  int port;

//   private JMSManager jmsMgr;
  private MyRestServicesManager srvMgr;

  String baseUrl() {
    return "http://localhost:" + port;
  }

  @Rule
  public MockServerRule msRule = new MockServerRule(this);

  @Before
  public void getPort() throws LockssRestException {
    port = msRule.getPort();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    getMockLockssDaemon().registerTestManager(RestServicesManager.class,
					      MyRestServicesManager.class);

    ConfigurationUtil.addFromArgs(RestServicesManager.PARAM_PROBE_INTERVAL,
				  "1s");

//     ConfigurationUtil.addFromArgs(JMSManager.PARAM_START_BROKER, "true");
    getMockLockssDaemon().startManagers(/*JMSManager.class,*/
					RestServicesManager.class);
//     jmsMgr = getMockLockssDaemon().getManagerByType(JMSManager.class);
    srvMgr = (MyRestServicesManager)getMockLockssDaemon().getManagerByType(RestServicesManager.class);
  }

  @After
  public void tearDown() throws Exception {
    getMockLockssDaemon().stopManagers();
    getMockLockssDaemon().stopDaemon();
    super.tearDown();
  }

  String toJson(ApiStatus as) {
    try {
      return new ObjectMapper().writeValueAsString(as);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Json error", e);
    }
  }

  ApiStatus AS_READY = new ApiStatus()
    .setReady(true).setReason("steady").setComponentName("Comp 1");

  ApiStatus AS_NOTREADY = new ApiStatus()
    .setReady(false).setReason("Starting still").setComponentName("Comp 1");

  @Test
  public void testReady() throws LockssRestException {
    msClient
      .when(request()
            .withMethod("GET")
            .withPath("/status"))
      .respond(response()
	       .withStatusCode(200)
	       .withHeaders(new Header("Content-Type",
				       "application/json; charset=utf-8"))
	       .withBody(toJson(AS_READY)));

    ServiceDescr d = ServiceDescr.SVC_CONFIG;
    ServiceBinding b = new MyServiceBinding("localhost", 1111, 2222)
      .setRestStem(baseUrl());

    srvMgr.probeOnce(d, b);
    ServiceStatus stat = srvMgr.getServiceStatus(b);
    log.debug("stat: {}", stat);
    assertTrue(stat.isReady());
    ServiceStatus stat2 =
      srvMgr.waitServiceReady(d, b, Deadline.in(TIMEOUT_SHOULDNT));
    assertTrue(stat2.isReady());
  }

  @Test
  public void testNotReady() throws LockssRestException {
    msClient
      .when(request()
            .withMethod("GET")
            .withPath("/status"))
      .respond(response()
	       .withStatusCode(200)
	       .withHeaders(new Header("Content-Type",
				       "application/json; charset=utf-8"))
	       .withBody(toJson(AS_NOTREADY)));

    ServiceDescr d = ServiceDescr.SVC_CONFIG;
    ServiceBinding b = new MyServiceBinding("localhost", 1111, 2222)
      .setRestStem(baseUrl());

    srvMgr.probeOnce(d, b);
    ServiceStatus stat = srvMgr.getServiceStatus(b);
    log.debug("stat: {}", stat);
    assertFalse(stat.isReady());
    ServiceStatus stat2 = srvMgr.waitServiceReady(d, b, Deadline.EXPIRED);
    assertFalse(stat2.isReady());
  }

  // Functional test of probe threads and progression through various
  // errors and states, mock-like programmed responses (no MockServer)

  LockssRestException e1 =
    new LockssRestNetworkException("lrne1",
				   new java.net.UnknownHostException("unkh"));
  LockssRestException e2 =
    new LockssRestNetworkException("lrne2",
				   new java.net.ConnectException("Connection refused (Connection refused)"));

  ApiStatus apiReady = new ApiStatus().setReady(true);
  ApiStatus apiNotReady = new ApiStatus().setReady(false).setReason("rhyme");
  ServiceDescr sd1 = new ServiceDescr("s1", "1");
  ServiceDescr sd2 = new ServiceDescr("s2", "2");
  ServiceBinding sb1 = new ServiceBinding("h", 123, 1111);
  ServiceBinding sb2 = new ServiceBinding("h", 124, 2222);

  @Test
  public void testProbes() throws LockssRestException {
    Map<ServiceDescr,ServiceBinding> map = MapUtil.map(sd1, sb1, sd2, sb2);
    srvMgr.setDescrs(ListUtil.list(sd1, sd2));
    srvMgr.setBindings(map);
    List<TimeResp> l1 = ListUtil.list(new TimeResp(200, apiReady));
    List<TimeResp> l2 = ListUtil.list(new TimeResp(100, e1),
				      new TimeResp(100, e2),
				      new TimeResp(100, apiNotReady));
    srvMgr.setProbeMap(MapUtil.map(sb1, l1, sb2, l2));
    srvMgr.setStart(true);
    srvMgr.startProbes();
    srvMgr.waitProbes();
    List<ServiceStatus> stats1 = srvMgr.getStats(sb1);
    List<ServiceStatus> stats2 = srvMgr.getStats(sb2);
    log.debug("stats1 {}: ", stats1);
    log.debug("stats2 {}: ", stats2);
    // The getReason() tests may need adjusting for portability
    assertFalse(stats1.get(0).isReady());
    assertEquals("Updating", stats1.get(0).getReason());
    assertTrue(stats1.get(1).isReady());
    assertFalse(stats2.get(0).isReady());
    assertEquals("Updating", stats2.get(0).getReason());
    assertFalse(stats2.get(1).isReady());
    assertEquals("UnknownHostException: unkh", stats2.get(1).getReason());
    assertFalse(stats2.get(2).isReady());
    assertEquals("Connection refused", stats2.get(2).getReason());
    assertFalse(stats2.get(3).isReady());
    assertEquals("rhyme", stats2.get(3).getReason());
  }

  public static class MyRestServicesManager extends RestServicesManager {
    private boolean doStart = false;
    private List<ServiceDescr> descrs;
    private Map<ServiceDescr,ServiceBinding> bindMap;
    private Map<ServiceBinding,List<TimeResp>> probeMap;
    private ListValuedMap<ServiceBinding,ServiceStatus> stats =
      new ArrayListValuedHashMap<>();

    void waitProbes() {
      while (!probeThreads.isEmpty()) {
	TimerUtil.guaranteedSleep(100);
      }
    }

    MyRestServicesManager setStart(boolean val) {
      doStart = val;
      return this;
    }

    void startProbes() {
      if (!doStart) return;
      super.startProbes();
    }

    @Override
    protected List<ServiceDescr> getAllServiceDescrs() {
      if (descrs != null) {
	return descrs;
      }
      return super.getAllServiceDescrs();
    }

    MyRestServicesManager setDescrs(List<ServiceDescr> descrs) {
      this.descrs = descrs;
      return this;
    }

    @Override
    protected ServiceBinding getServiceBinding(ServiceDescr sd) {
      if (bindMap != null) {
	return bindMap.get(sd);
      }
      return super.getServiceBinding(sd);
    }

    MyRestServicesManager setBindings(Map<ServiceDescr,ServiceBinding> map) {
      this.bindMap = map;
      return this;
    }

    MyRestServicesManager setProbeMap(Map<ServiceBinding,List<TimeResp>> map) {
      this.probeMap = map;
      return this;
    }

    @Override
    protected ApiStatus callClientStatus(ServiceBinding binding)
	throws LockssRestException {
      if (probeMap != null) {
	List<TimeResp> lst = probeMap.get(binding);
	if (lst != null) {
	  if (lst.isEmpty()) {
	    throw new RuntimeException("Exit probe thread");
	  }
	  TimeResp tr = lst.remove(0);
	  TimerUtil.guaranteedSleep(tr.getTime());
	  if (tr.isThrow()) {
	    throw tr.getException();
	  } else {
	    return tr.getApiStatus();
	  }
	}
      }
      return super.callClientStatus(binding);
    }

    @Override
    void putServiceStatus(ServiceBinding binding, ServiceStatus stat) {
      stats.put(binding, stat);
      super.putServiceStatus(binding, stat);
    }

    List<ServiceStatus> getStats(ServiceBinding binding) {
      return stats.get(binding);
    }
  }

  class TimeResp {
    long time;
    ApiStatus apiStat;
    LockssRestException ex;

    TimeResp(long time, ApiStatus apiStat) {
      this.time = time;
      this.apiStat = apiStat;
    }

    TimeResp(long time, LockssRestException ex) {
      this.time = time;
      this.ex = ex;
    }
    long getTime() { return time; }
    ApiStatus getApiStatus() { return apiStat; }
    LockssRestException getException() { return ex; }
    boolean isThrow() { return ex != null; }
  }

  class MyServiceBinding extends ServiceBinding {
    String restStem;
    public MyServiceBinding(String host, int uiPort, int restPort) {
      super(host, uiPort, restPort);
    }

    MyServiceBinding setRestStem(String restStem) {
      this.restStem = restStem;
      return this;
    }

    @Override
    public String getRestStem() {
      return restStem != null ? restStem : super.getRestStem();
    }
  }
}
