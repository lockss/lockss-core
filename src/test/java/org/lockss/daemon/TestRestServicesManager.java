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
import java.util.concurrent.TimeUnit;
import org.junit.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mockserver.junit.*;
import org.mockserver.client.*;
import static org.mockserver.model.HttpRequest.*;
import static org.mockserver.model.HttpResponse.*;
import org.mockserver.model.Header;

import org.lockss.laaws.status.model.ApiStatus;
import org.lockss.app.*;
import org.lockss.log.*;
import org.lockss.rs.exception.*;
import org.lockss.test.LockssTestCase4;

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
  private RestServicesManager srvMgr;

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
//     ConfigurationUtil.addFromArgs(JMSManager.PARAM_START_BROKER, "true");
    getMockLockssDaemon().startManagers(/*JMSManager.class,*/
					RestServicesManager.class);
//     jmsMgr = getMockLockssDaemon().getManagerByType(JMSManager.class);
    srvMgr = getMockLockssDaemon().getManagerByType(RestServicesManager.class);
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
    RestServicesManager.ServiceStatus stat = srvMgr.getServiceStatus(b);
    log.debug("stat: {}", stat);
    assertTrue(stat.isReady());
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
    RestServicesManager.ServiceStatus stat = srvMgr.getServiceStatus(b);
    log.debug("stat: {}", stat);
    assertFalse(stat.isReady());
  }

//   @Test
//   public void testConnectTimeout() throws LockssRestException {
//     msClient
//       .when(request()
//             .withMethod("GET")
//             .withPath("/status")
// 	    )
//       .respond(response()
// 	       .withStatusCode(200)
// 	       .withHeaders(new Header("Content-Type",
// 				       "application/json; charset=utf-8"))
// 	       .withBody(toJson(AS_READY))
// 	       );

//     RestStatusClient rsc = new RestStatusClient("http://www.lockss.org:45678",
// 						1, 60);

//     try {
//       ApiStatus as = rsc.getStatus();
//       fail("Expected read timeout to throw but returned: " + as);
//     } catch (LockssRestNetworkException e) {
//       assertMatchesRE("Cannot get status.*connect timed out", e.getMessage());
//     } catch (LockssRestException e) {
//       fail("Should have thrown LockssRestNetworkException but threw LockssRestException: " + e);
//     }
//   }

//   @Test
//   public void testResponseTimeout() throws LockssRestException {
//     msClient
//       .when(request()
//             .withMethod("GET")
//             .withPath("/status")
// 	    )
//       .respond(response()
// 	       .withStatusCode(200)
// 	       .withHeaders(new Header("Content-Type",
// 				       "application/json; charset=utf-8"))
// 	       .withDelay(TimeUnit.SECONDS, 10)
// 	       .withBody(toJson(AS_READY))
// 	       );

//     RestStatusClient rsc = new RestStatusClient(baseUrl(), 60, 1);

//     try {
//       ApiStatus as = rsc.getStatus();
//       fail("Expected read timeout to throw but returned: " + as);
//     } catch (LockssRestNetworkException e) {
//       assertMatchesRE("Cannot get status.*Read timed out", e.getMessage());
//     } catch (LockssRestException e) {
//       fail("Should have thrown LockssRestNetworkException but threw LockssRestException: " + e);
//     }
//   }

//   @Test
//   public void test500() throws LockssRestException {
//     msClient
//       .when(request()
//             .withMethod("GET")
//             .withPath("/status"))
//       .respond(response()
// 	       .withStatusCode(500)
// 	       .withReasonPhrase("English Pointer Exception")
// 	       );

//     RestStatusClient rsc = new RestStatusClient(baseUrl());

//     try {
//       ApiStatus as = rsc.getStatus();
//       fail("Expected 500 response to throw but returned: " + as);
//     } catch (LockssRestHttpException e) {
//       assertMatchesRE("Cannot get status", e.getMessage());
//       assertEquals(500, e.getHttpStatusCode());
//       // XXX We always get the default message for the status code
// //       assertEquals("English Pointer Exception", e.getHttpStatusMessage());
//     } catch (LockssRestException e) {
//       fail("Should have thrown LockssRestHttpException but threw LockssRestException: " + e);
//     }
//   }

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
