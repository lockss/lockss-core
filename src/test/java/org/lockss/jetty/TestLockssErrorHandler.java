/*

Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University.
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
package org.lockss.jetty;

import java.io.StringWriter;
import org.junit.Test;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockHttpRequest;
import org.mortbay.util.URI;

/**
 * Regression tests for the two ways this daemon used to disclose its server
 * product and version, which a penetration test flagged: the {@code Server}
 * response header, and the footer of Jetty error pages.
 * <p>
 * The equivalent fix in the 1.x daemon is commit {@code 0093b3d8}, which
 * removed the "powered by Jetty" link from {@link LockssErrorHandler} and set
 * {@code org.mortbay.http.Version.paranoid}. These tests pin both halves down
 * in 2.x so neither can quietly come back.
 */
public class TestLockssErrorHandler extends LockssTestCase4 {

  /**
   * The error page must not name the server product or its version. It is the
   * page an unauthenticated caller is most likely to see, since it is produced
   * for 404s and 403s.
   */
  @Test
  public void testErrorPageDoesNotDiscloseTheServer() throws Exception {
    String page = renderErrorPage(404, "Not Found");

    assertNoServerDisclosure(page);

    // The page is still useful: it says what went wrong and for which URI.
    assertTrue("the error page should still report the status, was:\n" + page,
        page.contains("404"));
    assertTrue("the error page should still report the URI, was:\n" + page,
        page.contains("/no/such/path"));
  }

  /**
   * Same for a 500, whose message is built from the failing exception's class
   * name and so takes a different path through the handler.
   */
  @Test
  public void testServerErrorPageDoesNotDiscloseTheServer() throws Exception {
    assertNoServerDisclosure(renderErrorPage(500, "Internal Server Error"));
  }

  /**
   * The {@code Server} response header is suppressed by
   * {@code org.mortbay.http.Version.paranoid}, which Jetty reads once in
   * {@code org.mortbay.http.Version}'s own static initializer and then caches
   * for the life of the JVM.
   * <p>
   * That caching is why the property is set in {@link JettyManager}, the common
   * base of every class that starts a Jetty, rather than in one subclass: a
   * Jetty class loaded before the property is set latches the version back on
   * permanently, and the proxy managers extend JettyManager directly without
   * going through {@code BaseServletManager}.
   * <p>
   * This asserts the property, not {@code Version.isParanoid()}, deliberately.
   * The property is what this code controls and is deterministic; whether
   * Version happens to have latched it depends on what else the surrounding
   * JVM loaded first, which a unit test cannot govern.
   */
  @Test
  public void testJettyManagerSuppressesTheServerVersionHeader()
      throws Exception {
    // Force the class to load without needing an instance; its static
    // initializer is what sets the property.
    Class.forName(JettyManager.class.getName());

    assertTrue("loading JettyManager must set the Jetty paranoid property, so"
            + " that Jetty omits its version from the Server response header",
        Boolean.getBoolean("org.mortbay.http.Version.paranoid"));
  }

  /**
   * Renders an error page the way {@code handle()} does, via the same
   * {@code writeErrorPage()} it calls.
   */
  private String renderErrorPage(int code, String message) throws Exception {
    MockHttpRequest request = new MockHttpRequest();
    request.setURI(new URI("/no/such/path"));

    StringWriter out = new StringWriter();
    new LockssErrorHandler("daemon").writeErrorPage(request, out, code, message);

    return out.toString();
  }

  /**
   * Fails if the page names the server product or anything version-shaped
   * alongside it.
   */
  private void assertNoServerDisclosure(String page) {
    String lower = page.toLowerCase();

    assertFalse("the error page must not name Jetty, was:\n" + page,
        lower.contains("jetty"));
    assertFalse("the error page must not name Tomcat, was:\n" + page,
        lower.contains("tomcat"));
    assertFalse("the error page must not link to the server vendor, was:\n"
            + page, lower.contains("mortbay"));
    assertFalse("the error page must not advertise what powers it, was:\n"
            + page, lower.contains("powered by"));
  }
}
