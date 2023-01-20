/*
 * $Id$
 */

/*

 Copyright (c) 2013 Board of Trustees of Leland Stanford Jr. University,
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

/**
 * A base client for the daemon status web service.
 */
package org.lockss.ws.status.client;

import java.net.URL;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import org.lockss.ws.AllServicesBaseClient;
import org.lockss.ws.status.DaemonStatusService;

public abstract class DaemonStatusServiceBaseClient extends
    AllServicesBaseClient {
  private static final String ADDRESS_LOCATION =
      "http://localhost:8081/ws/DaemonStatusService?wsdl";
  private static final String TARGET_NAMESPACE = "http://status.ws.lockss.org/";
  private static final String SERVICE_NAME = "DaemonStatusServiceImplService";

  private static final String TIMEOUT_KEY =
      "com.sun.xml.internal.ws.request.timeout";

  // The length of the client connection timeout in seconds.
  private static final int TIMEOUT_VALUE = 600;

  /**
   * @throws Exception
   */
  protected DaemonStatusService getProxy() throws Exception {
    super.authenticate();
    Service service = Service.create(new URL(ADDRESS_LOCATION), new QName(
	TARGET_NAMESPACE, SERVICE_NAME));

    DaemonStatusService port = service.getPort(DaemonStatusService.class);

    // Set the client connection timeout.
    ((javax.xml.ws.BindingProvider) port).getRequestContext().put(TIMEOUT_KEY,
	Integer.valueOf(TIMEOUT_VALUE*1000));

    return port;
  }
}
