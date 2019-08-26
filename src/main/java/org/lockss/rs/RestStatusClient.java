/*

 Copyright (c) 2018-2019 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.rs;

import java.net.URI;
import org.springframework.http.*;
import org.springframework.http.client.*;
import org.springframework.web.client.*;
import org.springframework.web.util.*;
import org.lockss.log.*;
import org.lockss.util.Constants;
import org.lockss.rs.exception.*;
import org.lockss.laaws.status.model.ApiStatus;

public class RestStatusClient /*extends BaseClient*/ {
  private static L4JLogger log = L4JLogger.getLogger();

  private String serviceUrl;
  private long connectTimeout;
  private long readTimeout;

  /**
   * Constructor.
   * 
   * @param restConfigServiceUrl
   *          A String with the information necessary to access the
   *          REST web service.
   */
  public RestStatusClient(String serviceUrl) {
    this(serviceUrl, 10 * Constants.SECOND, 30 * Constants.SECOND);
  }

  public RestStatusClient(String serviceUrl,
			  long connectTimeout, long readTimeout) {
    this.serviceUrl = serviceUrl;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
  }

  public ApiStatus getStatus() throws LockssRestException  {
    String template = serviceUrl + "/status";

    // Create the URI of the request to the REST service.
    UriComponents uriComponents =
      UriComponentsBuilder.fromUriString(template).build();

    UriComponentsBuilder builder =
      UriComponentsBuilder.newInstance().uriComponents(uriComponents);

    URI uri = builder.build().encode().toUri();
    log.debug2("uri = " + uri);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();

//     // Set the authentication credentials.
//     setAuthenticationCredentials(requestHeaders);

    // Create the request entity.
    HttpEntity<String> requestEntity =
	new HttpEntity<String>(null, requestHeaders);

    try {
      ResponseEntity<ApiStatus> response =
	RestUtil.callRestService(createRestTemplate(), uri, HttpMethod.GET,
				 requestEntity, ApiStatus.class,
				 "Can't get status");
      int status = response.getStatusCodeValue();
      log.debug2("status = " + status);
      ApiStatus result = response.getBody();
      log.debug2("result = " + result);
      return result;
    } catch (RuntimeException e) {
      throw new LockssRestNetworkException(e);
    }
  }

  private RestTemplate createRestTemplate() {
    final String DEBUG_HEADER = "createRestTemplate(): ";

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();

    // Do not throw exceptions on non-success response status codes.
    restTemplate.setErrorHandler(new DefaultResponseErrorHandler(){
      protected boolean hasError(HttpStatus statusCode) {
	return false;
      }
    });

    // Specify the timeouts.
    SimpleClientHttpRequestFactory requestFactory =
	(SimpleClientHttpRequestFactory)restTemplate.getRequestFactory();

    requestFactory.setConnectTimeout((int)connectTimeout);
    requestFactory.setReadTimeout((int)readTimeout);

    return restTemplate;
  }

  private RestTemplate getRestTemplate() {
    // Specifying the factory is necessary to get Spring support for PATCH
    // operations.
    RestTemplate restTemplate =
	new RestTemplate(new HttpComponentsClientHttpRequestFactory());

    // Do not throw exceptions on non-success response status codes.
    restTemplate.setErrorHandler(new DefaultResponseErrorHandler(){
      protected boolean hasError(HttpStatus statusCode) {
	return false;
      }
    });

    return restTemplate;
  }
}
