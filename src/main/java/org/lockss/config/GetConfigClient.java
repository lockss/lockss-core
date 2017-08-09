/*

 Copyright (c) 2017 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.config;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Properties;
import org.lockss.util.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * A client for the Configuration REST web service operation that provides the
 * current configuration.
 */
public class GetConfigClient {
  private static Logger log = Logger.getLogger(GetConfigClient.class);

  private String serviceLocation = null;
  private String serviceUser = null;
  private String servicePassword = null;
  private Integer serviceTimeout = null;

  /**
   * Constructor.
   * 
   * @param location
   *          A String with the location of the REST web service.
   * @param userName
   *          A String with the name of the user that performs the operation.
   * @param password
   *          A String with the password of the user that performs the
   *          operation.
   * @param timeoutValue
   *          An Integer with the connection and socket timeout, in seconds.
   */
  public GetConfigClient(String location, String userName, String password,
      Integer timeoutValue) {
    serviceLocation = location;
    serviceUser = userName;
    servicePassword = password;
    serviceTimeout = timeoutValue;
  }

  /**
   * Retrieves the current configuration from a Configuration REST web service.
   * 
   * @return a Configuration with the current configuration.
   */
  public Configuration getConfig() {
    final String DEBUG_HEADER = "getConfig(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "serviceLocation = " + serviceLocation);
      log.debug2(DEBUG_HEADER + "serviceUser = " + serviceUser);
      //log.debug2(DEBUG_HEADER + "servicePassword = " + servicePassword);
      log.debug2(DEBUG_HEADER + "serviceTimeout = " + serviceTimeout);
    }

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();
    SimpleClientHttpRequestFactory requestFactory =
	(SimpleClientHttpRequestFactory)restTemplate.getRequestFactory();

    requestFactory.setReadTimeout(1000*serviceTimeout);
    requestFactory.setConnectTimeout(1000*serviceTimeout);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    String credentials = serviceUser + ":" + servicePassword;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(Charset.forName("US-ASCII")));
    headers.set("Authorization", authHeaderValue);

    // Make the request to the REST service and get its response.
    ResponseEntity<Properties> response =
	restTemplate.exchange(serviceLocation + "/config",
	    HttpMethod.GET, new HttpEntity<String>(null, headers),
	    Properties.class);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    Properties result = response.getBody();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "result = " + result);

    // Convert the response into a the expected result.
    Configuration config = ConfigManager.newConfiguration();

    for (Object key : result.keySet()) {
      config.put((String)key,  (String)result.get((String)key));
    }

    if (log.isDebug2()) log.debug2("config = " + config);
    return config;
  }
}
