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
package org.lockss.daemon;

import java.nio.charset.Charset;
import java.util.Base64;
import org.lockss.config.CurrentConfig;
import org.lockss.laaws.rs.model.ArtifactPage;
import org.lockss.plugin.PluginManager;
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
 * A client for the Repository REST Service
 * GET /repos/{repository}/artifacts?uri={uri} operation.
 */
public class GetUrlRepositoryPropertiesClient {
  private static Logger log =
      Logger.getLogger(GetUrlRepositoryPropertiesClient.class);

  /**
   * Provides the repository artifact properties for a URL.
   * 
   * @param url
   *          A String with the URL.
   * @return an ArtifactPage with the URL repository artifact metadata.
   * @throws Exception
   *           if there are problems getting the indication.
   */
  public ArtifactPage getUrlRepositoryProperties(String url) throws Exception {
    final String DEBUG_HEADER = "getUrlRepositoryProperties(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);

    // Get the configured REST service location.
    String restServiceLocation = CurrentConfig.getParam(
	PluginManager.PARAM_URL_ARTIFACT_REST_SERVICE_LOCATION);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "restServiceLocation = "
	+ restServiceLocation);

    // Get the indication of whether the URL is cached from the REST service.
    int timeoutValue = CurrentConfig.getIntParam(
	PluginManager.PARAM_URL_ARTIFACT_WS_TIMEOUT_VALUE,
	PluginManager.DEFAULT_URL_ARTIFACT_WS_TIMEOUT_VALUE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "timeoutValue = " + timeoutValue);

    // Get the authentication credentials.
    String userName =
	CurrentConfig.getParam(PluginManager.PARAM_URL_ARTIFACT_WS_USER_NAME);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "userName = '" + userName + "'");
    String password =
	CurrentConfig.getParam(PluginManager.PARAM_URL_ARTIFACT_WS_PASSWORD);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "password = '" + password + "'");

    // Build the REST service URL.
    String restServiceUrl = restServiceLocation.replace("{uri}", url);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Making request to '" + restServiceUrl + "'");

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();
    SimpleClientHttpRequestFactory requestFactory =
	(SimpleClientHttpRequestFactory)restTemplate.getRequestFactory();

    requestFactory.setReadTimeout(1000*timeoutValue);
    requestFactory.setConnectTimeout(1000*timeoutValue);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);

    String credentials = userName + ":" + password;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(Charset.forName("US-ASCII")));
    headers.set("Authorization", authHeaderValue);

    // Make the request to the REST service and get its response.
    ResponseEntity<ArtifactPage> response =
	restTemplate.exchange(restServiceUrl,
	    HttpMethod.GET, new HttpEntity<String>(null, headers),
	    ArtifactPage.class);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);


    ArtifactPage result = response.getBody();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }
}
