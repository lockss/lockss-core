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
package org.lockss.metadata;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Base64;
import org.lockss.config.CurrentConfig;
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
 * A client for the REST web service operation that deletes the metadata of an
 * Archival Unit.
 */
public class DeleteAuItemsClient {
  private static Logger log = Logger.getLogger(DeleteAuItemsClient.class);

  /**
   * Deletes the metadata of an Archival Unit via a REST web service operation.
   * 
   * @param auId
   *          A Sring with the Archival Unit identifier.
   * @return an Integer with the count of items deleted.
   */
  public Integer deleteAuItems(String auId)
      throws UnsupportedEncodingException {
    final String DEBUG_HEADER = "deleteAuItems(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "auId = " + auId);

//    String encodedAuId = URLEncoder.encode(auId, "UTF-8");
//    System.out.println("encodedAuId = '" + encodedAuId + "'");

    // Get the configured REST service location.
    String restServiceLocation =
	CurrentConfig.getParam(MetadataManager.PARAM_MD_REST_SERVICE_LOCATION);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "restServiceLocation = "
	+ restServiceLocation);

    // Get the client connection timeout.
    int timeoutValue = CurrentConfig.getIntParam(
	MetadataManager.PARAM_MD_REST_TIMEOUT_VALUE,
	MetadataManager.DEFAULT_MD_REST_TIMEOUT_VALUE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "timeoutValue = " + timeoutValue);

    // Get the authentication credentials.
    String userName =
	CurrentConfig.getParam(MetadataManager.PARAM_MD_REST_USER_NAME);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "userName = '" + userName + "'");
    String password =
	CurrentConfig.getParam(MetadataManager.PARAM_MD_REST_PASSWORD);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "password = '" + password + "'");

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
    ResponseEntity<Integer> response =
	restTemplate.exchange(restServiceLocation + "/aus/" + auId,
	    HttpMethod.DELETE, new HttpEntity<String>(null, headers),
	    Integer.class);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    Integer mdItemSeq = response.getBody();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "mdItemSeq = " + mdItemSeq);
    return mdItemSeq;
  }
}
