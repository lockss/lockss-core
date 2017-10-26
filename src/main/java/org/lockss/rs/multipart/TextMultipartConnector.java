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
package org.lockss.rs.multipart;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.lockss.util.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * Utility to simplify calling a REST service that returns an HTTP Multipart
 * response where the payload is text.
 */
public class TextMultipartConnector {
  private static Logger log = Logger.getLogger(TextMultipartConnector.class);

  private URI uri;
  private HttpHeaders requestHeaders;

  /**
   * Constructor.
   * 
   * @param uri
   *          A URI defining the REST service operation to be called.
   * @param requestHeaders
   *          An HttpHeaders with the headers of the request to be made.
   */
  public TextMultipartConnector(URI uri, HttpHeaders requestHeaders) {
    this.uri = uri;
    this.requestHeaders = requestHeaders;
  }

  /**
   * Performs the request.
   *
   * @return a TextMultipartResponse with the response.
   * @throws IOException
   *           if there are problems.
   */
  public TextMultipartResponse request() throws IOException {
    return request(60, 60);
  }

  /**
   * Performs the request.
   * 
   * @param connectTimeout
   *          An int with the connection timeout in seconds.
   * @param readTimeout
   *          An int with the read timeout in seconds.
   * @return a TextMultipartResponse with the response.
   * @throws IOException
   *           if there are problems.
   */
  public TextMultipartResponse request(int connectTimeout, int readTimeout)
      throws IOException {
    final String DEBUG_HEADER = "request(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "connectTimeout = " + connectTimeout);
      log.debug2(DEBUG_HEADER + "readTimeout = " + readTimeout);
    }

    // Initialize the request to the REST service.
    RestTemplate restTemplate = createRestTemplate(connectTimeout, readTimeout);

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Making request to '" + uri + "'...");

    // Make the request to the REST service and get its response.
    ResponseEntity<String> response = restTemplate.exchange(uri,
	HttpMethod.GET, new HttpEntity<String>(null, requestHeaders),
	String.class);

    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "Obtained response.");

    // Parse the response and return it.
    return parseResponse(response);
  }

  /**
   * Creates the REST template for the request.
   * 
   * @param connectTimeout
   *          An int with the connection timeout in seconds.
   * @param readTimeout
   *          An int with the read timeout in seconds.
   * @return a RestTemplate with the REST template for the request.
   */
  private RestTemplate createRestTemplate(int connectTimeout, int readTimeout) {
    final String DEBUG_HEADER = "createRestTemplate(): ";

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();

    // Add the multipart/form-data converter to the set of default converters.
    List<HttpMessageConverter<?>> messageConverters =
	restTemplate.getMessageConverters();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "messageConverters = " + messageConverters);

    for (HttpMessageConverter<?> hmc : messageConverters) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "hmc = " + hmc);
      if (hmc instanceof MappingJackson2HttpMessageConverter) {
	((MappingJackson2HttpMessageConverter)hmc).setSupportedMediaTypes(
	    Arrays.asList(MediaType.MULTIPART_FORM_DATA));
      }
    }

    // Specify the timeouts.
    SimpleClientHttpRequestFactory requestFactory =
	(SimpleClientHttpRequestFactory)restTemplate.getRequestFactory();

    requestFactory.setConnectTimeout(1000*connectTimeout);
    requestFactory.setReadTimeout(1000*readTimeout);

    return restTemplate;
  }

  /**
   * Parses the Multipart response.
   * 
   * @param response
   *          A ResponseEntity<String> with the response from the REST service.
   * @return a TextMultipartResponse with the parsed response.
   * @throws IOException
   *           if there are problems.
   */
  private TextMultipartResponse parseResponse(ResponseEntity<String> response)
      throws IOException {
    final String DEBUG_HEADER = "parseResponse(): ";

    // Initialize the parsed response.
    TextMultipartResponse multipartResponse = new TextMultipartResponse();

    // Populate the status code.
    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    multipartResponse.setStatusCode(statusCode);

    // Populate the response headers.
    HttpHeaders responseHeaders = response.getHeaders();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "responseHeaders = " + responseHeaders);

    multipartResponse.setResponseHeaders(responseHeaders);

    // Parse the response body.
    multipartResponse.parseResponseBody(response.getBody());

    return multipartResponse;
  }
}
