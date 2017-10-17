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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Vector;
import org.lockss.util.LineEndingBufferedReader;
import org.lockss.util.Logger;
import org.lockss.util.ReaderInputStream;
import org.lockss.util.StringUtil;
import org.lockss.util.UrlUtil;
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
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

public class RestConfigService {
  private static Logger log = Logger.getLogger(RestConfigService.class);

  // The REST configuration service URL.
  private String restConfigServiceUrl;

  // The properties needed to access the Configuration REST web service.
  private String serviceLocation = null;
  private String serviceUser = null;
  private String servicePassword = null;
  private Integer serviceTimeout = new Integer(60);

  public RestConfigService(String restConfigServiceUrl) {
    this.restConfigServiceUrl = restConfigServiceUrl;
    parseRestConfigServiceUrl();
  }

  private void parseRestConfigServiceUrl() {
    final String DEBUG_HEADER = "parseRestConfigServiceUrl(): ";
    if (StringUtil.isNullString(restConfigServiceUrl)) {
      return;
    }

    try {
      URL url = new URL(restConfigServiceUrl);
      System.out.println("parseRestConfigServiceUrl(): url.getProtocol() = "
	  + url.getProtocol());
      System.out.println("parseRestConfigServiceUrl(): url.getAuthority() = "
	  + url.getAuthority());
      System.out.println("parseRestConfigServiceUrl(): url.getFile() = "
	  + url.getFile());
      System.out.println("parseRestConfigServiceUrl(): url.getHost() = "
	  + url.getHost());
      System.out.println("parseRestConfigServiceUrl(): url.getPath() = "
	  + url.getPath());
      System.out.println("parseRestConfigServiceUrl(): url.getPort() = "
	  + url.getPort());
      System.out.println("parseRestConfigServiceUrl(): url.getUserInfo() = "
	  + url.getUserInfo());

      String credentialsAsString = url.getUserInfo();
//	  StringUtil.getTextBetween(restConfigServiceUrl, "://", "@");
      System.out.println("parseRestConfigServiceUrl(): credentialsAsString = "
	  + credentialsAsString);

      if (StringUtil.isNullString(credentialsAsString)) {
	serviceLocation = restConfigServiceUrl;
      } else {
	Vector<String> credentials =
	    StringUtil.breakAt(credentialsAsString, ":");

	if (credentials != null && credentials.size() > 0) {
	  serviceUser = credentials.get(0);
	  System.out.println("parseRestConfigServiceUrl(): serviceUser = "
	      + serviceUser);

	  if (credentials.size() > 1) {
	    servicePassword = credentials.get(1);
	    System.out.println("parseRestConfigServiceUrl(): servicePassword = "
		+ servicePassword);
	  }
	}

	serviceLocation = new URL(url.getProtocol(), url.getHost(),
	    url.getPort(), url.getFile()).toString();
      }
    } catch (MalformedURLException mue) {
      
    }

    log.info(DEBUG_HEADER + "serviceLocation = " + serviceLocation);
  }

  /**
   * Provides the REST configuration service location.
   *
   * @return a String with the REST configuration service location.
   */
  public String getServiceLocation() {
    return serviceLocation;
  }

  /**
   * Provides an indication of whether a URL corresponds to this service.
   * 
   * @param urlText
   *          A String with the URL to be checked.
   * @return a boolean with <code>true</code> if the URL corresponds to this
   *         service, <code>false</code>, otherwise
   * 
   */
  public boolean isPartOfThisService(String urlText) {
    try {
      if (!UrlUtil.isHttpOrHttpsUrl(urlText)
	  || !UrlUtil.isSameHost(urlText, restConfigServiceUrl)) {
	return false;
      }

      URL url = new URL(urlText);
      URL serviceLocation = new URL(restConfigServiceUrl);

      int urlPort = url.getPort();
      int servicePort = serviceLocation.getPort();

      if (urlPort == servicePort) {
	return true;
      }

      return url.getProtocol() == serviceLocation.getProtocol()
	  && url.getPort() == serviceLocation.getPort();
    } catch (MalformedURLException e) {
      log.warning("isSameService", e);
      return false;
    }
  }

  public String getResponseBody(String url) {
    final String DEBUG_HEADER = "getResponseBody(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);

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

    requestFactory.setReadTimeout(1000*serviceTimeout);
    requestFactory.setConnectTimeout(1000*serviceTimeout);

    // Initialize the request headers.
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.MULTIPART_FORM_DATA);
    headers.setAccept(Arrays.asList(MediaType.MULTIPART_FORM_DATA));

    // Set the authentication credentials.
    String credentials = serviceUser + ":" + servicePassword;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(Charset.forName("US-ASCII")));
    headers.set("Authorization", authHeaderValue);

    // Create the URI of the request to the REST service.
    UriComponents uriComponents =
	UriComponentsBuilder.fromUriString(url).build();

    URI uri = UriComponentsBuilder.newInstance()
	.uriComponents(uriComponents).build().encode().toUri();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Making request to '" + uri + "'...");

    // Make the request to the REST service and get its response.
    ResponseEntity<String> response = restTemplate.exchange(uri,
	HttpMethod.GET, new HttpEntity<String>(null, headers), String.class);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    String result = response.getBody();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "result = " + result);

    return result;
  }

  public InputStream getInputStreamFromResponseBody(String responseBody)
  throws IOException {
    final String DEBUG_HEADER = "getInputStreamFromResponseBody(): ";

    LineEndingBufferedReader lebr =
	new LineEndingBufferedReader(new StringReader(responseBody));

    String contentType = null;
    String contentLength = null;

    int ctCount = 0;
    int emptyCount = 0;
    String line = null;

    while ((line = lebr.readLine()) != null) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "line = " + line);

      if (line.startsWith("Content-Type: ")) {
	if (ctCount > 0) {
	  contentType = line.substring("Content-Type: ".length());
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "contentType = " + contentType);
	} else {
	  ctCount++;
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "ctCount = " + ctCount);
	}
      } else if ("\r\n".equals(line)) {
	if (emptyCount > 0) {
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "Done with the header lines.");
	  break;
	} else {
	  emptyCount++;
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "emptyCount = " + emptyCount);
	}
      } else if (line.startsWith("Content-Length: ")) {
	contentLength = line.substring("Content-Length: ".length());
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);
      }
    }

    InputStream inputStream = new ReaderInputStream(lebr);
    return inputStream;
  }

  public TdbAu getTdbAu(String auId) throws Exception {
    return new GetTdbAuClient(serviceLocation, serviceUser,
	    servicePassword, serviceTimeout).getTdbAu(auId);
  }

  public Configuration getAuConfig(String auId) throws Exception {
    return new GetAuConfigClient(serviceLocation, serviceUser,
	    servicePassword, serviceTimeout).getAuConfig(auId);
  }
}
