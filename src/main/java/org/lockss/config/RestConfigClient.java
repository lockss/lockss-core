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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Base64;
import java.util.Vector;
import org.lockss.rs.multipart.TextMultipartConnector;
import org.lockss.rs.multipart.TextMultipartResponse;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.lockss.util.UrlUtil;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * A client representation of the Configuration REST web service.
 */
public class RestConfigClient {
  private static Logger log = Logger.getLogger(RestConfigClient.class);

  // The REST configuration service URL.
  private String restConfigServiceUrl;

  // The properties needed to access the Configuration REST web service.
  private String serviceLocation = null;
  private String serviceUser = null;
  private String servicePassword = null;
  private Integer serviceTimeout = new Integer(60);

  /**
   * Constructor.
   * 
   * @param restConfigServiceUrl
   *          A String with the information necessary to access the
   *          Configuration REST web service.
   */
  public RestConfigClient(String restConfigServiceUrl) {
    this.restConfigServiceUrl = restConfigServiceUrl;

    // Save the individual components of the Configuration REST web service URL.
    parseRestConfigServiceUrl();
  }

  /**
   * Saves the individual components of the Configuration REST web service URL.
   */
  private void parseRestConfigServiceUrl() {
    final String DEBUG_HEADER = "parseRestConfigServiceUrl(): ";

    // Ignore missing information about the Configuration REST web service.
    if (StringUtil.isNullString(restConfigServiceUrl)) {
      return;
    }

    try {
      URL url = new URL(restConfigServiceUrl);

      // Get the passed credentials.
      String credentialsAsString = url.getUserInfo();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "credentialsAsString = " + credentialsAsString);

      // Check whether credentials were passed.
      if (StringUtil.isNullString(credentialsAsString)) {
	// No.
	serviceLocation = restConfigServiceUrl;
      } else {
	// Yes: Parse them.
	Vector<String> credentials =
	    StringUtil.breakAt(credentialsAsString, ":");

	if (credentials != null && credentials.size() > 0) {
	  serviceUser = credentials.get(0);
	  if (log.isDebug3())
	    log.debug3(DEBUG_HEADER + "serviceUser = " + serviceUser);

	  if (credentials.size() > 1) {
	    servicePassword = credentials.get(1);
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "servicePassword = " + servicePassword);
	  }
	}

	// Get the service location.
	serviceLocation = new URL(url.getProtocol(), url.getHost(),
	    url.getPort(), url.getFile()).toString();
      }
    } catch (MalformedURLException mue) {
      log.error("Error parsing REST Configuration Service URL: "
	  + mue.toString());

      serviceLocation = null;
      serviceUser = null;
      servicePassword = null;
    }

    log.info("REST Configuration service location = " + serviceLocation);
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
      // Check whether the URL is neither HTTP nor HTTPS or points to a
      // different host than this service.
      if (!UrlUtil.isHttpOrHttpsUrl(urlText)
	  || !UrlUtil.isSameHost(urlText, restConfigServiceUrl)) {
	// Yes.
	return false;
      }

      // No: Parse the location URLs to help comparing them.
      URL url = new URL(urlText);
      URL serviceLocation = new URL(restConfigServiceUrl);

      int urlPort = url.getPort();
      int servicePort = serviceLocation.getPort();

      // Check whether the ports are identical.
      if (urlPort == servicePort) {
	// Yes:
	return true;
      }

      // No: Handle default ports.
      return url.getProtocol() == serviceLocation.getProtocol()
	  && url.getPort() == serviceLocation.getPort();
    } catch (MalformedURLException e) {
      log.warning("isSameService", e);
      return false;
    }
  }

  /**
   * Calls the service Text Multipart GET method with the given URL and provides
   * the response.
   * 
   * @param url
   *          String with the URL.
   * @return a TextMultipartResponse with the response.
   * @throws IOException
   *           if there are problems.
   */
  public TextMultipartResponse callGetTextMultipartRequest(String url)
      throws Exception {
    final String DEBUG_HEADER = "callGetTextMultipartRequest(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "url = " + url);

    // Create the URI of the request to the REST service.
    UriComponents uriComponents =
	UriComponentsBuilder.fromUriString(url).build();

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.setAccept(Arrays.asList(MediaType.MULTIPART_FORM_DATA,
	MediaType.APPLICATION_JSON));

    // Set the authentication credentials.
    String credentials = serviceUser + ":" + servicePassword;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(Charset.forName("US-ASCII")));
    requestHeaders.set("Authorization", authHeaderValue);

    // Make the request and obtain the response.
    TextMultipartResponse response = new TextMultipartConnector(uri,
	requestHeaders).request(serviceTimeout, serviceTimeout);

    return response;
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
