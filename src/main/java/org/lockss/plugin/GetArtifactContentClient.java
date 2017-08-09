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
package org.lockss.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Date;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import org.lockss.config.CurrentConfig;
import org.lockss.util.Logger;
import org.lockss.ws.entities.ContentResult;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 * A client for the Repository REST Service
 * GET /repos/{repository}/artifacts/{artifactid} operation.
 */
public class GetArtifactContentClient {
  private static Logger log = Logger.getLogger(GetArtifactContentClient.class);

  public ContentResult getArtifactContent(String artifactId, String url)
      throws Exception {
    final String DEBUG_HEADER = "getArtifactContent(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "artifactId = " + artifactId);

    // Get the configured REST service location.
    String restServiceLocation = CurrentConfig.getParam(
	PluginManager.PARAM_URL_CONTENT_REST_SERVICE_LOCATION);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "restServiceLocation = "
	+ restServiceLocation);

    // Get the indication of whether the URL is cached from the REST service.
    int timeoutValue = CurrentConfig.getIntParam(
	PluginManager.PARAM_URL_CONTENT_WS_TIMEOUT_VALUE,
	PluginManager.DEFAULT_URL_CONTENT_WS_TIMEOUT_VALUE);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "timeoutValue = " + timeoutValue);

    // Get the authentication credentials.
    String userName =
	CurrentConfig.getParam(PluginManager.PARAM_URL_CONTENT_WS_USER_NAME);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "userName = '" + userName + "'");
    String password =
	CurrentConfig.getParam(PluginManager.PARAM_URL_CONTENT_WS_PASSWORD);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "password = '" + password + "'");

    // Build the REST service URL.
    String restServiceUrl =
	restServiceLocation.replace("{artifactid}", artifactId);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "Making request to '" + restServiceUrl + "'");

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();
    SimpleClientHttpRequestFactory requestFactory =
	(SimpleClientHttpRequestFactory)restTemplate.getRequestFactory();

    requestFactory.setReadTimeout(1000*timeoutValue);
    requestFactory.setConnectTimeout(1000*timeoutValue);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.MULTIPART_FORM_DATA);

    String credentials = userName + ":" + password;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(Charset.forName("US-ASCII")));
    headers.set("Authorization", authHeaderValue);

    // Make the request to the REST service and get its response.
    ResponseEntity<MultiValueMap> response =
	restTemplate.exchange(restServiceUrl, HttpMethod.GET,
	    new HttpEntity<String>(null, headers), MultiValueMap.class);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    MultiValueMap<String, Object> result = response.getBody();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "result = " + result);

    HttpEntity<ByteArrayResource> contentPart =
	(HttpEntity<ByteArrayResource>)result.getFirst("content");

    HttpHeaders partHeaders = contentPart.getHeaders();

    String contentType = partHeaders.getContentType().toString();
    long contentLength = partHeaders.getContentLength();

    InputStream inputStream = contentPart.getBody().getInputStream();

    /*
    LineEndingBufferedReader lebr =
	new LineEndingBufferedReader(new StringReader(result));

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
//	new ByteArrayInputStream(result.getBytes(StandardCharsets.UTF_8));

//    int newLineCount = 0;
//    StringBuilder sb = new StringBuilder();
//
//    while (newLineCount < 5) {
//      int code = inputStream.read();
//
//      if (code == 13) {
//	newLineCount++;
//	if (log.isDebug3())
//	  log.debug3(DEBUG_HEADER + "Found " + newLineCount + " newline.");
//	
//	String line = sb.toString();
//	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "line = " + line);
//
//	if (newLineCount == 3) {
//	  contentType = line.substring("Content-Type: ".length());
//	  if (log.isDebug3())
//	    log.debug3(DEBUG_HEADER + "contentType = " + contentType);
//	} else if (newLineCount == 4) {
//	  contentLength = line.substring("Content-Length: ".length());
//	  if (log.isDebug3())
//	    log.debug3(DEBUG_HEADER + "contentLength = " + contentLength);
//	}
//
//	sb = new StringBuilder();
//      } else {
//	sb.append((char)code);
//      }
//    }*/

    // Populate the response.
    ContentResult cr = new ContentResult();

    // TODO: Fill the right properties, which are the equivalent of
    // CachedUrl.getProperties().
    Properties properties = new Properties();
    properties.setProperty("x-lockss-node-url", url);

    Date now = new Date();
    
    properties.setProperty("x_lockss-server-date",
	String.valueOf(now.getTime()));
    properties.setProperty("date", now.toString());
    properties.setProperty("pragma", "no-cache");
    properties.setProperty("cache-control", "no-cache");
    properties.setProperty("org.lockss.version.number", "1");
    properties.setProperty("content-type", contentType);
    properties.setProperty("x-lockss-content-type", contentType);
    properties.setProperty("content-length", String.valueOf(contentLength));

    cr.setProperties(properties);
    cr.setDataHandler(new DataHandler(new InputStreamDataSource(inputStream)));

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "cr = " + cr);
    return cr;
  }

  public class InputStreamDataSource implements DataSource {

    private InputStream is;

    public InputStreamDataSource(InputStream is) {
      this.is = is;
    }

    @Override
    public String getContentType() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      // TODO Auto-generated method stub
      return is;
    }

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      // TODO Auto-generated method stub
      return null;
    }
  }
}
