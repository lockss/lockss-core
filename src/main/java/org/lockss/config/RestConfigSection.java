/*

 Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.io.input.ReaderInputStream;
import org.lockss.rs.multipart.TextMultipartResponse;
import org.lockss.rs.multipart.TextMultipartResponse.Part;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

/**
 * A representation of a Configuration REST web service configuration section.
 */
public class RestConfigSection {
  private static Logger log = Logger.getLogger(RestConfigSection.class);

  private RestConfigClient serviceClient = null;
  private String sectionName = null;
  private InputStream inputStream = null;
  private String ifModifiedSince = null;
  private TextMultipartResponse response = null;
  private HttpStatus statusCode = null;
  private String errorMessage = null;
  private String lastModified = null;
  private String contentType = null;

  /**
   * Constructor.
   */
  public RestConfigSection() {
    serviceClient = ConfigManager.getConfigManager().getRestConfigClient();
  }

  /**
   * Constructor.
   */
  public RestConfigSection(RestConfigClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  public String getSectionName() {
    return sectionName;
  }

  public RestConfigSection setSectionName(String sectionName) {
    this.sectionName = sectionName;
    return this;
  }

  public InputStream getInputStream() {
    return inputStream;
  }

  public RestConfigSection setInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
    return this;
  }

  public String getIfModifiedSince() {
    return ifModifiedSince;
  }

  public RestConfigSection setIfModifiedSince(String ifModifiedSince) {
    this.ifModifiedSince = ifModifiedSince;
    return this;
  }

  public TextMultipartResponse getResponse() {
    return response;
  }

  public HttpStatus getStatusCode() {
    return statusCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getLastModified() {
    return lastModified;
  }

  public String getContentType() {
    return contentType;
  }

  /**
   * Loads from the REST Configuration Service the configuration of a section.
   */
  public void loadSection() {
    final String DEBUG_HEADER = "loadSection(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "serviceClient = " + serviceClient);

    if (serviceClient == null) {
      errorMessage = "Couldn't load config section '" + sectionName
	  + "': Service definition is null.";
      log.error(errorMessage);
      statusCode = HttpStatus.SERVICE_UNAVAILABLE;
      return;
    } else if (!serviceClient.isActive()) {
      errorMessage = "Couldn't load config section '" + sectionName
	  + "': Service is not active.";
      log.error(errorMessage);
      statusCode = HttpStatus.SERVICE_UNAVAILABLE;
      return;
    }

    String requestUrl = null;

    try {
      requestUrl = getRequestUrl(sectionName);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "requestUrl = " + requestUrl);

      response = serviceClient.callGetTextMultipartRequest(requestUrl,
	  ifModifiedSince);
    } catch (Exception e) {
      errorMessage = "Couldn't load config section '" + sectionName
	  + "' from URL '" + requestUrl + "': " + e.toString();
      log.error(errorMessage, e);
      statusCode = HttpStatus.SERVICE_UNAVAILABLE;
      return;
    }

    statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    switch (statusCode) {
    case OK:
      LinkedHashMap<String, Part> parts = response.getParts();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "parts = " + parts);

      Part configDataPart = parts.get("config-data");
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "configDataPart = " + configDataPart);

      String partLastModified = null;

      partLastModified = configDataPart.getLastModified();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "partLastModified = " + partLastModified);

      Map<String, String> partHeaders = configDataPart.getHeaders();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "partHeaders = " + partHeaders);

      if (StringUtil.isNullString(partLastModified)) {
	partLastModified = partHeaders.get(HttpHeaders.LAST_MODIFIED);
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "partLastModified = " + partLastModified);
      }

      lastModified = partLastModified;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModified = " + lastModified);

      contentType = partHeaders.get("Content-Type");
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "contentType = " + contentType);

      String partPayload = configDataPart.getPayload();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "partPayload = " + partPayload);

      StringReader payloadReader = new StringReader(partPayload);

      inputStream =
	  new ReaderInputStream(payloadReader, StandardCharsets.UTF_8);
      break;
    case NOT_MODIFIED:
      if (log.isDebug2())
	log.debug2("REST Service content not changed, not reloading.");
      errorMessage = statusCode.toString();
      break;
    default:
      errorMessage = statusCode.toString();
    }
  }

  public void putSection() {
  }

  /**
   * Provides the URL needed to read from, or write to, the REST Configuration
   * Service the configuration of a section.
   * 
   * @param serviceName
   *          A String with the name of the section.
   * @return a String with the URL.
   */
  private String getRequestUrl(String serviceName) {
    return serviceClient.getServiceLocation() + "/config/file/" + serviceName;
  }
}
