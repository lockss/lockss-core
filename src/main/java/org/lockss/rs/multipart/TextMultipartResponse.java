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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.mail.Header;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * A representation of an HTTP Multipart response where the payload is text.
 */
public class TextMultipartResponse {
  private static Logger log = Logger.getLogger(TextMultipartResponse.class);

  private HttpStatus statusCode;
  private HttpHeaders responseHeaders;
  private LinkedHashMap<String, Part> parts = new LinkedHashMap<String, Part>();

  /**
   * Default constructor.
   */
  public TextMultipartResponse() {
  }

  /**
   * Constructor using a multipart response.
   * 
   * @param response
   *          A ResponseEntity<MimeMultipart> with the multipart response.
   * @throws Exception
   *           if there are problems.
   */
  public TextMultipartResponse(ResponseEntity<MimeMultipart> response)
      throws Exception {
    final String DEBUG_HEADER = "TextMultipartResponse(response): ";

    // Populate the status code.
    statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    // Populate the response headers.
    responseHeaders = response.getHeaders();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "responseHeaders = " + responseHeaders);

    // The multipart response body.
    MimeMultipart responseBody = response.getBody();

    // Get the count of parts in the response body.
    int partCount = 0;

    try {
      partCount = responseBody.getCount();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "partCount = " + partCount);
    } catch (MessagingException me) {
      log.warning("Cannot get multipart response part count", me);
    }

    if (log.isDebug3()) {
      log.debug3(DEBUG_HEADER + "partCount = " + partCount);
      log.debug3(DEBUG_HEADER + "responseBody.getContentType() = "
	+ responseBody.getContentType());
    }

    // Loop through all the parts.
    for (int i = 0; i < partCount; i++) {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "part index = " + i);

      // Get the part body.
      MimeBodyPart partBody = (MimeBodyPart)responseBody.getBodyPart(i);

      // Process the part headers.
      Map<String, String> partHeaders = new HashMap<String, String>();
      
      for (Enumeration<?> enumeration = partBody.getAllHeaders();
  	enumeration.hasMoreElements();) {
  	Header header=(Header)enumeration.nextElement();
  	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "header = " + header);

  	String headerName = header.getName();
  	String headerValue = header.getValue();
  	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "headerName = "
  	    + headerName + ", headerValue = " + headerValue);

  	partHeaders.put(headerName, headerValue);
      }

      // Create and save the part.
      Part part = new Part();
      part.setHeaders(partHeaders);
      part.setPayload((String)partBody.getContent());
      addPart(part);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "payload = '" + part.getPayload() + "'");
    }
  }

  /**
   * Provides the status code of the response.
   *
   * @return an HttpStatus with the response status code.
   */
  public HttpStatus getStatusCode() {
    return statusCode;
  }

  /**
   * Populates the status code of the response.
   * 
   * @param statusCode An HttpStatus with the response status code.
   */
  public void setStatusCode(HttpStatus statusCode) {
    this.statusCode = statusCode;
  }

  /**
   * Provides the headers of the response.
   *
   * @return an HttpHeaders with the response headers.
   */
  public HttpHeaders getResponseHeaders() {
    return responseHeaders;
  }

  /**
   * Populates the headers of the response.
   * 
   * @param responseHeaders An HttpHeaders with the response headers.
   */
  public void setResponseHeaders(HttpHeaders responseHeaders) {
    this.responseHeaders = responseHeaders;
  }

  /**
   * Provides the parts of the response, indexed by the part name.
   * 
   * @return a LinkedHashMap<String, Part> with the response parts, indexed by
   *         the part name.
   */
  public LinkedHashMap<String, Part> getParts() {
    return parts;
  }

  /**
   * Saves a part.
   * 
   * @param part
   *          A Part with the part to be saved.
   */
  protected void addPart(Part part) {
    final String DEBUG_HEADER = "addPart(): ";
    String partName = part.getName();

    if (partName == null) {
      int index = parts.size();
      partName = "Part-" + index;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "Trying partName = '" + partName + "'");

      while (parts.containsKey(partName)) {
	index++;
	partName = "Part-" + index;
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "Trying partName = '" + partName + "'");
      }

      part.setName(partName);
    }

    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "partName = '" + partName + "'");

    parts.put(partName, part);
  }

  @Override
  public String toString() {
    return "[MultipartResponse statusCode=" + statusCode + ", responseHeaders="
	+ responseHeaders + ", parts=" + parts + "]";
  }

  /**
   * A representation of an HTTP Multipart response part where the payload is
   * text.
   */
  public static class Part {
    private static Logger log = Logger.getLogger(Part.class);

    Map<String, String> headers;
    String payload;
    String name;

    /**
     * Provides the headers of the part.
     *
     * @return a Map<String, String> with the part headers.
     */
    public Map<String, String> getHeaders() {
      return headers;
    }

    /**
     * Populates the headers of the part.
     * 
     * @param headers A Map<String, String> with the part headers.
     */
    public void setHeaders(Map<String, String> headers) {
      this.headers = headers;
    }

    /**
     * Provides the value of the Content-Length header.
     * 
     * @return a long with the value of the Content-Length header, or -1 if
     *         there is no Content-Length header.
     * @throws NumberFormatException
     *           if the Content-Length header value cannot be parsed as a
     *           number.
     */
    public long getContentLength() throws NumberFormatException {
      final String DEBUG_HEADER = "getContentLength(): ";
      String contentLengthValue = headers.get(HttpHeaders.CONTENT_LENGTH);

      if (contentLengthValue == null) {
	return -1;
      }

      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "contentLengthValue = " + contentLengthValue);

      long contentLength = Long.parseLong(contentLengthValue);
      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "contentLength = " + contentLength);
      return contentLength;
    }

    /**
     * Provides the value of the ETag header.
     * 
     * @return a String with the value of the ETag header.
     */
    public String getLastModified() {
      final String DEBUG_HEADER = "getLastModified(): ";
      String lastModifiedValue = headers.get(HttpHeaders.ETAG);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModifiedValue = " + lastModifiedValue);

      lastModifiedValue = parseEtag(lastModifiedValue);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModifiedValue = " + lastModifiedValue);

      return lastModifiedValue;
    }

    /**
     * Provides the payload of the part.
     *
     * @return a String with the part payload.
     */
    public String getPayload() {
      return payload;
    }

    /**
     * Populates the payload of the part.
     * 
     * @param payload A String with the part payload.
     */
    public void setPayload(String payload) {
      this.payload = payload;
    }

    /**
     * Provides the name of the part.
     *
     * @return a String with the part name.
     */
    public String getName() {
      final String DEBUG_HEADER = "getName(): ";

      if (name == null) {
	// Get the value of the Content-Disposition header.
	String cdHeaderValue = headers.get("Content-Disposition");
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "cdHeaderValue = '" + cdHeaderValue + "'");

	if (!StringUtil.isNullString(cdHeaderValue)) {
	  // Extract the part name from the Content-Disposition header.
	  name = getPartNameFromContentDispositionHeader(cdHeaderValue);
	}
      }

      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "name = '" + name + "'");
      return name;
    }

    /**
     * Populates the name of the part.
     * 
     * @param name A String with the part name.
     */
    private void setName(String name) {
      this.name = name;
    }

    /**
     * Provides the part name from the Content-Disposition header.
     *
     * @param contentDisposition
     *          A String with the value of the Content-Disposition header.
     * @return a String with the part name.
     */
    protected String getPartNameFromContentDispositionHeader(
        String contentDisposition) {
      final String DEBUG_HEADER = "getPartNameFromContentDispositionHeader(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER
	  + "contentDisposition = '" + contentDisposition + "'.");

      String partNamePrefix = "name=\"";
      String partName = null;

      // Loop through all the elements in the value of the Content-Disposition
      // header.
      for (String cdElement : StringUtil.breakAt(contentDisposition, ";")) {
	// Check whether it is the element defining the part name.
        if (cdElement.trim().startsWith(partNamePrefix)) {
          // Yes: Obtain the part name.
          partName = StringUtil.upToFinal(
              cdElement.trim().substring(partNamePrefix.length()), "\"").trim();
          break;
        }
      }

      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "partName = '" + partName + "'.");
      return partName;
    }

    /**
     * Parses an incoming ETag.
     * 
     * @param eTag
     *          A String with the incoming ETag.
     * @return a String with the parsed ETag.
     */
    private String parseEtag(String eTag) {
      final String DEBUG_HEADER = "parseEtag(): ";
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "eTag = " + eTag);

      // Check whether the raw eTag has content and it is surrounded by double
      // quotes.
      if (eTag != null && eTag.startsWith("\"") && eTag.endsWith("\"")) {
        // Yes: Remove the surrounding double quotes left by Spring.
        eTag = eTag.substring(1, eTag.length()-1);
        if (log.isDebug3()) log.debug3(DEBUG_HEADER + "eTag = " + eTag);
      }

      return eTag;
    }

    @Override
    public String toString() {
      return "[Part headers=" + headers + ", name=" + name + ", payload="
	  + payload + "]";
    }
  }
}
