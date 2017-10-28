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
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Vector;
import org.lockss.util.LineEndingBufferedReader;
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
   *          A ResponseEntity<String> with the multipart response.
   * @throws IOException
   *           if there are problems.
   */
  public TextMultipartResponse(ResponseEntity<String> response)
      throws IOException {
    final String DEBUG_HEADER = "TextMultipartResponse(response): ";

    // Populate the status code.
    statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    // Populate the response headers.
    responseHeaders = response.getHeaders();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "responseHeaders = " + responseHeaders);

    // Parse the response body.
    parseResponseBody(response.getBody());
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
   * Determines the response parts.
   * 
   * @param responseBody
   *          A String with the body of the response.
   * @throws IOException
   *           if there are problems.
   */
  public void parseResponseBody(String responseBody) throws IOException {
    final String DEBUG_HEADER = "parseResponseBody(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "responseBody = '" + responseBody + "'.");

    // Determine the boundary between parts.
    String boundary = getBoundaryFromContentTypeHeader(
	responseHeaders.getFirst("Content-Type"));
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "boundary = '" + boundary + "'.");

    Part part = null;
    HttpHeaders bodyHeaders = null;

    // Indication of whether the end-of-body boundary has been found.
    boolean endOfBodyBoundaryFound = false;

    // Indication of whether the headers of a part have all been read.
    boolean noMoreHeaders = false;

    String line = null;
    StringBuilder sb = null;

    // Create a reader for the response body.
    LineEndingBufferedReader lebr =
	new LineEndingBufferedReader(new StringReader(responseBody));

    try {
      // Loop through all the lines in the response body.
      while ((line = lebr.readLine()) != null) {
	if (log.isDebug3()) log.debug3(DEBUG_HEADER + "line = '" + line + "'.");

	// Check whether the line is a part boundary.
	if (line.trim().equals("--" + boundary)) {
	  // Yes: Check whether it is the boundary before the first part.
	  if (part == null) {
	    // Yes.
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "Found initial boundary.");
	  } else {
	    // No: Save the part just parsed.
	    if (log.isDebug3())
	      log.debug3(DEBUG_HEADER + "Found separation boundary.");

	    part.setPayload(sb.toString());
	    addPart(part);
	  }

	  // Initialize the incoming part.
	  part = new Part();
	  bodyHeaders = new HttpHeaders();
	  noMoreHeaders = false;
	  sb = new StringBuilder();

	  // Move to reading the next line.
	  continue;
	}

	// Check whether the line is the parts last boundary.
	if (line.trim().equals("--" + boundary + "--")) {
	  // Yes: Save the part just parsed.
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER + "End of body found.");

	  part.setPayload(sb.toString());
	  addPart(part);

	  // Done: Stop reading the response body.
	  endOfBodyBoundaryFound = true;
	  break;
	}

	// Check whether the boundary before the first part has not been read
	// yet.
	if (part == null) {
	  // Yes: Skip the line.
	  if (log.isDebug3()) log.debug3(DEBUG_HEADER
	      + "Skipped line before first part: '" + line + "'.");
	  continue;
	}

	// Check whether there are still more part headers to come.
	if (!noMoreHeaders) {
	  // Yes: Check whether it is a blank line.
	  if (line.trim().isEmpty()) {
	    // Yes: All the headers for the current part have been read.
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "End of headers.");
	    noMoreHeaders = true;
	    part.setHeaders(bodyHeaders);
	  } else {
	    // No: Parse and save this part header.
	    Vector<String> headerParts = StringUtil.breakAt(line.trim(), ": ");
	    if (log.isDebug3()) log.debug3(DEBUG_HEADER
		+ "headerParts.size() = " + headerParts.size());

	    if (headerParts.size() == 2) {
	      bodyHeaders.add(headerParts.get(0), headerParts.get(1));
	    }
	  }
	} else {
	  // No: Save the part payload line.
	  sb.append(line);
	}
      }
    } finally {
      lebr.close();
    }

    if (!endOfBodyBoundaryFound && part != null) {
      throw new IOException("Premature end of body");
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "Done.");
  }

  /**
   * Provides the part boundary from the Content-Type header.
   *
   * @param contentType
   *          A String with the value of the Content-Type header.
   * @return a String with the part boundary.
   */
  protected String getBoundaryFromContentTypeHeader(String contentType) {
    final String DEBUG_HEADER = "getBoundaryFromContentTypeHeader(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "contentType = '" + contentType + "'.");

    String boundaryPrefix = "boundary=";
    String boundary = null;

    // Loop through all the elements in the value of the Content-Type header.
    for (String ctElement : StringUtil.breakAt(contentType, ";")) {
      // Check whether it is the element defining the boundary.
      if (ctElement.trim().startsWith(boundaryPrefix)) {
	// Yes: Obtain the boundary.
	boundary = ctElement.trim().substring(boundaryPrefix.length()).trim();
	break;
      }
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "boundary = '" + boundary + "'.");
    return boundary;
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
    HttpHeaders headers;
    String payload;
    String name;

    /**
     * Provides the headers of the part.
     *
     * @return an HttpHeaders with the part headers.
     */
    public HttpHeaders getHeaders() {
      return headers;
    }

    /**
     * Populates the headers of the part.
     * 
     * @param headers An HttpHeaders with the part headers.
     */
    public void setHeaders(HttpHeaders headers) {
      this.headers = headers;
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
	String cdHeaderValue = headers.getFirst("Content-Disposition");
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "cdHeaderValue = '" + cdHeaderValue + "'");

	if (!StringUtil.isNullString(cdHeaderValue)) {
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
     *          A String with the value of the Content-contentDisposition
     *          header.
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

    @Override
    public String toString() {
      return "[Part headers=" + headers + ", name=" + name + ", payload="
	  + payload + "]";
    }
  }
}
