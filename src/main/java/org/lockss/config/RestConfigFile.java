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
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import org.apache.commons.io.input.ReaderInputStream;
import org.lockss.rs.multipart.TextMultipartResponse;
import org.lockss.rs.multipart.TextMultipartResponse.Part;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

/**
 * A ConfigFile loaded from a REST configuration service.
 */
public class RestConfigFile extends BaseConfigFile {

  private String lastModifiedString = null;

  /**
   * Constructor.
   *
   * @param url
   *          A String withe the URL of the file.
   * @param cfgMgr
   *          A ConfigManager with the configuration manager.
   */
  public RestConfigFile(String url, ConfigManager cfgMgr) {
    super(url, cfgMgr);
  }

  /** Return an InputStream open on the HTTP url.  If inaccessible and a
      local copy of the remote file exists, failover to it. */
  protected InputStream openInputStream() throws IOException {
    final String DEBUG_HEADER = "openInputStream(): ";
    if (m_cfgMgr == null) {
      throw new IOException("Null ConfigManager for RestConfigFile with URL '"
	  + m_fileUrl + "'");
    }

    RestConfigClient serviceClient = m_cfgMgr.getRestConfigClient();

    if (serviceClient == null) {
      throw new RuntimeException("Null RestConfigClient for RestConfigFile "
	  + "with URL '" + m_fileUrl + "'");
    }

    TextMultipartResponse response =
	serviceClient.callGetTextMultipartRequest(m_fileUrl);

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    LinkedHashMap<String, Part> parts = response.getParts();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "parts = " + parts);

    Part configDataPart = parts.get("config-data");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "configDataPart = " + configDataPart);

    HttpHeaders partHeaders = configDataPart.getHeaders();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "partHeaders = " + partHeaders);

    String partPayload = configDataPart.getPayload();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "partPayload = " + partPayload);

    StringReader payloadReader = new StringReader(partPayload);

    lastModifiedString = partHeaders.getFirst("last-modified");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "lastModifiedString = " + lastModifiedString);

    String contentType = partHeaders.getFirst("Content-Type");
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "contentType = " + contentType);

    if (MediaType.TEXT_XML_VALUE.equals(contentType)) {
      m_fileType = XML_FILE;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "m_fileType = " + m_fileType);
    }

    return new ReaderInputStream(payloadReader, StandardCharsets.UTF_8);
  }

  protected String calcNewLastModified() {
    return lastModifiedString;
  }
}
