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
import java.util.Map;
import org.apache.commons.io.input.ReaderInputStream;
import org.lockss.rs.multipart.TextMultipartResponse;
import org.lockss.rs.multipart.TextMultipartResponse.Part;
import org.lockss.util.StringUtil;
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

  /**
   * Provides an input stream to the content of this file.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  protected InputStream getInputStreamIfModified() throws IOException {
    final String DEBUG_HEADER = "getInputStreamIfModified(" + m_fileUrl + "): ";

    String ifModifiedSince = null;

    if (m_config != null && m_lastModified != null) {
      if (log.isDebug2()) log.debug2(DEBUG_HEADER
	  + "Setting request if-modified-since to: " + m_lastModified);
      ifModifiedSince = m_lastModified;
    }

    return getInputStreamIfModifiedSince(ifModifiedSince);
  }

  /**
   * Provides an input stream to the content of this file, ignoring previous
   * history.
   * <br />
   * Use this to stream the file contents.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return getInputStreamIfModifiedSince(null);
  }

  /**
   * Provides an input stream to the content of this file, ignoring previous
   * history.
   * <br />
   * Use this to stream the file contents.
   * 
   * @return an InputStream with the input stream to the file contents.
   * @throws IOException
   *           if there are problems.
   */
  public InputStream getInputStreamIfModifiedSince(String ifModifiedSince)
      throws IOException {
    final String DEBUG_HEADER =
	"getInputStreamIfModifiedSince(" + m_fileUrl + "): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "ifModifiedSince = " + ifModifiedSince);

    if (m_cfgMgr == null) {
      throw new RuntimeException("Null ConfigManager for RestConfigFile with "
	  + "URL '" + m_fileUrl + "'");
    }

    RestConfigClient serviceClient = m_cfgMgr.getRestConfigClient();

    if (serviceClient == null) {
      throw new RuntimeException("Null RestConfigClient for RestConfigFile "
	  + "with URL '" + m_fileUrl + "'");
    }

    TextMultipartResponse response = null;

    try {
      response =
	  serviceClient.callGetTextMultipartRequest(m_fileUrl, ifModifiedSince);
    } catch (Exception e) {
      m_loadError = e.getMessage();
      throw new RuntimeException(m_loadError);
    }

    InputStream in = null;

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    switch (statusCode) {
    case OK:
      m_loadError = null;

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

      lastModifiedString = partLastModified;
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModifiedString = " + lastModifiedString);

      String contentType = partHeaders.get("Content-Type");
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "contentType = " + contentType);

      if (MediaType.TEXT_XML_VALUE.equals(contentType)) {
	m_fileType = XML_FILE;
	if (log.isDebug3())
	  log.debug3(DEBUG_HEADER + "m_fileType = " + m_fileType);
      }

      String partPayload = configDataPart.getPayload();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "partPayload = " + partPayload);

      StringReader payloadReader = new StringReader(partPayload);

      in = new ReaderInputStream(payloadReader, StandardCharsets.UTF_8);
      break;
    case NOT_MODIFIED:
      m_loadError = null;
      log.debug2("Rest Service content not changed, not reloading.");
      break;
    default:
      m_loadError = statusCode.toString();
      throw new IOException(m_loadError);
    }

    return in;
  }

  /**
   * Provides the last modification timestamp as a text string.
   * 
   * @return a String with the last modification timestamp.
   */
  protected String calcNewLastModified() {
    return lastModifiedString;
  }
}
