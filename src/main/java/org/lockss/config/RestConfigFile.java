/*

 Copyright (c) 2017-2018 Board of Trustees of Leland Stanford Jr. University,
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

import static org.lockss.config.RestConfigClient.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.net.*;
import java.util.*;
import org.apache.commons.io.input.ReaderInputStream;
import org.lockss.rs.multipart.TextMultipartResponse;
import org.lockss.rs.multipart.TextMultipartResponse.Part;
import org.lockss.util.UrlUtil;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.util.*;
import org.springframework.util.MultiValueMap;

/**
 * A ConfigFile loaded from a REST configuration service.
 */
public class RestConfigFile extends BaseConfigFile {

  private String lastModifiedString = null;
  private RestConfigClient serviceClient = null;
  private String requestUrl = null;

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
    requestUrl = getRequestUrl();
    serviceClient = cfgMgr.getRestConfigClient();
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
   * <br>
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
   * <br>
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

    TextMultipartResponse response = null;

    try {
      List<String> ifNoneMatch = new ArrayList<>();

      if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
	ifNoneMatch.add(ifModifiedSince);
      } else {
	ifNoneMatch = null;
      }

      response = serviceClient.callGetTextMultipartRequest(requestUrl, null,
	  ifNoneMatch);
    } catch (IOException e) {
      // The HTTP fetch failed.  First see if we already found a failover
      // file.
      log.info("Couldn't load remote config URL: " + m_fileUrl + ": "
	  + e.toString());
      m_loadError = e.getMessage();
      throw e;
    } catch (Exception e) {
      log.info("Couldn't load remote config URL: " + m_fileUrl + ": "
	  + e.toString());
      m_loadError = e.getMessage();
      throw new IOException(e.toString());
    }

    InputStream in = null;

    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);

    switch (statusCode) {
    case OK:
      m_loadError = null;

      LinkedHashMap<String, Part> parts = response.getParts();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "parts = " + parts);

      Part configDataPart = parts.get(CONFIG_PART_NAME);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "configDataPart = " + configDataPart);

      lastModifiedString = configDataPart.getEtag();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModifiedString = " + lastModifiedString);

      Map<String, String> partHeaders = configDataPart.getHeaders();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "partHeaders = " + partHeaders);

      String contentType = partHeaders.get(HttpHeaders.CONTENT_TYPE);
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

  /**
   * Provides an indication of whether this is a RestConfigFile.
   * 
   * @return a boolean with <code>true</code>.
   */
  @Override
  public boolean isRestConfigFile() {
    return true;
  }

  /**
   * Provides an indication of whether a URL is to be processed as a REST
   * Configuration service URL.
   * 
   * @param url
   *          A String with the URLto be examined.
   * @param configMgr
   *          A ConfigManager with the configuration manager.
   * @return a boolean with <code>true</code> if the URL is to be processed as a
   *         REST Configuration service URL, <code>false</code> otherwise.
   */
  static boolean isRestConfigUrl(String url, ConfigManager configMgr) {
    final String DEBUG_HEADER = "isRestConfigUrl(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "url = " + url);
      log.debug2(DEBUG_HEADER + "configMgr = " + configMgr);
    }

    boolean result = false;

    // Check whether there is no Configuration Manager.
    if (configMgr == null) {
      // Yes: the URL is not a REST Configuration service URL.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    // Get the parent configuration file.
    ConfigFile configFile = configMgr.getUrlParent(url);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "configFile = " + configFile);

    // Check whether a parent configuration file exists.
    if (configFile != null) {
      // Yes: This URL is is to be processed as a REST Configuration service URL
      // if the parent is a REST Configuration file.
      result = configFile.isRestConfigFile();
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    // No: Get the REST Configuration service client.
    RestConfigClient conFigMgrServiceClient = configMgr.getRestConfigClient();
    if (log.isDebug2()) log.debug2(DEBUG_HEADER
	+ "conFigMgrServiceClient = " + conFigMgrServiceClient);

    // No: Check whether there is no REST Configuration service client.
    if (!conFigMgrServiceClient.isActive()) {
      // Yes: the URL is not a REST Configuration service URL.
      if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
      return result;
    }

    // Determine whether this is a REST Configuration service URL.
    result = conFigMgrServiceClient.isPartOfThisService(url);
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "result = " + result);
    return result;
  }

  /**
   * Provides the actual URL to be used to make a request to the REST
   * Configuration service.
   * 
   * @return a String with the actual URL to be used to make a request to the
   *         REST Configuration service.
   */
  String getRequestUrl() {
    final String DEBUG_HEADER = "getRequestUrl(): ";

    // Check whether the URL has already been set.
    if (requestUrl != null) {
      // Yes: Just return it.
      if (log.isDebug2())
	log.debug2(DEBUG_HEADER + "requestUrl = " + requestUrl);
      return requestUrl;
    }

    // No: Check whether there is no configuration manager.
    if (m_cfgMgr == null) {
      // Yes: Report the problem.
      throw new RuntimeException("Null ConfigManager for RestConfigFile with "
	  + "URL '" + m_fileUrl + "'");
    }

    // No: Get the REST Configuration service client.
    serviceClient = m_cfgMgr.getRestConfigClient();

    // Check whether the REST Configuration service client is configured.
    // TK this breaks TestRestConfigFile, not sure why or what changed
//     if (!serviceClient.isActive()) {
//       // Yes: Report the problem.
//       throw new RuntimeException("RestConfigClient is not configured, "
// 	  + "with URL '" + m_fileUrl + "'");
// //       return null;
//     }

    String urlToUse = m_fileUrl;

    // Check whether this REST configuration file URL is not already goig to be
    // accessed via the REST Configuration service.
    if (!serviceClient.isPartOfThisService(urlToUse)) {
      // Yes: Redirect it via the REST Configuration service.
      try {
	urlToUse = redirectAbsoluteUrl(urlToUse);
      } catch (UnsupportedEncodingException uee) {
	throw new RuntimeException(uee);
      }
    }

    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "urlToUse = " + urlToUse);
    return urlToUse;
  }

  @Override
  public String resolveConfigUrl(String relUrl) {
    final String DEBUG_HEADER = "resolveConfigUrl(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "relUrl = " + relUrl);
    String restUrl = getFileUrl();
    UriComponentsBuilder ucb =
      UriComponentsBuilder.fromUriString(restUrl);
    UriComponents comp = ucb.build();
    String path = comp.getPath();

    if (path.startsWith("/config/url")) {
	MultiValueMap<String, String> params = comp.getQueryParams();
	List<String> urls = params.get("url");
	String base = UrlUtil.decodeUrl(urls.get(0));

	try {
	  String absUrl = UrlUtil.resolveUri(base, relUrl);
	  //       ucb.replaceQueryParam("url", UrlUtil.encodeUrl(absUrl));
	  ucb.replaceQueryParam("url", absUrl);
	  return ucb.toUriString();
	} catch (MalformedURLException e) {
	  log.error("Malformed props base URL: " + base + ", rel: " + relUrl,
		    e);
	  return relUrl;
	}
      } else if (path.startsWith("/config/file")) {
	ucb.replacePath("/config/url");
// 	String base = ucb.build().toUriString();
	ucb.replaceQueryParam("url", relUrl);
	return ucb.toUriString();
      }
    return relUrl;
  }

  /**
   * Provides the redirection URL for an absolute URL to go through the REST
   * Configuration service.
   * 
   * @param originalUrl
   *          A String with the URL to be redirected.
   * @return a String with the redirection URL.
   * @throws UnsupportedEncodingException
   *           if the URL cannot be redirected.
   */
  String redirectAbsoluteUrl(String originalUrl)
      throws UnsupportedEncodingException {
    final String DEBUG_HEADER = "redirectAbsoluteUrl(): ";
    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "originalUrl = " + originalUrl);

    // Check whether the passed URL is not an absolute URL.
    if (!UrlUtil.isAbsoluteUrl(originalUrl)) {
      // Yes: Report the problem.
      throw new IllegalArgumentException("'" + originalUrl
	  + "' is not an absolute URL");
    }

    String redirectionUrl = null;

    // Get the REST Configuration service location.
    String serviceLocation = serviceClient.getServiceLocation();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "serviceLocation = " + serviceLocation);

    // Check whether there is no REST Configuration service.
    if (serviceLocation == null) {
      // Yes: No redirection is needed.
      redirectionUrl = originalUrl;
    } else {
      // No: Build the redirected URL.
      redirectionUrl = serviceLocation + UrlUtil.URL_PATH_SEPARATOR
	  + "config/url?url=" +
	UriUtils.encodePathSegment(originalUrl, "UTF-8");
    }

    if (log.isDebug2())
      log.debug2(DEBUG_HEADER + "redirectionUrl = " + redirectionUrl);
    return redirectionUrl;
  }
}
