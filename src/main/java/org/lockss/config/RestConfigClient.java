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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import javax.mail.MessagingException;
import org.lockss.laaws.rs.util.NamedInputStreamResource;
import org.lockss.rs.multipart.MultipartConnector;
import org.lockss.rs.multipart.MultipartResponse;
import org.lockss.rs.multipart.MultipartResponse.Part;
import org.lockss.util.Logger;
import org.lockss.util.StringUtil;
import org.lockss.util.UrlUtil;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * A client representation of the Configuration REST web service.
 */
public class RestConfigClient {
  // The part with the configuration file must be named this way because the
  // use of Swagger 2 requires it.
  // In the Swagger YAML configuration file, a multipart/form-data body payload
  // is represented by the 'in: formData' parameter, which needs to have a type
  // of 'file' and Swagger 2 uses the type as the part name.
  public static String CONFIG_PART_NAME = "file";

  private static Logger log = Logger.getLogger();

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
   * Provides an indication of whether the Configuration REST web service is
   * available.
   * 
   * @return a boolean with <code>true</code> if the Configuration REST web
   *         service is available, <code>false</code>, otherwise
   */
  public boolean isActive() {
    return serviceLocation != null;
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
   * Provides an indication of whether a URL corresponds to the Configuration
   * REST web service.
   * 
   * @param urlText
   *          A String with the URL to be checked.
   * @return a boolean with <code>true</code> if the URL corresponds to the
   *         Configuration REST web service, <code>false</code>, otherwise
   * 
   */
  public boolean isPartOfThisService(String urlText) {
    final String DEBUG_HEADER = "isPartOfThisService(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "urlText = " + urlText);

    try {
      if (log.isDebug3()) log.debug3(DEBUG_HEADER
	  + "restConfigServiceUrl = " + restConfigServiceUrl);

      if (restConfigServiceUrl == null) {
	return false;
      }

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
   * Provides the configuration of a section obtained via the REST web service.
   * 
   * @param restConfigSection
   *          A RestConfigSection with the request parameters.
   * @return a RestConfigSection with the result.
   */
  public RestConfigSection getConfigSection(RestConfigSection input) {
    final String DEBUG_HEADER = "getConfigSection(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "input = " + input);

    // Validation of the input object.
    if (input == null) {
      String errorMessage = "RestConfigSection is null";
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Validation of the section name.
    String sectionName = input.getSectionName();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "sectionName = " + sectionName);

    if (StringUtil.isNullString(sectionName)) {
      String errorMessage = "Invalid section name '" + sectionName + "'";
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Handle an unavailable service.
    if (!isActive()) {
      String errorMessage = "Couldn't load config section '" + sectionName
	  + "': Service is not active.";
      log.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }

    RestConfigSection output = new RestConfigSection();
    output.setSectionName(sectionName);
    String requestUrl = null;
    MultipartResponse response = null;

    try {
      // Get the request URL.
      requestUrl = getSectionNameRequestUrl(sectionName);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "requestUrl = " + requestUrl);

      // Make the request and get the response.
      response = callGetMultipartRequest(requestUrl,
	  input.getHttpRequestPreconditions());
      output.setResponse(response);
    } catch (HttpClientErrorException hcee) {
      String errorMessage = "Couldn't load config section '" + sectionName
	  + "' from URL '" + requestUrl + "': " + hcee.toString();
      log.error(errorMessage, hcee);
      log.error("hcee.getStatusCode() = " + hcee.getStatusCode());
      output.setErrorMessage(errorMessage);
      output.setStatusCode(hcee.getStatusCode());
      return output;
    } catch (IOException | MessagingException e) {
      String errorMessage = "Couldn't load config section '" + sectionName
	  + "' from URL '" + requestUrl + "': " + e.toString();
      log.error(errorMessage, e);
      output.setErrorMessage(errorMessage);
      output.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
      return output;
    }

    // Get the response status.
    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "statusCode = " + statusCode);
    output.setStatusCode(statusCode);

    switch (statusCode) {
    case OK:
      // Get the parts returned in the response.
      LinkedHashMap<String, Part> parts = response.getParts();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "parts = " + parts);

      // Get the configuration data.
      Part configDataPart = parts.get(CONFIG_PART_NAME);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "configDataPart = " + configDataPart);

      // Get and populate the configuration data last modified header.
      String lastModified = configDataPart.getLastModified();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModified = " + lastModified);

      output.setLastModified(lastModified);

      // Get and populate the configuration data ETag header.
      String etag = configDataPart.getEtag();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "etag = " + etag);

      output.setEtag(etag);

      Map<String, String> partHeaders = configDataPart.getHeaders();
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "partHeaders = " + partHeaders);

      // Get and populate the content type.
      String contentType = partHeaders.get(HttpHeaders.CONTENT_TYPE);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "contentType = " + contentType);
      output.setContentType(contentType);

      // Get and populate an input stream to the configuration data.
      output.setInputStream(configDataPart.getInputStream());
      break;
    case NOT_MODIFIED:
      if (log.isDebug2())
	log.debug2("REST Service content not changed, not reloading.");
      output.setErrorMessage(statusCode.toString());
      break;
    default:
      output.setErrorMessage(statusCode.toString());
    }

    return output;
  }

  /**
   * Calls the service Multipart GET method with the given URL and provides the
   * response.
   * 
   * @param url
   *          A String with the URL.
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @return a MultipartResponse with the response.
   * @throws IOException
   *           if there are problems getting the part payload.
   * @throws MessagingException
   *           if there are other problems.
   */
  public MultipartResponse callGetMultipartRequest(String url,
      HttpRequestPreconditions preconditions)
	  throws IOException, MessagingException {
    final String DEBUG_HEADER = "callGetMultipartRequest(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "url = " + url);
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);
    }

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
    setAuthenticationCredentials(requestHeaders);

    // Get the individual preconditions.
    List<String> ifMatch = null;
    String ifModifiedSince = null;
    List<String> ifNoneMatch = null;
    String ifUnmodifiedSince = null;

    if (preconditions != null) {
	ifMatch = preconditions.getIfMatch();
	ifModifiedSince = preconditions.getIfModifiedSince();
	ifNoneMatch = preconditions.getIfNoneMatch();
	ifUnmodifiedSince = preconditions.getIfUnmodifiedSince();
    }

    // Check whether there are If-Match preconditions.
    if (ifMatch != null && !ifMatch.isEmpty()) {
      // Yes.
      requestHeaders.setIfMatch(ifMatch);
    }

    // Check whether there is an If-Modified-Since precondition.
    if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
      // Yes.
      requestHeaders.set(HttpHeaders.IF_MODIFIED_SINCE, ifModifiedSince);
    }

    // Check whether there are If-None-Match preconditions.
    if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
      // Yes.
      requestHeaders.setIfNoneMatch(ifNoneMatch);
    }

    // Check whether there is an If-Unmodified-Since precondition.
    if (ifUnmodifiedSince != null && !ifUnmodifiedSince.isEmpty()) {
      // Yes.
      requestHeaders.set(HttpHeaders.IF_UNMODIFIED_SINCE, ifUnmodifiedSince);
    }

    // Make the request and obtain the response.
    MultipartResponse response = new MultipartConnector(uri, requestHeaders)
	.requestGet(serviceTimeout, serviceTimeout);

    return response;
  }

  public TdbAu getTdbAu(String auId) throws Exception {
    return new GetTdbAuClient(serviceLocation, serviceUser,
	    servicePassword, serviceTimeout).getTdbAu(auId);
  }

  /**
   * Sends the configuration of a section to the REST web service for saving.
   * 
   * @param restConfigSection
   *          A RestConfigSection with the request parameters.
   * @return a RestConfigSection with the result.
   */
  public RestConfigSection putConfigSection(RestConfigSection input) {
    final String DEBUG_HEADER = "putConfigSection(): ";
    if (log.isDebug2()) log.debug2(DEBUG_HEADER + "input = " + input);

    // Validation of the input object.
    if (input == null) {
      String errorMessage = "RestConfigSection is null";
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Validation of the section name.
    String sectionName = input.getSectionName();
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "sectionName = " + sectionName);

    if (StringUtil.isNullString(sectionName)) {
      String errorMessage = "Invalid section name '" + sectionName + "'";
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Validation of the payload data.
    InputStream config = input.getInputStream();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "config = " + config);

    if (config == null) {
      String errorMessage = "Configuration input stream is null";
      log.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Handle an unavailable service.
    if (!isActive()) {
      String errorMessage = "Couldn't save config section '" + sectionName
	  + "': Service is not active.";
      log.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }

    RestConfigSection output = new RestConfigSection();
    output.setSectionName(sectionName);
    String requestUrl = null;

    try {
      // Get the request URL.
      requestUrl = getSectionNameRequestUrl(sectionName);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "requestUrl = " + requestUrl);

      // Make the request and get the result.
      ResponseEntity<?> response = callPutMultipartRequest(requestUrl,
	  input.getHttpRequestPreconditions(), config, input.getContentType(),
	  input.getContentLength());
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "response = " + response);

      HttpStatus statusCode = response.getStatusCode();
      output.setStatusCode(statusCode);
      output.setErrorMessage(statusCode.toString());

      // Get and populate the configuration data last modified header.
      String lastModified =
	  response.getHeaders().getFirst(HttpHeaders.LAST_MODIFIED);
      if (log.isDebug3())
	log.debug3(DEBUG_HEADER + "lastModified = " + lastModified);

      output.setLastModified(lastModified);

      // Get and populate the configuration data ETag header.
      String etag = response.getHeaders().getETag();
      if (log.isDebug3()) log.debug3(DEBUG_HEADER + "etag = " + etag);

      output.setEtag(etag);
    } catch (HttpClientErrorException hcee) {
      String errorMessage = "Couldn't save config section '" + sectionName
	  + "' from URL '" + requestUrl + "': " + hcee.toString();
      log.error(errorMessage, hcee);
      log.error("hcee.getStatusCode() = " + hcee.getStatusCode());
      output.setStatusCode(hcee.getStatusCode());
      output.setErrorMessage(errorMessage);
    } catch (IOException ioe) {
      String errorMessage = "Couldn't save config section '" + sectionName
	  + "' from URL '" + requestUrl + "': " + ioe.toString();
      log.error(errorMessage, ioe);
      output.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
      output.setErrorMessage(errorMessage);
    }

    return output;
  }

  /**
   * Calls the service Multipart PUT method with the given URL and provides the
   * response.
   * 
   * @param url
   *          A String with the URL.
   * @param preconditions
   *          An HttpRequestPreconditions with the request preconditions to be
   *          met.
   * @param inputStream
   *          An InputStream to the content to be sent to the server.
   * @param contentType
   *          A String with the content type of the content to be sent to the
   *          server.
   * @param contentLength
   *          A long with the length of the content to be sent to the server.
   * @return an HttpStatus with the status of the response.
   * @throws IOException
   *           if there are problems reading the input stream.
   */
  public ResponseEntity<?> callPutMultipartRequest(String url,
      HttpRequestPreconditions preconditions, InputStream inputStream,
      String contentType, long contentLength) throws IOException {
    final String DEBUG_HEADER = "callPutMultipartRequest(): ";
    if (log.isDebug2()) {
      log.debug2(DEBUG_HEADER + "url = " + url);
      log.debug2(DEBUG_HEADER + "preconditions = " + preconditions);
      log.debug2(DEBUG_HEADER + "contentType = " + contentType);
      log.debug2(DEBUG_HEADER + "contentLength = " + contentLength);
    }

    // Create the URI of the request to the REST service.
    UriComponents uriComponents =
	UriComponentsBuilder.fromUriString(url).build();

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "uri = " + uri);

    // Initialize the part headers.
    HttpHeaders partHeaders = new HttpHeaders();
    partHeaders.setContentType(MediaType.valueOf(contentType));

    // This must be set or else AbstractResource#contentLength will read the
    // entire InputStream to determine the content length, which will exhaust
    // the InputStream.
    partHeaders.setContentLength(contentLength);
    if (log.isDebug3())
      log.debug3(DEBUG_HEADER + "partHeaders = " + partHeaders);

    // Add the payload.
    MultiValueMap<String, Object> parts =
	new LinkedMultiValueMap<String, Object>();

    Resource resource =
	new NamedInputStreamResource(CONFIG_PART_NAME, inputStream);

    parts.add(CONFIG_PART_NAME, new HttpEntity<>(resource, partHeaders));
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "parts = " + parts);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
    requestHeaders.setContentType(MediaType.MULTIPART_FORM_DATA);

    // Set the authentication credentials.
    setAuthenticationCredentials(requestHeaders);

    // Get the individual preconditions.
    List<String> ifMatch = null;
    String ifModifiedSince = null;
    List<String> ifNoneMatch = null;
    String ifUnmodifiedSince = null;

    if (preconditions != null) {
	ifMatch = preconditions.getIfMatch();
	ifModifiedSince = preconditions.getIfModifiedSince();
	ifNoneMatch = preconditions.getIfNoneMatch();
	ifUnmodifiedSince = preconditions.getIfUnmodifiedSince();
    }

    // Check whether there are If-Match preconditions.
    if (ifMatch != null && !ifMatch.isEmpty()) {
      // Yes.
      requestHeaders.setIfMatch(ifMatch);
    }

    // Check whether there is an If-Modified-Since precondition.
    if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
      // Yes.
      requestHeaders.set(HttpHeaders.IF_MODIFIED_SINCE, ifModifiedSince);
    }

    // Check whether there are If-None-Match preconditions.
    if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
      // Yes.
      requestHeaders.setIfNoneMatch(ifNoneMatch);
    }

    // Check whether there is an If-Unmodified-Since precondition.
    if (ifUnmodifiedSince != null && !ifUnmodifiedSince.isEmpty()) {
      // Yes.
      requestHeaders.set(HttpHeaders.IF_UNMODIFIED_SINCE, ifUnmodifiedSince);
    }

    // Make the request and obtain the response.
    ResponseEntity<?> response = new MultipartConnector(uri, requestHeaders,
	parts).requestPut(serviceTimeout, serviceTimeout);
    if (log.isDebug3()) log.debug3(DEBUG_HEADER + "response = " + response);

    return response;
  }

  /**
   * Provides the configurations of all the Archival Units obtained via the REST
   * web service.
   * 
   * @return a Collection<AuConfiguration> with the result.
   */
  public Collection<AuConfiguration> getAllArchivalUnitConfiguration() {
    // Create the URI of the request to the REST service.
    UriComponents uriComponents =
	UriComponentsBuilder.fromUriString(serviceLocation + "/aus").build();

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();
    if (log.isDebug3()) log.debug3("uri = " + uri);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();

    // Set the authentication credentials.
    setAuthenticationCredentials(requestHeaders);

    // Create the request entity.
    HttpEntity<Collection<AuConfiguration>> requestEntity =
	new HttpEntity<Collection<AuConfiguration>>(null, requestHeaders);

    // Make the request and get the response. 
    ResponseEntity<Collection<AuConfiguration>> response = getRestTemplate()
	.exchange(uri, HttpMethod.GET, requestEntity,
	    new ParameterizedTypeReference<Collection<AuConfiguration>>(){});

    // Get the response status.
    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3("statusCode = " + statusCode);

    if (!isSuccess(statusCode)) {
      if (log.isDebug2())
	log.debug2("auConfigurations = " + Collections.emptyList());
      return Collections.emptyList();
    }

    Collection<AuConfiguration> result = response.getBody();
    if (log.isDebug2()) log.debug2("result = " + result);
    return result;
  }

  /**
   * Provides the configurations of an Archival Unit obtained via the REST web
   * service.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return an AuConfiguration with the result.
   */
  public AuConfiguration getArchivalUnitConfiguration(String auId) {
    if (log.isDebug2()) log.debug2("auId = " + auId);

    // Get the URL template.
    String template = getAuConfigRequestUrl();

    // Create the URI of the request to the REST service.
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(template)
	.build().expand(Collections.singletonMap("auid", auId));

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();
    if (log.isDebug3()) log.debug3("uri = " + uri);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();

    // Set the authentication credentials.
    setAuthenticationCredentials(requestHeaders);

    // Create the request entity.
    HttpEntity<AuConfiguration> requestEntity =
	new HttpEntity<AuConfiguration>(null, requestHeaders);

    // Make the request and get the response. 
    ResponseEntity<AuConfiguration> response = getRestTemplate().exchange(uri,
	HttpMethod.GET, requestEntity, AuConfiguration.class);

    // Get the response status.
    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3("statusCode = " + statusCode);

    AuConfiguration result = null;

    if (!isSuccess(statusCode)) {
      if (log.isDebug2()) log.debug2("result = " + result);
      return result;
    }

    result = response.getBody();
    if (log.isDebug2()) log.debug2("result = " + result);
    return result;
  }

  /**
   * Deletes the configurations of an Archival Unit via the REST web service.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return an AuConfiguration with the configuration that has been deleted.
   */
  public AuConfiguration deleteArchivalUnitConfiguration(String auId) {
    if (log.isDebug2()) log.debug2("auId = " + auId);

    // Get the URL template.
    String template = getAuConfigRequestUrl();

    // Create the URI of the request to the REST service.
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(template)
	.build().expand(Collections.singletonMap("auid", auId));

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();
    if (log.isDebug3()) log.debug3("uri = " + uri);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();

    // Set the authentication credentials.
    setAuthenticationCredentials(requestHeaders);

    // Create the request entity.
    HttpEntity<AuConfiguration> requestEntity =
	new HttpEntity<AuConfiguration>(null, requestHeaders);

    // Make the request and get the response. 
    ResponseEntity<AuConfiguration> response = getRestTemplate().exchange(uri,
	HttpMethod.DELETE, requestEntity, AuConfiguration.class);

    // Get the response status.
    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3("statusCode = " + statusCode);

    AuConfiguration result = null;

    if (!isSuccess(statusCode)) {
      if (log.isDebug2()) log.debug2("result = " + result);
      return result;
    }

    result = response.getBody();
    if (log.isDebug2()) log.debug2("result = " + result);
    return result;
  }

  /**
   * Stores the configurations of an Archival Unit via the REST web service.
   * 
   * @param auId
   *          A String with the Archival Unit identifier.
   * @return a Long with the key under which the Archival Unit configuration has
   *         been stored.
   */
  public Long putArchivalUnitConfiguration(AuConfiguration auConfiguration) {
    if (log.isDebug2()) log.debug2("auConfiguration = " + auConfiguration);

    // Get the URL template.
    String template = getAuConfigRequestUrl();

    // Create the URI of the request to the REST service.
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(template)
	.build().expand(Collections.singletonMap("auid",
	    auConfiguration.getAuId()));

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();
    if (log.isDebug3()) log.debug3("uri = " + uri);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();

    // Set the authentication credentials.
    setAuthenticationCredentials(requestHeaders);

    // Create the request entity.
    HttpEntity<AuConfiguration> requestEntity =
	new HttpEntity<AuConfiguration>(null, requestHeaders);

    // Make the request and get the response. 
    ResponseEntity<Long> response = getRestTemplate().exchange(uri,
	HttpMethod.PUT, requestEntity, Long.class);

    // Get the response status.
    HttpStatus statusCode = response.getStatusCode();
    if (log.isDebug3()) log.debug3("statusCode = " + statusCode);

    Long result = null;

    if (!isSuccess(statusCode)) {
      if (log.isDebug2()) log.debug2("result = " + result);
      return result;
    }

    result = response.getBody();
    if (log.isDebug2()) log.debug2("result = " + result);
    return result;
  }

  /**
   * Provides the URL needed to read from, or write to, the REST Configuration
   * Service the configuration of a section.
   * 
   * @param sectionName
   *          A String with the name of the section.
   * @return a String with the URL.
   */
  private String getSectionNameRequestUrl(String sectionName) {
    return serviceLocation + "/config/file/" + sectionName;
  }

  /**
   * Provides the URL needed to read from, write to, or delete from, the REST
   * Configuration Service the configuration of an Archival Unit.
   * 
   * @return a String with the URL.
   */
  private String getAuConfigRequestUrl() {
    return serviceLocation + "/aus/{auid}";
  }

  /**
   * Sets the authentication credentials in a request.
   * 
   * @param requestHeaders
   *          An HttpHeaders with the request headers.
   */
  private void setAuthenticationCredentials(HttpHeaders requestHeaders) {
    String credentials = serviceUser + ":" + servicePassword;
    String authHeaderValue = "Basic " + Base64.getEncoder()
    .encodeToString(credentials.getBytes(StandardCharsets.US_ASCII));
    requestHeaders.set("Authorization", authHeaderValue);
    if (log.isDebug3()) log.debug3("requestHeaders = " + requestHeaders);
  }

  /**
   * Provides a standard REST template.
   * 
   * @return a RestTemplate with the standard REST template.
   */
  private RestTemplate getRestTemplate() {
    RestTemplate restTemplate = new RestTemplate();

    // Do not throw exceptions on non-success response status codes.
    restTemplate.setErrorHandler(new DefaultResponseErrorHandler(){
      protected boolean hasError(HttpStatus statusCode) {
	return false;
      }
    });

    return restTemplate;
  }

  /**
   * Provides an indication of whether a successful response has been obtained.
   * 
   * @param statusCode
   *          An HttpStatus with the response status code.
   * @return a boolean with <code>true</code> if a successful response has been
   *         obtained, <code>false</code> otherwise.
   */
  private boolean isSuccess(HttpStatus statusCode) {
    return statusCode.is2xxSuccessful();
  }
}
