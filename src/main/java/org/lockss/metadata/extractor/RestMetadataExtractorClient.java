/*

Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.metadata.extractor;

import static org.lockss.util.rest.MetadataExtractorConstants.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.lockss.app.LockssDaemon;
import org.lockss.log.L4JLogger;
import org.lockss.util.Constants;
import org.lockss.util.rest.exception.LockssRestException;
import org.lockss.util.rest.exception.LockssRestNetworkException;
import org.lockss.util.rest.RestUtil;

/**
 * A client for the REST metadata extractor service.
 * 
 * @author Fernando García-Loygorri
 */
public class RestMetadataExtractorClient {
  private static L4JLogger log = L4JLogger.getLogger();
  private String endpointUrl;
  private String serviceUser = null;
  private String servicePassword = null;
  private long connectTimeout;
  private long readTimeout;

  /**
   * Constructor with the endpoint URL.
   * 
   * @param endpointUrl A String with the REST web service endpoint URL.
   */
  public RestMetadataExtractorClient(String endpointUrl) {
    this(endpointUrl, 10 * Constants.SECOND, 30 * Constants.SECOND);
  }

  /**
   * Constructor with the endpoint URL and timeouts.
   * 
   * @param endpointUrl    A String with the REST web service endpoint URL.
   * @param connectTimeout A long with the connection timeout in milliseconds.
   * @param readTimeout    A long with the read timeout in milliseconds.
   */
  public RestMetadataExtractorClient(String endpointUrl, long connectTimeout,
      long readTimeout) {
    this.endpointUrl = endpointUrl;
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;

    // Populate the authentication credentials, if any.
    setAuthenticationCredentials();
  }


  /**
   * Saves the authentication credentials, if any.
   */
  private void setAuthenticationCredentials() {
    // Get the REST client credentials.
    List<String> restClientCredentials = LockssDaemon.getLockssDaemon()
	.getRestClientCredentials();
    log.trace("restClientCredentials = " + restClientCredentials);

    // Check whether there is a user name.
    if (restClientCredentials != null && restClientCredentials.size() > 0) {
      // Yes: Get the user name.
      serviceUser = restClientCredentials.get(0);
      log.trace("serviceUser = " + serviceUser);

      // Check whether there is a user password.
      if (restClientCredentials.size() > 1) {
	// Yes: Get the user password.
	servicePassword = restClientCredentials.get(1);
      }
    }
  }

  /**
   * Schedules a metadata extraction operation via the REST service.
   * 
   * @param auId        A String with the identifier of the archival unit
   *                    involved in the operation.
   * @param fullReindex A boolean indicating whether a full metadata reindex is
   *                    requested.
   * @return a String with information about the scheduled operation.
   * @throws LockssRestException if there are problems scheduling the operation.
   */
  public String scheduleMetadataExtraction(String auId, boolean fullReindex)
      throws LockssRestException  {
    log.debug2("auId = {}", auId);
    log.debug2("fullReindex = {}", fullReindex);

    // Validate the archival unit identifier.
    if (auId == null || auId.trim().isEmpty()) {
      throw new IllegalArgumentException("Null or empty Archival Unit ID");
    }

    String template = endpointUrl + "/mdupdates";

    // Create the URI of the request to the REST service.
    UriComponents uriComponents =
	UriComponentsBuilder.fromUriString(template).build();

    URI uri = UriComponentsBuilder.newInstance().uriComponents(uriComponents)
	.build().encode().toUri();
    log.trace("uri = {}", () -> uri);

    // Initialize the request headers.
    HttpHeaders requestHeaders = new HttpHeaders();

    // Set the authentication credentials.
    setAuthenticationCredentials(requestHeaders);

    // Initialize the payload.
    MetadataUpdateSpec metadataUpdateSpec = new MetadataUpdateSpec();
    metadataUpdateSpec.setAuid(auId);

    String updateType = fullReindex
	? MD_UPDATE_INCREMENTAL_EXTRACTION : MD_UPDATE_FULL_EXTRACTION;
    metadataUpdateSpec.setUpdateType(updateType);
    log.trace("metadataUpdateSpec = {}", metadataUpdateSpec);

    // Create the request entity.
    HttpEntity<MetadataUpdateSpec> requestEntity =
	new HttpEntity<MetadataUpdateSpec>(metadataUpdateSpec, requestHeaders);

    try {
      // Make the REST call.
      ResponseEntity<String> response =
	RestUtil.callRestService(createRestTemplate(), uri, HttpMethod.POST,
				 requestEntity, String.class,
				 "Can't schedule Metadata Extraction");

      // Get the status.
      int status = response.getStatusCodeValue();
      log.trace("status = " + status);

      // Get the response body.
      String result = response.getBody();
      log.debug2("result = " + result);
      return result;
    } catch (RuntimeException re) {
      log.error("Exception caught", re);
      throw new LockssRestNetworkException(re);
    }
  }

  /**
   * Sets the authentication credentials in a request.
   * 
   * @param requestHeaders
   *          An HttpHeaders with the request headers.
   */
  private void setAuthenticationCredentials(HttpHeaders requestHeaders) {
    if (serviceUser != null && servicePassword != null) {
      String credentials = serviceUser + ":" + servicePassword;
      String authHeaderValue = "Basic " + Base64.getEncoder()
      .encodeToString(credentials.getBytes(StandardCharsets.US_ASCII));
      requestHeaders.set("Authorization", authHeaderValue);
      log.trace("requestHeaders = {}", requestHeaders);
    }
  }

  /**
   * Provides the REST template to be used to make the call to the REST service.
   * 
   * @return a RestTemplate with the REST template.
   */
  private RestTemplate createRestTemplate() {
    log.debug2("Invoked");

    // Initialize the request to the REST service.
    RestTemplate restTemplate = new RestTemplate();

    // Do not throw exceptions on non-success response status codes.
    restTemplate.setErrorHandler(new DefaultResponseErrorHandler(){
      protected boolean hasError(HttpStatus statusCode) {
	return false;
      }
    });

    // Specify the timeouts.
    SimpleClientHttpRequestFactory requestFactory =
	(SimpleClientHttpRequestFactory)restTemplate.getRequestFactory();

    requestFactory.setConnectTimeout((int)connectTimeout);
    requestFactory.setReadTimeout((int)readTimeout);

    log.debug2("restTemplate = {}", restTemplate);
    return restTemplate;
  }

  /**
   * The information defining an AU metadata update operation
   */
  public class MetadataUpdateSpec   {
    private String auid = null;
    private String updateType = null;

    /**
     * Provides the identifier of the AU for which the metadata update is to be
     * performed.
     * 
     * @return a String with the AU identifier.
     */
    public String getAuid() {
      return auid;
    }

    /**
     * Saves the identifier of the AU for which the metadata update is to be
     * performed.
     * 
     * @param auid A String with the AU identifier.
     */
    public void setAuid(String auid) {
      this.auid = auid;
    }

    /**
     * Provides the type of metadata update to be performed.
     * 
     * @return updateType a String with the update type.
     */
    public String getUpdateType() {
      return updateType;
    }

    /**
     * Saves the type of metadata update to be performed.
     * 
     * @param updateType A String with the update type.
     */
    public void setUpdateType(String updateType) {
      this.updateType = updateType;
    }

    @Override
    public boolean equals(java.lang.Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MetadataUpdateSpec metadataUpdateSpec = (MetadataUpdateSpec) o;
      return Objects.equals(this.auid, metadataUpdateSpec.auid) &&
          Objects.equals(this.updateType, metadataUpdateSpec.updateType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(auid, updateType);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("class MetadataUpdateSpec {\n");
      
      sb.append("    auid: ").append(toIndentedString(auid)).append("\n");
      sb.append("    updateType: ").append(toIndentedString(updateType))
      .append("\n");
      sb.append("}");
      return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(Object o) {
      if (o == null) {
        return "null";
      }
      return o.toString().replace("\n", "\n    ");
    }
  }
}
